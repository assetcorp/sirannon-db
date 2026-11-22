import { describe, expect, it } from 'vitest'
import { cleanupDockerResources, type EtcdClusterHandle, startEtcdCluster } from './docker.js'
import { EtcdOracle, type StoredGroupAtRevision } from './etcd-oracle.js'
import { expectGroupHolds, waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  DEFAULT_GATE_SESSION_TTL_MS,
  type GateEnvironment,
  GROUP_ID,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  pointEtcdLinksAt,
  requireNode,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { clusterStatusOf, statusOf, waitForStatus } from './gate-observations.js'
import { itemRow, postSql, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { sleep, waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'

const ORACLE_READ_ATTEMPTS = 5
const ORACLE_POLL_INTERVAL_MS = 25
const ORACLE_POLL_SLACK_MS = 250
const PROMOTION_DEADLINE_MS = 20_000

async function expectNodeHoldsStoredGroup(env: GateEnvironment, node: FailoverNodeProcess): Promise<void> {
  for (let attempt = 0; attempt < ORACLE_READ_ATTEMPTS; attempt++) {
    const before = await env.etcdOracle.groupWithModRevision()
    const status = await statusOf(node)
    const after = await env.etcdOracle.groupWithModRevision()
    if (!before || !after) throw new Error('etcd holds no group record')
    if (before.modRevision !== after.modRevision) continue
    expect(status.coordinator?.groupStateVersion).toBe(String(after.modRevision))
    expect(status.coordinator?.groupStateStoreId).toBe(after.etcdClusterId)
    expect([...(status.coordinator?.inSyncNodeIds ?? [])].sort()).toEqual([...after.group.inSyncNodeIds].sort())
    return
  }
  throw new Error(`etcd changed the group record during each of ${ORACLE_READ_ATTEMPTS} reads`)
}

describe('the group state that a node holds', () => {
  const environment = useFailoverEnvironment()

  it('keeps the newer state when etcd answers a read before a later write and the node receives that answer after it', async () => {
    const env = environment()
    await startCluster(env)
    const primary = requireNode(env, NODE_A)
    await writeItemMajority(primary, itemRow(1, NODE_A, 'before-the-held-read'))
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await waitForCondition(async () => {
      const stored = await env.etcdOracle.groupWithModRevision()
      return (await statusOf(primary)).coordinator?.groupStateVersion === String(stored?.modRevision)
    }, 10_000)
    await expectNodeHoldsStoredGroup(env, primary)

    await primary.groupReads('hold')
    const majorityRead = Promise.allSettled([
      postSql(env, NODE_A, 'query', { sql: 'SELECT id FROM failover_items', readConcern: { level: 'majority' } }),
    ])
    await waitForCondition(async () => (await primary.groupReads('count')) === 1, 10_000)
    const answeredBefore = await env.etcdOracle.groupWithModRevision()
    expect([...(answeredBefore?.group.inSyncNodeIds ?? [])].sort()).toEqual([...NODE_IDS].sort())

    await setReplicationLinks(env, NODE_C, false)
    await writeItemMajority(primary, itemRow(2, NODE_A, 'while-the-read-is-held'))
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    const removal = await env.etcdOracle.groupWithModRevision()
    if (!removal || !answeredBefore) throw new Error('etcd holds no group record')
    expect(removal.modRevision).toBeGreaterThan(answeredBefore.modRevision)
    await waitForStatus(
      primary,
      status => BigInt(status.coordinator?.groupStateVersion ?? '0') >= removal.modRevision,
      10_000,
      `a group state at etcd revision ${removal.modRevision} or later`,
    )

    expect(await primary.groupReads('release')).toBe(1)
    await expectNodeHoldsStoredGroup(env, primary)
    const routing = await clusterStatusOf(env, NODE_A)
    expect(routing.readEndpoints?.find(entry => entry.nodeId === NODE_C)?.readConcerns).toEqual(['local'])

    const [settled] = await majorityRead
    expect(settled.status).toBe('fulfilled')
    if (settled.status === 'fulfilled') expect(settled.value.status).toBe(200)
  })

  it('follows a new etcd cluster whose revisions start below the ones that the nodes hold, without a restart or a change of primary', async () => {
    const env = environment()
    await startSettledCluster(env)

    await withCopiedEtcd(env, async ({ handle, oracle, copied }) => {
      await pointEtcdLinksAt(env, handle)
      for (const nodeId of NODE_IDS) {
        await waitForStatus(
          requireNode(env, nodeId),
          status =>
            status.coordinator?.groupStateStoreId === copied.etcdClusterId &&
            BigInt(status.coordinator.groupStateVersion ?? '0') >= copied.modRevision,
          30_000,
          `a group state from etcd cluster ${copied.etcdClusterId}`,
        )
      }
      await waitForCondition(async () => {
        const live = await oracle.liveNodeIds()
        const controller = await oracle.controller()
        return live.length === NODE_IDS.length && controller !== null && (await allNodesHoldStoredVersion(env))
      }, 30_000)
      await expectGroupHolds(env, NODE_A, copied.group.primaryTerm, 2 * DEFAULT_GATE_SESSION_TTL_MS)
      expect(await oracle.primaryHistory()).toEqual([{ nodeId: NODE_A, primaryTerm: copied.group.primaryTerm }])

      const primary = requireNode(env, NODE_A)
      await setReplicationLinks(env, NODE_C, false)
      await writeItemMajority(primary, itemRow(2, NODE_A, 'on-the-new-etcd-cluster'))
      await waitForInSyncSet(env, [NODE_A, NODE_B], 15_000)
      await waitForCondition(async () => {
        const stored = await oracle.groupWithModRevision()
        return (await statusOf(primary)).coordinator?.groupStateVersion === String(stored?.modRevision)
      }, 10_000)
      await expectNodeHoldsStoredGroup(env, primary)
      const cutNodeReadConcerns = async () =>
        (await clusterStatusOf(env, NODE_A)).readEndpoints?.find(entry => entry.nodeId === NODE_C)?.readConcerns
      await waitForCondition(async () => (await cutNodeReadConcerns())?.length === 1, 10_000)
      expect(await cutNodeReadConcerns()).toEqual(['local'])
      expect(await oracle.primaryHistory()).toEqual([{ nodeId: NODE_A, primaryTerm: copied.group.primaryTerm }])
    })
  })

  it('promotes a replica over a primary that is down when a controller takes the lease on a new etcd cluster, once a session lease length has passed', async () => {
    const env = environment()
    await startSettledCluster(env)

    await withCopiedEtcd(env, async ({ handle, oracle, copied }) => {
      await killNode(env, NODE_A)
      await pointEtcdLinksAt(env, handle)
      const { controllerSeenAtMs, promotedAtMs } = await timeControllerAndPromotion(oracle)

      expect(promotedAtMs - controllerSeenAtMs).toBeGreaterThanOrEqual(
        DEFAULT_GATE_SESSION_TTL_MS - ORACLE_POLL_SLACK_MS,
      )
      const promoted = await waitForPrimary(env, [NODE_B, NODE_C], 5_000)
      expect(promoted.primaryTerm).toBe(copied.group.primaryTerm + 1n)
      await writeItemMajority(
        requireNode(env, promoted.currentPrimary.nodeId),
        itemRow(2, promoted.currentPrimary.nodeId, 'after-the-promotion'),
      )
    })
  })
})

interface CopiedEtcd {
  handle: EtcdClusterHandle
  oracle: EtcdOracle
  copied: StoredGroupAtRevision
}

async function allNodesHoldStoredVersion(env: GateEnvironment): Promise<boolean> {
  const stored = await env.etcdOracle.groupWithModRevision()
  const statuses = await Promise.all([...env.nodes.values()].map(async node => statusOf(node)))
  return statuses.every(status => status.coordinator?.groupStateVersion === String(stored?.modRevision))
}

async function startSettledCluster(env: GateEnvironment): Promise<void> {
  await startCluster(env)
  await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'on-the-first-etcd-cluster'))
  await waitForInSyncSet(env, NODE_IDS, 15_000)
  await waitForCondition(async () => allNodesHoldStoredVersion(env), 10_000)
}

async function withCopiedEtcd(env: GateEnvironment, run: (replacement: CopiedEtcd) => Promise<void>): Promise<void> {
  const record = await env.etcdOracle.groupRecord(GROUP_ID)
  const original = await env.etcdOracle.groupWithModRevision()
  if (!original || !record) throw new Error('etcd holds no group record')
  const originalOracle = env.etcdOracle
  const handle = await startEtcdCluster(`${env.runPrefix}-swap${env.scenario}`)
  try {
    const oracle = new EtcdOracle(handle.endpoints, env.keyPrefix, CLUSTER_ID, GROUP_ID)
    await oracle.writeGroupRecord(GROUP_ID, JSON.stringify(record))
    const copied = await oracle.groupWithModRevision()
    if (!copied) throw new Error('the new etcd cluster holds no group record')
    expect(copied.modRevision).toBeLessThan(original.modRevision)
    expect(copied.etcdClusterId).not.toBe(original.etcdClusterId)
    env.etcdOracle = oracle
    await run({ handle, oracle, copied })
  } finally {
    env.etcdOracle = originalOracle
    await pointEtcdLinksAt(env, env.etcd)
    await cleanupDockerResources({ containers: handle.containerNames, networks: [handle.networkName] })
  }
}

async function timeControllerAndPromotion(
  oracle: EtcdOracle,
): Promise<{ controllerSeenAtMs: number; promotedAtMs: number }> {
  const deadline = Date.now() + PROMOTION_DEADLINE_MS
  let controllerSeenAtMs: number | null = null
  while (Date.now() < deadline) {
    const [controller, group] = await Promise.all([oracle.controller(), oracle.group()])
    const now = Date.now()
    if (controller !== null) controllerSeenAtMs ??= now
    const promotedNodeId = group?.currentPrimary?.nodeId
    if (promotedNodeId !== undefined && promotedNodeId !== NODE_A) {
      if (controllerSeenAtMs === null) throw new Error(`etcd names ${promotedNodeId} as primary before any controller`)
      return { controllerSeenAtMs, promotedAtMs: now }
    }
    await sleep(ORACLE_POLL_INTERVAL_MS)
  }
  throw new Error(`no replica became primary within ${PROMOTION_DEADLINE_MS}ms of the etcd swap`)
}
