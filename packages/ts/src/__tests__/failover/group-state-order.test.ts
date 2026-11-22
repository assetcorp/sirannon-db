import { describe, expect, it } from 'vitest'
import { cleanupDockerResources, startEtcdCluster } from './docker.js'
import { EtcdOracle } from './etcd-oracle.js'
import { waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  type GateEnvironment,
  GROUP_ID,
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
import { waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'

const ORACLE_READ_ATTEMPTS = 5

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

  it('follows a new etcd cluster whose revisions start below the ones that the nodes hold, without a restart', async () => {
    const env = environment()
    await startCluster(env)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'on-the-first-etcd-cluster'))
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await waitForCondition(async () => {
      const stored = await env.etcdOracle.groupWithModRevision()
      const statuses = await Promise.all(NODE_IDS.map(async nodeId => statusOf(requireNode(env, nodeId))))
      return statuses.every(status => status.coordinator?.groupStateVersion === String(stored?.modRevision))
    }, 10_000)
    const record = await env.etcdOracle.groupRecord(GROUP_ID)
    const original = await env.etcdOracle.groupWithModRevision()
    if (!original || !record) throw new Error('etcd holds no group record')

    const originalOracle = env.etcdOracle
    const replacement = await startEtcdCluster(`${env.runPrefix}-swap${env.scenario}`)
    try {
      const replacementOracle = new EtcdOracle(replacement.endpoints, env.keyPrefix, CLUSTER_ID, GROUP_ID)
      await replacementOracle.writeGroupRecord(GROUP_ID, JSON.stringify(record))
      const copied = await replacementOracle.groupWithModRevision()
      if (!copied) throw new Error('the new etcd cluster holds no group record')
      expect(copied.modRevision).toBeLessThan(original.modRevision)
      expect(copied.etcdClusterId).not.toBe(original.etcdClusterId)

      env.etcdOracle = replacementOracle
      await pointEtcdLinksAt(env, replacement)
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
        const live = await replacementOracle.liveNodeIds()
        const controller = await replacementOracle.controller()
        const stored = await replacementOracle.groupWithModRevision()
        const statuses = await Promise.all(NODE_IDS.map(async nodeId => statusOf(requireNode(env, nodeId))))
        return (
          live.length === NODE_IDS.length &&
          controller !== null &&
          statuses.every(status => status.coordinator?.groupStateVersion === String(stored?.modRevision))
        )
      }, 30_000)

      const { currentPrimary } = await waitForPrimary(env, NODE_IDS, 15_000)
      const primary = requireNode(env, currentPrimary.nodeId)
      const [cutNodeId, ...remaining] = NODE_IDS.filter(nodeId => nodeId !== currentPrimary.nodeId)
      if (!cutNodeId) throw new Error('the cluster has no replica')
      await setReplicationLinks(env, cutNodeId, false)
      await writeItemMajority(primary, itemRow(2, currentPrimary.nodeId, 'on-the-new-etcd-cluster'))
      await waitForInSyncSet(env, [currentPrimary.nodeId, ...remaining], 15_000)
      await waitForCondition(async () => {
        const stored = await replacementOracle.groupWithModRevision()
        return (await statusOf(primary)).coordinator?.groupStateVersion === String(stored?.modRevision)
      }, 10_000)
      await expectNodeHoldsStoredGroup(env, primary)
      const cutNodeReadConcerns = async () =>
        (await clusterStatusOf(env, currentPrimary.nodeId)).readEndpoints?.find(entry => entry.nodeId === cutNodeId)
          ?.readConcerns
      await waitForCondition(async () => (await cutNodeReadConcerns())?.length === 1, 10_000)
      expect(await cutNodeReadConcerns()).toEqual(['local'])
    } finally {
      env.etcdOracle = originalOracle
      await pointEtcdLinksAt(env, env.etcd)
      await cleanupDockerResources({ containers: replacement.containerNames, networks: [replacement.networkName] })
    }
  })
})
