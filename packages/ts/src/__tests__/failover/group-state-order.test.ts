import { describe, expect, it } from 'vitest'
import { waitForInSyncSet } from './gate-cluster-state.js'
import {
  type GateEnvironment,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
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
})
