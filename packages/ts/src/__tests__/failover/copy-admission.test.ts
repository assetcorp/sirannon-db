import { describe, expect, it } from 'vitest'
import { waitForIdenticalFiles } from './gate-assertions.js'
import { waitForInSyncSet } from './gate-cluster-state.js'
import { CLUSTER_ID, GROUP_ID, NODE_A, NODE_B, NODE_C, NODE_IDS, requireNode } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForStatus } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { holdsThroughout, waitForCondition } from './gate-support.js'

describe('admissions in flight when a replica requests a full copy', () => {
  const environment = useFailoverEnvironment()

  it('settles an admission already in flight before it resets the replica and streams a copy', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { maxReplicaLagMs: 60_000 } })
    const rows = [1, 2, 3].map(id => itemRow(id, NODE_A, 'before-the-copy'))
    const primary = requireNode(env, NODE_A)
    for (const row of rows) await writeItemMajority(primary, row)
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await primary.holdInSyncUpdates({ admissions: true, admitsNodeId: NODE_C })
    await primary.copyTraffic('hold', 'batch', NODE_C)
    await env.coordinator.updateInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      inSyncNodeIds: [NODE_A, NODE_B],
    })
    await waitForCondition(async () => (await primary.heldInSyncUpdates()) > 0, 10_000)
    const receivedRequests = await primary.copyTraffic('received', 'request', NODE_C)
    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_C,
      repairing: true,
    })
    await waitForStatus(
      requireNode(env, NODE_C),
      status => status.syncState?.phase === 'syncing',
      10_000,
      'a requested copy',
    )
    await waitForCondition(
      async () => (await primary.copyTraffic('received', 'request', NODE_C)) > receivedRequests,
      10_000,
    )
    await holdsThroughout(
      async () => (await primary.copyTraffic('count')) === 0,
      500,
      'the primary to wait for the pending admission before streaming the copy',
    )
    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_C,
      repairing: false,
    })
    await primary.releaseInSyncUpdates()
    await waitForCondition(async () => (await primary.copyTraffic('count')) > 0, 10_000)
    await holdsThroughout(
      async () => (await env.etcdOracle.group())?.inSyncNodeIds.includes(NODE_C) === false,
      1_000,
      'a pending admission to give no credit to the replica after the reset',
    )
    await primary.copyTraffic('release')
    await waitForInSyncSet(env, NODE_IDS, 30_000)
    expect(await waitForIdenticalFiles(env, NODE_IDS, 15_000)).toEqual(rows)
  })
})
