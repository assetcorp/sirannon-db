import { afterEach, describe, it } from 'vitest'
import {
  assertItemAbsent,
  assertItemVisible,
  expectRejectsWith,
  expectResourceUseBelow,
  waitForEvent,
  waitForItemNote,
  waitForRows,
} from './gate-assertions.js'
import {
  assertHealth,
  expectExactlyOneWritablePrimary,
  waitForAnyCurrentPrimary,
  waitForCurrentPrimary,
  waitForInSyncSet,
  waitForNoSafePrimary,
} from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  cleanupEnvironment,
  type GateEnvironment,
  GROUP_ID,
  healNodeSirannonLinks,
  httpEndpointFor,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  setEtcdLink,
  startEnvironment,
  startNode,
} from './gate-environment.js'
import { waitForNodeReady, waitForPeerConnected } from './gate-observations.js'
import {
  executeMajority,
  executePublicClient,
  executePublicHttp,
  executePublicWebSocket,
  expectPublicHttpRejectsWith,
} from './gate-public-api.js'
import { restartReplicaThroughRepair, runSeededSoak } from './gate-scenarios.js'
import {
  expectStaleBatchRejectedWithoutMutation,
  expectStaleForwardRejectedWithoutMutation,
  expectStaleSyncBatchRejectedWithoutMutation,
  expectStaleSyncRequestRejectedWithoutMutation,
} from './gate-stale-messages.js'

let env: GateEnvironment | null = null

describe('batch two failover conformance gate', () => {
  afterEach(async () => {
    if (env) {
      await cleanupEnvironment(env)
      env = null
    }
  })

  it('proves coordinator-mode failover invariants with real etcd, gRPC, durable nodes, and TCP link faults', async () => {
    env = await startEnvironment()

    const nodeA = await startNode(env, NODE_A, 'primary', [], true, true)
    await waitForNodeReady(nodeA)
    const nodeB = await startNode(env, NODE_B, 'replica', [NODE_A, NODE_C], false, false)
    await waitForNodeReady(nodeB)
    const nodeC = await startNode(env, NODE_C, 'replica', [NODE_A, NODE_B], false, false)
    await waitForNodeReady(nodeC)
    await nodeB.reconnectTransport()
    await waitForPeerConnected(nodeB, NODE_C, 10_000)

    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 30_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, '1')

    await executeMajority(nodeA, 1, 'majority-before-failover')
    await waitForRows(env, [NODE_A, NODE_B, NODE_C], 1, 30_000)
    await executePublicHttp(env, NODE_A, "UPDATE failover_items SET note = 'public-http' WHERE id = 1")
    await waitForItemNote(env, [NODE_A, NODE_B, NODE_C], 1, 'public-http', 30_000)
    await assertHealth(env, NODE_A, '1', 'available', 'available')

    await setEtcdLink(env, NODE_A, false)
    await expectRejectsWith(
      nodeA.execute("INSERT INTO failover_items (id, owner, value, note) VALUES (99, 'a', 99, 'stale')"),
      ['COORDINATOR_UNAVAILABLE', 'STALE_PRIMARY'],
    )
    await expectPublicHttpRejectsWith(
      env,
      NODE_A,
      "INSERT INTO failover_items (id, owner, value, note) VALUES (98, 'a', 98, 'public-stale')",
      ['COORDINATOR_UNAVAILABLE', 'STALE_PRIMARY'],
    )
    await assertItemAbsent(env, [NODE_A, NODE_B, NODE_C], 99, 10_000)
    await assertItemAbsent(env, [NODE_A, NODE_B, NODE_C], 98, 10_000)

    nodeA.kill()
    env.nodes.delete(NODE_A)
    const promotedNodeId = await waitForAnyCurrentPrimary(env, [NODE_B, NODE_C], 30_000)
    const promotedNode = requireNode(env, promotedNodeId)
    const forwardingNodeId = promotedNodeId === NODE_B ? NODE_C : NODE_B
    const forwardingNode = requireNode(env, forwardingNodeId)
    await expectExactlyOneWritablePrimary(env, promotedNodeId, '2')
    await assertItemVisible(promotedNode, 1)
    await forwardingNode.reconnectTransport()
    await waitForPeerConnected(forwardingNode, promotedNodeId, 10_000)

    await expectStaleBatchRejectedWithoutMutation(
      env,
      forwardingNode,
      promotedNode,
      promotedNodeId,
      forwardingNodeId,
      70,
    )
    await expectStaleForwardRejectedWithoutMutation(env, forwardingNode, promotedNodeId, 71)
    await expectStaleSyncRequestRejectedWithoutMutation(env, forwardingNode, promotedNodeId, forwardingNodeId, 72)
    await expectStaleSyncBatchRejectedWithoutMutation(env, forwardingNode, promotedNodeId, 73)

    await forwardingNode.execute(
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (10, 1, 'forwarded', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 10, 20_000)
    await executePublicClient(
      env,
      forwardingNodeId,
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (11, 1, 'public-client', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 11, 20_000)
    await executePublicWebSocket(
      env,
      forwardingNodeId,
      "INSERT INTO failover_events (id, item_id, kind, detail) VALUES (12, 1, 'public-ws', 'after-failover')",
    )
    await waitForEvent(env, [promotedNodeId, forwardingNodeId], 12, 20_000)
    await assertItemVisible(forwardingNode, 1)

    await healNodeSirannonLinks(env, NODE_A)
    const restartedA = await startNode(env, NODE_A, 'primary', [NODE_B, NODE_C], false, false)
    await waitForNodeReady(restartedA, 30_000)
    await waitForInSyncSet(env, [NODE_A, promotedNodeId, forwardingNodeId], 30_000)
    await waitForEvent(env, [NODE_A], 10, 20_000)
    await assertHealth(env, promotedNodeId, '2', 'available', 'available')

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: promotedNodeId,
      draining: true,
    })
    await waitForCurrentPrimary(env, NODE_A, 30_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, '3')
    await assertHealth(env, NODE_A, '3', 'available', 'available')

    const safeReplicaAfterMaintenance = promotedNodeId === NODE_B ? NODE_C : NODE_B
    await runSeededSoak(env, NODE_A, [NODE_A, safeReplicaAfterMaintenance])
    await restartReplicaThroughRepair(env, safeReplicaAfterMaintenance, NODE_A)
    await expectResourceUseBelow(env, 256 * 1024 * 1024)

    await env.coordinator.updateInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      inSyncNodeIds: [NODE_A, safeReplicaAfterMaintenance],
    })
    restartedA.kill()
    env.nodes.delete(NODE_A)
    await waitForCurrentPrimary(env, safeReplicaAfterMaintenance, 30_000)
    await expectExactlyOneWritablePrimary(env, safeReplicaAfterMaintenance, '4')
    await waitForRows(env, [safeReplicaAfterMaintenance], 13, 30_000)

    await env.coordinator.setReplicationGroupState({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      votingDataBearingNodeIds: [...NODE_IDS],
      currentPrimary: {
        nodeId: safeReplicaAfterMaintenance,
        endpoint: httpEndpointFor(env, safeReplicaAfterMaintenance),
      },
      primaryTerm: 5n,
      inSyncNodeIds: [safeReplicaAfterMaintenance],
      compatibility: {
        packageVersion: '2.0.0',
        specVersion: '2.0.0',
        protocolVersion: '2.0.0',
      },
    })
    await waitForCurrentPrimary(env, safeReplicaAfterMaintenance, 10_000)
    const finalPrimary = requireNode(env, safeReplicaAfterMaintenance)
    await expectRejectsWith(
      finalPrimary.execute(
        "INSERT INTO failover_items (id, owner, value, note) VALUES (90, 'c', 90, 'incompatible')",
        undefined,
        {
          writeConcern: { level: 'local' },
        },
      ),
      ['PROTOCOL_VERSION_MISMATCH'],
    )

    finalPrimary.kill()
    env.nodes.delete(safeReplicaAfterMaintenance)
    const survivors = [...env.nodes.keys()]
    await waitForNoSafePrimary(env, survivors, 30_000)
    for (const survivorId of survivors) {
      await expectRejectsWith(
        requireNode(env, survivorId).execute(
          "INSERT INTO failover_items (id, owner, value, note) VALUES (80, 'survivor', 80, 'unsafe')",
        ),
        ['TOPOLOGY_ERROR', 'STALE_PRIMARY', 'NO_SAFE_PRIMARY', 'PROTOCOL_VERSION_MISMATCH', 'SYNC_ERROR'],
      )
    }
  })
})
