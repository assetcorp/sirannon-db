import { waitForRows } from './gate-assertions.js'
import {
  CLUSTER_ID,
  type GateEnvironment,
  GROUP_ID,
  grpcProxyName,
  healNodeSirannonLinks,
  NODE_IDS,
  requireNode,
  startNode,
} from './gate-environment.js'
import { collectNodeDiagnostics, waitForNodeReady, waitForPeerConnected } from './gate-observations.js'
import { executeMajority } from './gate-public-api.js'
import { jsonReplacer, waitForCondition } from './gate-support.js'
import type { FailoverNodeProcess } from './node-process.js'

export async function restartReplicaThroughRepair(
  environment: GateEnvironment,
  nodeId: string,
  currentPrimaryNodeId: string,
): Promise<FailoverNodeProcess> {
  await environment.coordinator.updateNodeMaintenance({
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    nodeId,
    draining: false,
    repairing: true,
    faulted: false,
  })
  const existing = environment.nodes.get(nodeId)
  if (existing) {
    existing.kill()
    environment.nodes.delete(nodeId)
    await waitForCondition(async () => {
      const session = await environment.coordinator.getLiveNodeSession(CLUSTER_ID, nodeId)
      return session === null
    }, 5_000)
  }
  await healNodeSirannonLinks(environment, nodeId)
  const restarted = await startNode(
    environment,
    nodeId,
    'replica',
    NODE_IDS.filter(targetNodeId => targetNodeId !== nodeId),
    false,
    false,
  )
  await waitForNodeReady(restarted, 30_000)
  await waitForPeerConnected(restarted, currentPrimaryNodeId, 10_000)
  let latestState: unknown = null
  try {
    await waitForCondition(async () => {
      latestState = await environment.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      return (latestState as { inSyncNodeIds?: string[] } | null)?.inSyncNodeIds?.includes(nodeId) ?? false
    }, 30_000)
  } catch (err: unknown) {
    const diagnostics = await collectNodeDiagnostics(environment)
    const message = err instanceof Error ? err.message : String(err)
    throw new Error(
      `Timed out waiting for repaired replica ${nodeId} to re-enter in-sync set: ${message}\nstate=${JSON.stringify(
        latestState,
        jsonReplacer,
        2,
      )}\ndiagnostics=${JSON.stringify(diagnostics, jsonReplacer, 2)}`,
    )
  }
  return restarted
}

export async function runSeededSoak(
  environment: GateEnvironment,
  primaryNodeId: string,
  expectedNodeIds: readonly string[],
): Promise<void> {
  const primary = requireNode(environment, primaryNodeId)
  const latencyNodeId = expectedNodeIds.find(nodeId => nodeId !== primaryNodeId) ?? primaryNodeId
  const proxyName = grpcProxyName(latencyNodeId, primaryNodeId)
  await environment.toxiproxy.addLatency(proxyName, 'seeded-soak-latency', 40)
  try {
    for (let index = 0; index < 12; index++) {
      const id = 20 + index
      await executeMajority(primary, id, `seeded-soak-${index}`)
      if (index % 3 === 0) {
        await primary.execute('UPDATE failover_items SET value = value + 1 WHERE id = ?', [id], {
          writeConcern: { level: 'majority', timeoutMs: 15_000 },
        })
      }
    }
  } finally {
    await environment.toxiproxy.clearToxic(proxyName, 'seeded-soak-latency').catch(() => undefined)
  }
  await waitForRows(environment, expectedNodeIds, 13, 30_000)
}
