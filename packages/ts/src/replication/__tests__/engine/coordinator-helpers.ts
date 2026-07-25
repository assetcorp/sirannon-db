import type { SQLiteConnection } from '../../../core/driver/types.js'
import { prepareInsertAppliedChange } from '../../../core/system-catalog/applied-changes-table.js'
import type { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import type { ReplicationGroupState } from '../../coordinator/types.js'
import type { EngineTestHarness } from './helpers.js'

export const NODE_C = 'cccc0000cccc0000cccc0000cccc0000'

export async function waitForCurrentPrimary(
  coordinator: InMemoryClusterCoordinator,
  nodeId: string,
  timeoutMs: number = 250,
): Promise<ReplicationGroupState> {
  const startedAt = Date.now()
  let latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
  while (Date.now() - startedAt < timeoutMs) {
    latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    if (latest?.currentPrimary?.nodeId === nodeId) {
      return latest
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for primary '${nodeId}'`)
}

export async function waitForInSyncNodes(
  coordinator: InMemoryClusterCoordinator,
  nodeIds: string[],
  timeoutMs: number = 250,
): Promise<ReplicationGroupState> {
  const startedAt = Date.now()
  let latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
  while (Date.now() - startedAt < timeoutMs) {
    latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    const state = latest
    if (state && nodeIds.every(nodeId => state.inSyncNodeIds.includes(nodeId))) {
      return state
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for in-sync nodes '${nodeIds.join(', ')}'`)
}

export async function waitForSyncRequest(
  transport: EngineTestHarness['transport'],
  peerId: string,
  timeoutMs: number = 250,
) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    const request = transport.sentSyncRequests.find(entry => entry.peerId === peerId)
    if (request) {
      return request
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for sync request to '${peerId}'`)
}

export async function recordAppliedSeq(conn: SQLiteConnection, sourceNodeId: string, seq: bigint) {
  const stmt = await prepareInsertAppliedChange(conn)
  await stmt.run(sourceNodeId, seq.toString(), Date.now() / 1000)
}
