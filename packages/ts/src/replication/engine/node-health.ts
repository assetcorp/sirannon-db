import type { NodeHealth, NodeHealthReason } from '../../core/types.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import type { SyncPhase } from '../types.js'
import { isCoordinatorConnected } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'

function isSidelined(state: ReplicationGroupState, nodeId: string): boolean {
  return (
    state.drainingNodeIds.includes(nodeId) ||
    state.repairingNodeIds.includes(nodeId) ||
    state.faultedNodeIds.includes(nodeId)
  )
}

function excludingReason(state: ReplicationGroupState, nodeId: string): NodeHealthReason {
  if (state.drainingNodeIds.includes(nodeId)) return 'draining'
  if (state.faultedNodeIds.includes(nodeId)) return 'faulted'
  return 'sync-pending'
}

function staticHealth(engine: ReplicationEngine, phase: SyncPhase): NodeHealth {
  if (phase !== 'ready') {
    return { state: 'unavailable', reason: 'sync-pending', canRead: false, canWrite: false }
  }
  return { state: 'healthy', reason: 'in-sync', canRead: true, canWrite: engine.config.topology.canWrite() }
}

/**
 * Reports what this node alone can do right now.
 *
 * @param engine - The engine whose sync phase, group membership, and coordinator contact are read.
 * @returns The node's health state, the condition behind it, and whether the node can serve a read or accept a write.
 */
export function computeNodeHealth(engine: ReplicationEngine): NodeHealth {
  const phase = engine.syncState.phase
  if (phase === 'syncing' || phase === 'catching-up') {
    return { state: 'syncing', reason: 'sync-pending', canRead: false, canWrite: false }
  }
  if (!engine.isCoordinatorMode()) {
    return staticHealth(engine, phase)
  }

  const state = engine.coordinatorState
  if (!state) {
    return { state: 'unavailable', reason: 'no-group-state', canRead: false, canWrite: false }
  }

  const nodeId = engine.nodeId
  const canRead = phase === 'ready' && !isSidelined(state, nodeId)
  const canWrite = canRead && engine.coordinatorAuthority

  if (state.repairingNodeIds.includes(nodeId)) {
    return { state: 'repairing', reason: 'repairing', canRead, canWrite }
  }
  if (engine.coordinatorAuthority && !canWrite) {
    return { state: 'failing_over', reason: excludingReason(state, nodeId), canRead, canWrite }
  }
  if (!canRead) {
    return { state: 'unavailable', reason: excludingReason(state, nodeId), canRead, canWrite }
  }
  if (!isCoordinatorConnected(engine)) {
    return { state: 'degraded', reason: 'coordinator-unreachable', canRead, canWrite }
  }
  if (!state.inSyncNodeIds.includes(nodeId)) {
    return { state: 'degraded', reason: 'lagging', canRead, canWrite }
  }
  return { state: 'healthy', reason: 'in-sync', canRead, canWrite }
}
