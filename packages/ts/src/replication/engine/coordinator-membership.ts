import type { ReplicationGroupState } from '../coordinator/types.js'
import { AuthorityError, CoordinatorError } from '../errors.js'
import {
  arraysEqual,
  errorDetails,
  hasCurrentPrimaryAuthorityFor,
  refreshCoordinatorState,
} from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'

export function requiresCoordinatorRejoin(engine: ReplicationEngine, state: ReplicationGroupState): boolean {
  if (state.currentPrimary?.nodeId === engine.nodeId) return false
  return state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)
}

export function requiresCoordinatorRejoinSync(engine: ReplicationEngine, state: ReplicationGroupState | null): boolean {
  if (!state || state.currentPrimary?.nodeId === engine.nodeId) return false
  if (state.faultedNodeIds.includes(engine.nodeId)) return false
  return state.repairingNodeIds.includes(engine.nodeId)
}

export async function prepareCoordinatorRejoinIfNeeded(engine: ReplicationEngine): Promise<void> {
  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!state || !requiresCoordinatorRejoin(engine, state)) return
  await evaluateFormerPrimaryHistory(engine, state)
}

export async function markCoordinatorSyncReady(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!config || !state || state.currentPrimary?.nodeId === engine.nodeId) return
  if (!state.votingDataBearingNodeIds.includes(engine.nodeId)) return
  if (state.faultedNodeIds.includes(engine.nodeId)) return

  if (state.drainingNodeIds.includes(engine.nodeId)) {
    await removeLocalNodeFromCoordinatorInSyncSet(engine, state)
    return
  }

  const needsAdmission = !state.inSyncNodeIds.includes(engine.nodeId) || state.repairingNodeIds.includes(engine.nodeId)
  if (!needsAdmission) {
    engine.coordinatorState = state
    engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, state)
    return
  }

  const sourceNodeId = state.currentPrimary?.nodeId
  if (!sourceNodeId || engine.syncState.sourcePeerId !== sourceNodeId) {
    engine.coordinatorState = state
    engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, state)
    return
  }

  const appliedSeq = await engine.log.getLastAppliedSeq(sourceNodeId)
  if (appliedSeq < state.durabilityPointSeq) {
    engine.coordinatorState = state
    engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, state)
    return
  }

  const admitted = await config.coordinator.admitNodeToInSyncSet({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    sourceNodeId,
    appliedSeq,
  })
  engine.coordinatorState = admitted ?? state
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, engine.coordinatorState)
}

export async function handleCoordinatorAckProgress(
  engine: ReplicationEngine,
  nodeId: string,
  ackedSeq: bigint,
): Promise<void> {
  const config = engine.config.coordinator
  const state = engine.coordinatorState
  if (!config || !state) return
  if (nodeId === engine.nodeId) return
  if (!hasCurrentPrimaryAuthorityFor(engine, state)) return
  if (state.inSyncNodeIds.includes(nodeId)) return
  if (!state.votingDataBearingNodeIds.includes(nodeId)) return
  if (ackedSeq < state.durabilityPointSeq) return

  const admitted = await config.coordinator.admitNodeToInSyncSet({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId,
    sourceNodeId: engine.nodeId,
    appliedSeq: ackedSeq,
  })
  if (!admitted) return

  engine.coordinatorState = admitted
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, admitted)
}

async function removeLocalNodeFromCoordinatorInSyncSet(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const inSyncNodeIds = state.inSyncNodeIds.filter(nodeId => nodeId !== engine.nodeId)
  let nextState = state
  if (!arraysEqual(inSyncNodeIds, state.inSyncNodeIds)) {
    const updated = await config.coordinator.updateInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      inSyncNodeIds,
    })
    if (!updated) {
      throw new CoordinatorError('Failed to mark repaired node as in sync')
    }
    nextState = updated
  }

  engine.coordinatorState = nextState
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, nextState)
}

export function startCoordinatorRejoinSyncIfReady(engine: ReplicationEngine, state: ReplicationGroupState): void {
  if (!engine.running) return
  if (engine.coordinatorRejoinSyncStarting) return
  if (!requiresCoordinatorRejoinSync(engine, state)) return
  if (engine.syncState.phase !== 'pending') return
  const sourceNodeId = state.currentPrimary?.nodeId
  if (!sourceNodeId || !engine.config.transport.peers().has(sourceNodeId)) return

  engine.coordinatorRejoinSyncStarting = true
  engine.syncJoiner
    .initiateSync()
    .catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'coordinator-rejoin-sync', recoverable: true })
    })
    .finally(() => {
      engine.coordinatorRejoinSyncStarting = false
    })
}

export async function handleFormerPrimaryDemotion(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<void> {
  const repairingState = await markLocalNodeRepairing(engine, state)
  const evaluatedState = await evaluateFormerPrimaryHistory(engine, repairingState)
  if (!requiresCoordinatorRejoinSync(engine, evaluatedState)) return
  if (!engine.running || engine.syncState.phase === 'syncing' || engine.syncState.phase === 'catching-up') return
  engine.syncState = {
    phase: 'pending',
    sourcePeerId: null,
    snapshotSeq: null,
    completedTables: [],
    totalTables: 0,
    startedAt: null,
    error: null,
  }
  await engine.log.setSyncMeta('pending')
  await engine.syncJoiner.initiateSync()
}

async function markLocalNodeRepairing(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  const config = engine.config.coordinator
  if (!config || state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)) {
    return state
  }
  const updated = await config.coordinator.updateNodeMaintenance({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    repairing: true,
  })
  if (!updated) {
    throw new CoordinatorError('Failed to mark former primary for repair')
  }
  engine.coordinatorState = updated
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, updated)
  return updated
}

async function evaluateFormerPrimaryHistory(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  const config = engine.config.coordinator
  const currentPrimary = state.currentPrimary
  if (!config || !currentPrimary || state.faultedNodeIds.includes(engine.nodeId)) return state

  const localSeq = await engine.log.getLocalSeq()
  const currentPrimaryAckedSeq = await engine.log.getPeerAckedSeq(currentPrimary.nodeId)
  if (localSeq <= currentPrimaryAckedSeq) {
    return state
  }

  const inSyncNodeIds = state.inSyncNodeIds.filter(nodeId => nodeId !== engine.nodeId)
  let nextState = state
  if (inSyncNodeIds.length !== state.inSyncNodeIds.length) {
    const updated = await config.coordinator.updateInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      inSyncNodeIds,
    })
    if (!updated) {
      throw new CoordinatorError('Failed to remove divergent former primary from in-sync set')
    }
    nextState = updated
  }

  const faulted = await config.coordinator.updateNodeMaintenance({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    repairing: false,
    faulted: true,
  })
  nextState = faulted ?? nextState
  engine.coordinatorState = nextState
  engine.coordinatorAuthority = false
  engine.emitError({
    error: new AuthorityError('Former primary returned with local-only writes and was quarantined', 'AUTHORITY_LOST', {
      ...errorDetails(engine, nextState),
      localSeq: localSeq.toString(),
      currentPrimaryAckedSeq: currentPrimaryAckedSeq.toString(),
    }),
    operation: 'coordinator-former-primary-divergence',
    recoverable: false,
  })
  return nextState
}
