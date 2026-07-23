import { compatibilityAllowsPromotion } from '../coordinator/compatibility.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import {
  AuthorityError,
  CoordinatorError,
  NodeDrainingError,
  NodeNotInSyncError,
  ProtocolVersionMismatchError,
  ReadConcernError,
  StalePrimaryError,
  TopologyError,
} from '../errors.js'
import type { ReplicationStatus } from '../types.js'
import type { ReplicationEngine } from './engine.js'

export function arraysEqual(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false
  for (let i = 0; i < left.length; i++) {
    if (left[i] !== right[i]) return false
  }
  return true
}

export function errorDetails(engine: ReplicationEngine, state: ReplicationGroupState | null): Record<string, unknown> {
  return {
    currentPrimary: state?.currentPrimary ?? null,
    primaryTerm: state?.primaryTerm.toString(),
    replicationGroupId: engine.config.coordinator?.groupId,
  }
}

export function hasCurrentPrimaryAuthorityFor(engine: ReplicationEngine, state: ReplicationGroupState): boolean {
  return state.currentPrimary?.nodeId === engine.nodeId
}

export async function refreshCoordinatorState(engine: ReplicationEngine): Promise<ReplicationGroupState | null> {
  const config = engine.config.coordinator
  if (!config) return null
  try {
    const state = await config.coordinator.getReplicationGroupState(config.clusterId, config.groupId)
    engine.coordinatorState = state
    engine.coordinatorAuthority = state ? hasCurrentPrimaryAuthorityFor(engine, state) : false
    return state
  } catch {
    engine.coordinatorAuthority = false
    throw new CoordinatorError(
      'Coordinator is unavailable while refreshing replication-group authority',
      errorDetails(engine, engine.coordinatorState),
    )
  }
}

export function getCoordinatorMessageFields(engine: ReplicationEngine): { groupId?: string; primaryTerm?: bigint } {
  const config = engine.config.coordinator
  if (!config) return {}
  return {
    groupId: config.groupId,
    primaryTerm: engine.coordinatorState?.primaryTerm ?? engine.config.transportConfig?.primaryTerm,
  }
}

export function getForwardingPrimaryPeerId(engine: ReplicationEngine): string | null {
  if (engine.isCoordinatorMode()) {
    const primaryId = engine.coordinatorState?.currentPrimary?.nodeId
    if (primaryId && engine.config.transport.peers().has(primaryId)) {
      return primaryId
    }
    return null
  }

  for (const [peerId, info] of engine.config.transport.peers()) {
    if (info.role === 'primary') {
      return peerId
    }
  }
  return null
}

export function assertLocalCompatibility(engine: ReplicationEngine, state: ReplicationGroupState): void {
  if (compatibilityAllowsPromotion(state.compatibility, engine.config.coordinator?.compatibility)) return
  throw new ProtocolVersionMismatchError('Node compatibility metadata is incompatible with the replication group', {
    ...errorDetails(engine, state),
    groupCompatibility: state.compatibility,
    localCompatibility: engine.config.coordinator?.compatibility,
  })
}

export async function canAcceptLocalWrite(engine: ReplicationEngine): Promise<boolean> {
  if (!engine.isCoordinatorMode()) {
    return engine.config.topology.canWrite()
  }
  const state = await refreshCoordinatorState(engine)
  if (!state) {
    throw new CoordinatorError('Cannot prove write authority without replication-group state')
  }
  if (!hasCurrentPrimaryAuthorityFor(engine, state)) {
    return false
  }
  assertLocalCompatibility(engine, state)
  if (state.drainingNodeIds.includes(engine.nodeId)) {
    throw new NodeDrainingError('Node is draining and cannot accept writes', errorDetails(engine, state))
  }
  if (state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)) {
    throw new AuthorityError(
      'Node is not eligible to accept writes while repair or fault state is active',
      'AUTHORITY_LOST',
      errorDetails(engine, state),
    )
  }
  return true
}

export function throwNotCurrentPrimary(engine: ReplicationEngine): never {
  if (engine.isCoordinatorMode()) {
    throw new StalePrimaryError('This node is not the current primary', errorDetails(engine, engine.coordinatorState))
  }
  throw new TopologyError('This node cannot accept writes')
}

export async function assertReadConcern(engine: ReplicationEngine, level: string | undefined): Promise<void> {
  if (!engine.isCoordinatorMode()) return
  const readConcern = level ?? 'majority'
  if (readConcern === 'local') return
  const state = await refreshCoordinatorState(engine)
  if (!state) {
    throw new CoordinatorError('Cannot satisfy read concern without replication-group state')
  }
  if (readConcern === 'linearizable') {
    await verifyPrimaryAuthority(engine)
    return
  }
  if (readConcern === 'majority') {
    if (!state.inSyncNodeIds.includes(engine.nodeId)) {
      throw new NodeNotInSyncError('Node is not in the in-sync set for majority reads', errorDetails(engine, state))
    }
    if (state.drainingNodeIds.includes(engine.nodeId) || state.repairingNodeIds.includes(engine.nodeId)) {
      throw new ReadConcernError(
        'Node cannot serve majority reads while draining or repairing',
        errorDetails(engine, state),
      )
    }
    return
  }
  throw new ReadConcernError(`Unsupported read concern '${readConcern}'`, errorDetails(engine, state))
}

export async function verifyPrimaryAuthority(engine: ReplicationEngine): Promise<ReplicationGroupState> {
  const state = await refreshCoordinatorState(engine)
  if (!state) {
    throw new CoordinatorError('Cannot prove primary authority without replication-group state')
  }
  if (!hasCurrentPrimaryAuthorityFor(engine, state)) {
    throw new StalePrimaryError(
      'Node is not the current primary for this replication group',
      errorDetails(engine, state),
    )
  }
  assertLocalCompatibility(engine, state)
  if (state.drainingNodeIds.includes(engine.nodeId)) {
    throw new NodeDrainingError('Node is draining and cannot accept writes', errorDetails(engine, state))
  }
  if (state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)) {
    throw new AuthorityError(
      'Node is not eligible to accept writes while repair or fault state is active',
      'AUTHORITY_LOST',
      errorDetails(engine, state),
    )
  }
  return state
}

export async function assertInboundCoordinatorMessage(
  engine: ReplicationEngine,
  message: { groupId?: string; primaryTerm?: bigint },
  fromPeerId: string,
  direction: 'batch' | 'ack' | 'forward' | 'sync-request' | 'sync-data',
): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (!state) {
    throw new CoordinatorError('Cannot validate coordinator message without replication-group state')
  }
  if (message.groupId !== config.groupId || message.primaryTerm !== state.primaryTerm) {
    throw new StalePrimaryError(
      `Rejected ${direction} message for stale or wrong replication group term`,
      errorDetails(engine, state),
    )
  }
  if ((direction === 'batch' || direction === 'sync-data') && fromPeerId !== state.currentPrimary?.nodeId) {
    throw new StalePrimaryError(
      `Rejected ${direction} message from non-current primary '${fromPeerId}'`,
      errorDetails(engine, state),
    )
  }
  if (
    (direction === 'ack' || direction === 'forward' || direction === 'sync-request') &&
    !hasCurrentPrimaryAuthorityFor(engine, state)
  ) {
    throw new StalePrimaryError(
      `Rejected ${direction} message because this node is not current primary`,
      errorDetails(engine, state),
    )
  }
}

export async function updateCoordinatorProgressAfterWrite(
  engine: ReplicationEngine,
  seq: bigint,
  state: ReplicationGroupState,
): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const ackedNodeIds = engine.peerTracker.ackedConfiguredNodeIds(seq, engine.nodeId, state.votingDataBearingNodeIds)
  const retainedInSync = state.inSyncNodeIds.filter(nodeId => ackedNodeIds.includes(nodeId))
  let nextState = state

  if (!arraysEqual(retainedInSync, state.inSyncNodeIds) || state.durabilityPointSeq < seq) {
    const updated = await config.coordinator.updateInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      inSyncNodeIds: retainedInSync,
      durabilityPointSeq: seq,
    })
    if (!updated) {
      throw new CoordinatorError('Failed to update in-sync set before acknowledging majority write')
    }
    nextState = updated
  }

  for (const nodeId of ackedNodeIds) {
    if (nextState.inSyncNodeIds.includes(nodeId)) continue
    if (nodeId === engine.nodeId) continue
    const admitted = await config.coordinator.admitNodeToInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      nodeId,
      sourceNodeId: engine.nodeId,
      appliedSeq: seq,
    })
    if (!admitted) {
      throw new CoordinatorError('Failed to admit ACKing node to in-sync set before acknowledging majority write')
    }
    nextState = admitted
  }

  engine.coordinatorState = nextState
}

export async function waitForWriteConcern(
  engine: ReplicationEngine,
  seq: bigint,
  wc: { level: string; timeoutMs?: number },
): Promise<void> {
  const timeout = wc.timeoutMs ?? 5000

  if (engine.isCoordinatorMode()) {
    const state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
    if (!state) {
      throw new CoordinatorError('Cannot satisfy write concern without replication-group state')
    }
    if (wc.level === 'majority') {
      await engine.peerTracker.waitForConfiguredMajority(seq, engine.nodeId, state.votingDataBearingNodeIds, timeout)
      await updateCoordinatorProgressAfterWrite(engine, seq, state)
    } else if (wc.level === 'all') {
      const eligibleVoters = state.votingDataBearingNodeIds.filter(nodeId => !state.drainingNodeIds.includes(nodeId))
      await engine.peerTracker.waitForConfiguredAll(seq, engine.nodeId, eligibleVoters, timeout)
      await updateCoordinatorProgressAfterWrite(engine, seq, state)
    }
    return
  }

  if (wc.level === 'majority') {
    await engine.peerTracker.waitForMajority(seq, timeout)
  } else if (wc.level === 'all') {
    await engine.peerTracker.waitForAll(seq, timeout)
  }
}

export function getCoordinatorRuntimeStatus(engine: ReplicationEngine): ReplicationStatus['coordinator'] {
  const config = engine.config.coordinator
  const state = engine.coordinatorState
  if (!config || !state) return undefined
  return {
    clusterId: config.clusterId,
    groupId: config.groupId,
    currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
    primaryTerm: state.primaryTerm,
    inSyncNodeIds: [...state.inSyncNodeIds],
    drainingNodeIds: [...state.drainingNodeIds],
    repairingNodeIds: [...state.repairingNodeIds],
    faultedNodeIds: [...state.faultedNodeIds],
    votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
    authority: engine.coordinatorAuthority,
    controllerState: engine.controllerState,
  }
}
