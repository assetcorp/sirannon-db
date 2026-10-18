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
import type { ConflictResolver, ReplicationStatus, TopologyRole } from '../types.js'
import { DEFAULT_COORDINATOR_SESSION_TTL_MS } from './constants.js'
import type { ReplicationEngine } from './engine.js'
import { assertPrimaryInTerm, updateInSyncSetFromCurrent } from './in-sync-updates.js'
import { ensurePrimaryTermStart } from './term-history.js'

const WRITE_PROGRESS_PURPOSE = 'the progress of a write that it acknowledges'

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

export function noteCoordinatorContact(engine: ReplicationEngine): void {
  engine.coordinatorLastContactMs = Date.now()
}

export function isCoordinatorConnected(engine: ReplicationEngine): boolean {
  const config = engine.config.coordinator
  if (!config) return false
  if (engine.coordinatorLastContactMs === 0) return false
  const sessionTtlMs = config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
  return Date.now() - engine.coordinatorLastContactMs < sessionTtlMs
}

export function effectiveTopologyRole(engine: ReplicationEngine): TopologyRole {
  const state = engine.coordinatorState
  if (!engine.isCoordinatorMode() || !state) return engine.config.topology.role
  return state.currentPrimary?.nodeId === engine.nodeId ? 'primary' : 'replica'
}

/**
 * Returns true when a group state that this node read from the coordinator names another node as primary in a later
 * term than the one in which this node held primary duty. The function records the state on the engine in every case,
 * while it passes only such a state to the engine's demotion handler.
 *
 * The group watch, a session restore, and every refresh call this function, so that the node notices its own demotion
 * whichever of those reads returns the later term first.
 *
 * @param engine - The engine that keeps the group state and the authority flag.
 * @param next - The group state that the coordinator returned, or null when it holds no state for the group.
 * @returns True when the state names another primary in a later term, and false otherwise.
 */
export function applyCoordinatorState(engine: ReplicationEngine, next: ReplicationGroupState | null): boolean {
  const previous = engine.coordinatorState
  const wasPrimary = previous ? hasCurrentPrimaryAuthorityFor(engine, previous) : engine.coordinatorAuthority
  noteCoordinatorContact(engine)
  engine.coordinatorState = next
  engine.coordinatorAuthority = next ? hasCurrentPrimaryAuthorityFor(engine, next) : false
  if (!next || !wasPrimary || engine.coordinatorAuthority) return false
  if (next.primaryTerm <= (previous?.primaryTerm ?? 0n)) return false
  engine.coordinatorDemotionHandler?.(next)
  return true
}

export async function refreshCoordinatorState(engine: ReplicationEngine): Promise<ReplicationGroupState | null> {
  const config = engine.config.coordinator
  if (!config) return null
  try {
    const state = await config.coordinator.getReplicationGroupState(config.clusterId, config.groupId)
    applyCoordinatorState(engine, state)
    return state
  } catch (err: unknown) {
    engine.coordinatorAuthority = false
    throw new CoordinatorError('Coordinator is unavailable while refreshing replication-group authority', {
      ...errorDetails(engine, engine.coordinatorState),
      cause: err instanceof Error ? err.message : String(err),
    })
  }
}

export function resolverFor(engine: ReplicationEngine, table?: string): ConflictResolver {
  const specific = table ? engine.config.conflictResolvers?.[table] : undefined
  return specific ?? engine.defaultResolver
}

export function resolveWriteConcern(
  engine: ReplicationEngine,
  wc: { level: string; timeoutMs?: number } | undefined,
): { level: string; timeoutMs?: number } | undefined {
  if (wc) return wc
  return engine.isCoordinatorMode() ? { level: 'majority' } : undefined
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

/**
 * Updates the group state once a write meets its write concern. This node keeps in the in-sync set only the voters
 * that acknowledged the write and raises the durability point to the write's position. It then admits every
 * acknowledging voter outside the set, except a repairing or faulted voter, which rejoins through its own copy. Each
 * update takes effect only while this node is still the primary in the term in which it accepted the write.
 *
 * @param engine - The engine of the primary that accepted the write.
 * @param seq - The write's position in this node's change log.
 * @param state - The group state under which this node accepted the write.
 * @throws StalePrimaryError when the term or primary duty passes to another node before every update takes effect.
 *
 * @internal
 */
export async function updateCoordinatorProgressAfterWrite(
  engine: ReplicationEngine,
  seq: bigint,
  state: ReplicationGroupState,
): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  const term = state.primaryTerm
  const ackedNodeIds = engine.peerTracker.ackedConfiguredNodeIds(seq, engine.nodeId, state.votingDataBearingNodeIds)
  let nextState = await updateInSyncSetFromCurrent(engine, {
    purpose: WRITE_PROGRESS_PURPOSE,
    known: engine.coordinatorState ?? state,
    primaryTerm: term,
    change: base => {
      const retained = base.inSyncNodeIds.filter(nodeId => ackedNodeIds.includes(nodeId))
      if (arraysEqual(retained, base.inSyncNodeIds) && base.durabilityPointSeq >= seq) return null
      return { inSyncNodeIds: retained, durabilityPointSeq: seq }
    },
  })

  for (const nodeId of ackedNodeIds) {
    if (nodeId === engine.nodeId || nextState.inSyncNodeIds.includes(nodeId)) continue
    if (nextState.repairingNodeIds.includes(nodeId) || nextState.faultedNodeIds.includes(nodeId)) continue
    const admitted = await config.coordinator.admitNodeToInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      nodeId,
      sourceNodeId: engine.nodeId,
      appliedSeq: seq,
      expectedPrimaryTerm: term,
    })
    if (!admitted) {
      throw new CoordinatorError(
        'The coordinator holds no state for this replication group, so this node cannot admit the voters that acknowledged a write',
        errorDetails(engine, nextState),
      )
    }
    applyCoordinatorState(engine, admitted)
    assertPrimaryInTerm(engine, admitted, term, WRITE_PROGRESS_PURPOSE)
    nextState = admitted
  }
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
    if (engine.history.termStartTerm !== state.primaryTerm) await ensurePrimaryTermStart(engine)
    if (wc.level !== 'majority' && wc.level !== 'all') return
    assertPrimaryInTerm(engine, state, state.primaryTerm, WRITE_PROGRESS_PURPOSE)
    if (engine.history.termStartTerm !== state.primaryTerm) {
      throw new StalePrimaryError(
        `This node has not recorded the start of term ${state.primaryTerm}, so it cannot prove its authority for the write`,
        errorDetails(engine, state),
      )
    }
    if (wc.level === 'majority') {
      await engine.peerTracker.waitForConfiguredMajority(seq, engine.nodeId, state.votingDataBearingNodeIds, timeout)
    } else {
      const eligibleVoters = state.votingDataBearingNodeIds.filter(nodeId => !state.drainingNodeIds.includes(nodeId))
      await engine.peerTracker.waitForConfiguredAll(seq, engine.nodeId, eligibleVoters, timeout)
    }
    await updateCoordinatorProgressAfterWrite(engine, seq, state)
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
  const connected = isCoordinatorConnected(engine)
  return {
    ...(connected && engine.liveNodeIds ? { liveNodeIds: [...engine.liveNodeIds] } : {}),
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
    connected,
    controllerState: engine.controllerState,
  }
}
