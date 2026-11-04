import { compatibilityAllowsPromotion } from '../coordinator/compatibility.js'
import { isGroupMember } from '../coordinator/group-rules.js'
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

const NEWER_TERM_REFRESH_INTERVAL_MS = 250

/**
 * Holds the read of the group state that a message from a later term started, so that a burst of such messages
 * shares one read and starts at most one read every 250 milliseconds.
 *
 * @internal
 */
export class GroupStateRefresh {
  /** Holds the read in flight, or null while none runs. */
  pending: Promise<ReplicationGroupState | null> | null = null
  /** Holds the time in milliseconds at which the last read started, or 0 before the first one. */
  startedAtMs = 0
}

function refreshForNewerTerm(engine: ReplicationEngine): Promise<ReplicationGroupState | null> {
  const refresh = engine.groupStateRefresh
  if (refresh.pending) return refresh.pending
  if (Date.now() - refresh.startedAtMs < NEWER_TERM_REFRESH_INTERVAL_MS) {
    return Promise.resolve(engine.coordinatorState)
  }
  refresh.startedAtMs = Date.now()
  const pending = refreshCoordinatorState(engine).finally(() => {
    refresh.pending = null
  })
  refresh.pending = pending
  return pending
}

/**
 * Checks that a replication message from a peer belongs to this node's group and current term, and that the peer and
 * this node hold the roles that the message needs. A message that carries a later term than the group state that this
 * node holds makes the node read the group state again before it checks, so that a node whose group watch lags behind
 * a promotion answers the new primary, or serves as the new primary, without waiting for a retry.
 *
 * @param engine - The node's engine.
 * @param message - The group ID and term that the message carries.
 * @param fromPeerId - The node ID of the peer that sent the message.
 * @param direction - The kind of message.
 * @returns A promise that settles once the message passes every check, or at once outside coordinator mode.
 * @throws StalePrimaryError when the message belongs to another group or term, or comes from or reaches a node
 * without the primary duty that it needs.
 * @throws TopologyError when a message for the primary comes from a node outside the group.
 * @throws CoordinatorError when the node holds no group state and the coordinator gives none.
 *
 * @internal
 */
export async function assertInboundCoordinatorMessage(
  engine: ReplicationEngine,
  message: { groupId?: string; primaryTerm?: bigint },
  fromPeerId: string,
  direction: 'batch' | 'ack' | 'forward' | 'sync-request' | 'sync-data',
): Promise<void> {
  const config = engine.config.coordinator
  if (!config) return
  let state = engine.coordinatorState ?? (await refreshCoordinatorState(engine))
  if (state && message.groupId === config.groupId && (message.primaryTerm ?? 0n) > state.primaryTerm) {
    state = await refreshForNewerTerm(engine)
  }
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
  if (direction !== 'ack' && direction !== 'forward' && direction !== 'sync-request') return
  if (!hasCurrentPrimaryAuthorityFor(engine, state)) {
    throw new StalePrimaryError(
      `Rejected ${direction} message because this node is not current primary`,
      errorDetails(engine, state),
    )
  }
  if (!isGroupMember(state, fromPeerId)) {
    throw new TopologyError(
      `Rejected ${direction} message from '${fromPeerId}', which is not a member of replication group '${config.groupId}'`,
    )
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
    nodeEndpoints: { ...engine.memberAddresses.clientEndpoints },
    authority: engine.coordinatorAuthority,
    connected,
    controllerState: engine.controllerState,
  }
}
