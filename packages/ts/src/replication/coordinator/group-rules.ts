import { compatibilityAllowsPromotion } from './compatibility.js'
import type {
  AdmitNodeToInSyncSetInput,
  CoordinatorCompatibilityMetadata,
  CoordinatorNodeSession,
  ReplicationGroupState,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './types.js'

export const MIN_AUTOMATIC_FAILOVER_VOTERS = 3

export function assertNonEmpty(value: string, name: string): void {
  if (value.length === 0) {
    throw new TypeError(`${name} must not be empty`)
  }
}

export function assertPositiveTtl(ttlMs: number): void {
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
    throw new RangeError('ttlMs must be a positive safe integer')
  }
}

export function assertNonNegativeTerm(term: bigint): void {
  if (term < 0n) {
    throw new RangeError('primaryTerm must not be negative')
  }
}

export function assertNonNegativeSeq(seq: bigint, name: string): void {
  if (seq < 0n) {
    throw new RangeError(`${name} must not be negative`)
  }
}

export function assertSubset(values: string[], allowed: string[], name: string): void {
  const allowedSet = new Set(allowed)
  for (const value of values) {
    if (!allowedSet.has(value)) {
      throw new RangeError(`${name} contains node id '${value}' that is not configured for the replication group`)
    }
  }
}

export function assertPrimaryInGroup(primary: { nodeId: string } | null, votingDataBearingNodeIds: string[]): void {
  if (!primary) return
  assertNonEmpty(primary.nodeId, 'primary.nodeId')
  if (!votingDataBearingNodeIds.includes(primary.nodeId)) {
    throw new RangeError(`Primary node '${primary.nodeId}' is not configured for the replication group`)
  }
}

export function assertNoInSyncAdditions(previous: string[], next: string[]): void {
  const previousSet = new Set(previous)
  for (const nodeId of next) {
    if (!previousSet.has(nodeId)) {
      throw new RangeError(`Node '${nodeId}' cannot be added to the in-sync set without catch-up proof`)
    }
  }
}

export function normaliseNodeIds(nodeIds: string[], name: string): string[] {
  const seen = new Set<string>()
  const result: string[] = []
  for (const nodeId of nodeIds) {
    assertNonEmpty(nodeId, `${name} entry`)
    if (seen.has(nodeId)) {
      throw new RangeError(`${name} contains duplicate node id '${nodeId}'`)
    }
    seen.add(nodeId)
    result.push(nodeId)
  }
  return result
}

export function setMembership(values: string[], nodeId: string, enabled: boolean | undefined): string[] {
  if (enabled === undefined) return values
  const next = values.filter(value => value !== nodeId)
  if (enabled) {
    next.push(nodeId)
  }
  return next
}

export function removeNodeId(values: string[], nodeId: string): string[] {
  return values.filter(value => value !== nodeId)
}

export function arraysEqual(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false
  for (let i = 0; i < left.length; i++) {
    if (left[i] !== right[i]) return false
  }
  return true
}

export function markDisplacedPrimaryForRepair(
  state: ReplicationGroupState,
  displacedPrimaryId: string | undefined,
  nextPrimaryId: string,
): void {
  if (!displacedPrimaryId || displacedPrimaryId === nextPrimaryId) return
  state.inSyncNodeIds = removeNodeId(state.inSyncNodeIds, displacedPrimaryId)
  state.repairingNodeIds = setMembership(state.repairingNodeIds, displacedPrimaryId, true)
}

export function cloneCompatibility(
  compatibility: CoordinatorCompatibilityMetadata | undefined,
): CoordinatorCompatibilityMetadata | undefined {
  return compatibility ? { ...compatibility } : undefined
}

export function cloneMetadata(metadata: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  return metadata ? { ...metadata } : undefined
}

export function isEligiblePromotionSession(
  state: ReplicationGroupState,
  nodeId: string,
  session: CoordinatorNodeSession | null,
): session is CoordinatorNodeSession {
  if (!session) {
    return false
  }
  return (
    session.dataBearing &&
    session.voting &&
    state.inSyncNodeIds.includes(nodeId) &&
    compatibilityAllowsPromotion(state.compatibility, session.compatibility) &&
    !state.drainingNodeIds.includes(nodeId) &&
    !state.repairingNodeIds.includes(nodeId) &&
    !state.faultedNodeIds.includes(nodeId)
  )
}

/**
 * The three membership transitions, shared so the etcd and in-memory
 * coordinators cannot disagree about who is in sync. Each returns the state it
 * was given when the input changes nothing, which is how a caller tells a
 * refused update from an applied one.
 */
export function nextInSyncSetState(
  state: ReplicationGroupState,
  input: UpdateInSyncSetInput,
  nowMs: number,
): ReplicationGroupState {
  const inSyncNodeIds = normaliseNodeIds(input.inSyncNodeIds, 'inSyncNodeIds')
  assertSubset(inSyncNodeIds, state.votingDataBearingNodeIds, 'inSyncNodeIds')
  assertNoInSyncAdditions(state.inSyncNodeIds, inSyncNodeIds)
  const durabilityPointSeq =
    input.durabilityPointSeq !== undefined && input.durabilityPointSeq > state.durabilityPointSeq
      ? input.durabilityPointSeq
      : state.durabilityPointSeq
  if (arraysEqual(inSyncNodeIds, state.inSyncNodeIds) && durabilityPointSeq === state.durabilityPointSeq) {
    return state
  }
  return { ...state, durabilityPointSeq, inSyncNodeIds, updatedAtMs: nowMs }
}

export function nextAdmittedInSyncState(
  state: ReplicationGroupState,
  input: AdmitNodeToInSyncSetInput,
  nowMs: number,
): ReplicationGroupState {
  assertNodeInGroup(state, input.nodeId)
  if (
    state.currentPrimary?.nodeId !== input.sourceNodeId ||
    state.drainingNodeIds.includes(input.nodeId) ||
    state.faultedNodeIds.includes(input.nodeId) ||
    input.appliedSeq < state.durabilityPointSeq
  ) {
    return state
  }
  const inSyncNodeIds = state.inSyncNodeIds.includes(input.nodeId)
    ? state.inSyncNodeIds
    : [...state.inSyncNodeIds, input.nodeId]
  const repairingNodeIds = removeNodeId(state.repairingNodeIds, input.nodeId)
  if (arraysEqual(inSyncNodeIds, state.inSyncNodeIds) && arraysEqual(repairingNodeIds, state.repairingNodeIds)) {
    return state
  }
  return { ...state, inSyncNodeIds, repairingNodeIds, updatedAtMs: nowMs }
}

export function nextMaintenanceState(
  state: ReplicationGroupState,
  input: UpdateNodeMaintenanceInput,
  nowMs: number,
): ReplicationGroupState {
  assertNodeInGroup(state, input.nodeId)
  const leavingService = input.draining === true || input.repairing === true || input.faulted === true
  return {
    ...state,
    drainingNodeIds: setMembership(state.drainingNodeIds, input.nodeId, input.draining),
    repairingNodeIds: setMembership(state.repairingNodeIds, input.nodeId, input.repairing),
    faultedNodeIds: setMembership(state.faultedNodeIds, input.nodeId, input.faulted),
    inSyncNodeIds: leavingService ? removeNodeId(state.inSyncNodeIds, input.nodeId) : state.inSyncNodeIds,
    updatedAtMs: nowMs,
  }
}

function assertNodeInGroup(state: ReplicationGroupState, nodeId: string): void {
  if (!state.votingDataBearingNodeIds.includes(nodeId)) {
    throw new RangeError(`Node '${nodeId}' is not configured for the replication group`)
  }
}

export function buildReplicationGroupState(
  input: SetReplicationGroupStateInput,
  nowMs: number = Date.now(),
): ReplicationGroupState {
  assertNonEmpty(input.clusterId, 'clusterId')
  assertNonEmpty(input.groupId, 'groupId')
  const votingDataBearingNodeIds = normaliseNodeIds(input.votingDataBearingNodeIds, 'votingDataBearingNodeIds')
  const inSyncNodeIds = normaliseNodeIds(input.inSyncNodeIds ?? [], 'inSyncNodeIds')
  const drainingNodeIds = normaliseNodeIds(input.drainingNodeIds ?? [], 'drainingNodeIds')
  const repairingNodeIds = normaliseNodeIds(input.repairingNodeIds ?? [], 'repairingNodeIds')
  const faultedNodeIds = normaliseNodeIds(input.faultedNodeIds ?? [], 'faultedNodeIds')
  assertNonNegativeTerm(input.primaryTerm ?? 0n)
  assertNonNegativeSeq(input.durabilityPointSeq ?? 0n, 'durabilityPointSeq')
  assertPrimaryInGroup(input.currentPrimary ?? null, votingDataBearingNodeIds)
  assertSubset(inSyncNodeIds, votingDataBearingNodeIds, 'inSyncNodeIds')
  assertSubset(drainingNodeIds, votingDataBearingNodeIds, 'drainingNodeIds')
  assertSubset(repairingNodeIds, votingDataBearingNodeIds, 'repairingNodeIds')
  assertSubset(faultedNodeIds, votingDataBearingNodeIds, 'faultedNodeIds')

  return {
    clusterId: input.clusterId,
    groupId: input.groupId,
    votingDataBearingNodeIds,
    currentPrimary: input.currentPrimary ? { ...input.currentPrimary } : null,
    primaryTerm: input.primaryTerm ?? 0n,
    durabilityPointSeq: input.durabilityPointSeq ?? 0n,
    inSyncNodeIds,
    drainingNodeIds,
    repairingNodeIds,
    faultedNodeIds,
    compatibility: cloneCompatibility(input.compatibility),
    updatedAtMs: nowMs,
  }
}

export function cloneReplicationGroupState(state: ReplicationGroupState): ReplicationGroupState {
  return {
    ...state,
    currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
    votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
    durabilityPointSeq: state.durabilityPointSeq,
    inSyncNodeIds: [...state.inSyncNodeIds],
    drainingNodeIds: [...state.drainingNodeIds],
    repairingNodeIds: [...state.repairingNodeIds],
    faultedNodeIds: [...state.faultedNodeIds],
    compatibility: cloneCompatibility(state.compatibility),
  }
}
