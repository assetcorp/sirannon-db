import { randomUUID } from 'node:crypto'
import { NoSafePrimaryError } from '../errors.js'
import { compatibilityAllowsPromotion } from './compatibility.js'
import type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  ClusterCoordinator,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  CoordinatorCompatibilityMetadata,
  CoordinatorLease,
  CoordinatorNodeSession,
  PromoteEligibleReplicaInput,
  RegisterNodeSessionInput,
  ReplicationGroupState,
  ReplicationGroupWatcher,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './types.js'

const MIN_AUTOMATIC_FAILOVER_VOTERS = 3

export interface InMemoryClusterCoordinatorOptions {
  now?: () => number
  idFactory?: () => string
  onWatcherError?: (error: Error) => void
}

export class InMemoryClusterCoordinator implements ClusterCoordinator {
  private readonly now: () => number
  private readonly idFactory: () => string
  private readonly onWatcherError: ((error: Error) => void) | undefined
  private readonly controllerLeases = new Map<string, CoordinatorLease>()
  private readonly nodeSessions = new Map<string, CoordinatorNodeSession>()
  private readonly replicationGroups = new Map<string, ReplicationGroupState>()
  private readonly replicationGroupWatchers = new Map<string, Set<ReplicationGroupWatcher>>()

  constructor(options: InMemoryClusterCoordinatorOptions = {}) {
    this.now = options.now ?? Date.now
    this.idFactory = options.idFactory ?? randomUUID
    this.onWatcherError = options.onWatcherError
  }

  async tryAcquireControllerLease(input: AcquireControllerLeaseInput): Promise<AcquireControllerLeaseResult> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.holderId, 'holderId')
    assertPositiveTtl(input.ttlMs)

    const current = this.controllerLeases.get(input.clusterId)
    if (current && this.isLeaseLive(current)) {
      return { acquired: false, lease: cloneLease(current) }
    }

    const grantedAtMs = this.now()
    const lease: CoordinatorLease = {
      id: this.idFactory(),
      kind: 'controller',
      clusterId: input.clusterId,
      holderId: input.holderId,
      ttlMs: input.ttlMs,
      grantedAtMs,
      expiresAtMs: grantedAtMs + input.ttlMs,
      metadata: cloneMetadata(input.metadata),
    }
    this.controllerLeases.set(input.clusterId, lease)

    return { acquired: true, lease: cloneLease(lease) }
  }

  async renewLease(leaseId: string, ttlMs: number): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')
    assertPositiveTtl(ttlMs)

    const lease = this.findLease(leaseId)
    if (!lease || !this.isLeaseLive(lease)) {
      return false
    }

    lease.ttlMs = ttlMs
    lease.expiresAtMs = this.now() + ttlMs
    return true
  }

  async releaseLease(leaseId: string): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')

    for (const [clusterId, lease] of this.controllerLeases) {
      if (lease.id === leaseId) {
        this.controllerLeases.delete(clusterId)
        return true
      }
    }

    for (const [key, session] of this.nodeSessions) {
      if (session.lease.id === leaseId) {
        this.nodeSessions.delete(key)
        return true
      }
    }

    return false
  }

  async registerNodeSession(input: RegisterNodeSessionInput): Promise<CoordinatorNodeSession> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.nodeId, 'nodeId')
    assertPositiveTtl(input.ttlMs)

    const grantedAtMs = this.now()
    const lease: CoordinatorLease = {
      id: this.idFactory(),
      kind: 'node-session',
      clusterId: input.clusterId,
      holderId: input.nodeId,
      ttlMs: input.ttlMs,
      grantedAtMs,
      expiresAtMs: grantedAtMs + input.ttlMs,
      metadata: cloneMetadata(input.metadata),
    }
    const session: CoordinatorNodeSession = {
      clusterId: input.clusterId,
      nodeId: input.nodeId,
      lease,
      endpoint: input.endpoint,
      groupIds: [...(input.groupIds ?? [])],
      dataBearing: input.dataBearing ?? true,
      voting: input.voting ?? true,
      compatibility: cloneCompatibility(input.compatibility),
      metadata: cloneMetadata(input.metadata),
    }

    this.nodeSessions.set(nodeSessionKey(input.clusterId, input.nodeId), session)
    return cloneNodeSession(session)
  }

  async getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')

    const session = this.nodeSessions.get(nodeSessionKey(clusterId, nodeId))
    if (!session || !this.isLeaseLive(session.lease)) {
      return null
    }
    return cloneNodeSession(session)
  }

  async deregisterNodeSession(clusterId: string, nodeId: string): Promise<void> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')
    this.nodeSessions.delete(nodeSessionKey(clusterId, nodeId))
  }

  async setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    const votingDataBearingNodeIds = normaliseNodeIds(input.votingDataBearingNodeIds, 'votingDataBearingNodeIds')
    const inSyncNodeIds = normaliseNodeIds(input.inSyncNodeIds ?? [], 'inSyncNodeIds')
    const drainingNodeIds = normaliseNodeIds(input.drainingNodeIds ?? [], 'drainingNodeIds')
    const repairingNodeIds = normaliseNodeIds(input.repairingNodeIds ?? [], 'repairingNodeIds')
    const faultedNodeIds = normaliseNodeIds(input.faultedNodeIds ?? [], 'faultedNodeIds')
    assertNonNegativeTerm(input.primaryTerm ?? 0n)
    assertPrimaryInGroup(input.currentPrimary ?? null, votingDataBearingNodeIds)
    assertSubset(inSyncNodeIds, votingDataBearingNodeIds, 'inSyncNodeIds')
    assertSubset(drainingNodeIds, votingDataBearingNodeIds, 'drainingNodeIds')
    assertSubset(repairingNodeIds, votingDataBearingNodeIds, 'repairingNodeIds')
    assertSubset(faultedNodeIds, votingDataBearingNodeIds, 'faultedNodeIds')

    const state: ReplicationGroupState = {
      clusterId: input.clusterId,
      groupId: input.groupId,
      votingDataBearingNodeIds,
      currentPrimary: input.currentPrimary ? { ...input.currentPrimary } : null,
      primaryTerm: input.primaryTerm ?? 0n,
      inSyncNodeIds,
      drainingNodeIds,
      repairingNodeIds,
      faultedNodeIds,
      compatibility: cloneCompatibility(input.compatibility),
      updatedAtMs: this.now(),
    }
    this.replicationGroups.set(replicationGroupKey(input.clusterId, input.groupId), state)
    this.notifyReplicationGroupWatchers(state)
    return cloneReplicationGroupState(state)
  }

  async getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(groupId, 'groupId')
    const state = this.replicationGroups.get(replicationGroupKey(clusterId, groupId))
    return state ? cloneReplicationGroupState(state) : null
  }

  watchReplicationGroup(clusterId: string, groupId: string, watcher: ReplicationGroupWatcher): () => void {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(groupId, 'groupId')
    const key = replicationGroupKey(clusterId, groupId)
    let watchers = this.replicationGroupWatchers.get(key)
    if (!watchers) {
      watchers = new Set()
      this.replicationGroupWatchers.set(key, watchers)
    }
    watchers.add(watcher)

    return () => {
      const currentWatchers = this.replicationGroupWatchers.get(key)
      if (!currentWatchers) return
      currentWatchers.delete(watcher)
      if (currentWatchers.size === 0) {
        this.replicationGroupWatchers.delete(key)
      }
    }
  }

  async compareAndAdvancePrimaryTerm(
    input: CompareAndAdvancePrimaryTermInput,
  ): Promise<CompareAndAdvancePrimaryTermResult> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonNegativeTerm(input.expectedPrimaryTerm)
    assertNonEmpty(input.nextPrimary.nodeId, 'nextPrimary.nodeId')

    const key = replicationGroupKey(input.clusterId, input.groupId)
    const state = this.replicationGroups.get(key)
    if (!state) {
      return { advanced: false, state: null }
    }
    if (state.primaryTerm !== input.expectedPrimaryTerm) {
      return { advanced: false, state: cloneReplicationGroupState(state) }
    }
    assertPrimaryInGroup(input.nextPrimary, state.votingDataBearingNodeIds)

    movePrimary(state, input.nextPrimary)
    state.updatedAtMs = this.now()
    this.notifyReplicationGroupWatchers(state)

    return { advanced: true, state: cloneReplicationGroupState(state) }
  }

  async updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    const key = replicationGroupKey(input.clusterId, input.groupId)
    const state = this.replicationGroups.get(key)
    if (!state) {
      return null
    }

    const inSyncNodeIds = normaliseNodeIds(input.inSyncNodeIds, 'inSyncNodeIds')
    assertSubset(inSyncNodeIds, state.votingDataBearingNodeIds, 'inSyncNodeIds')
    state.inSyncNodeIds = inSyncNodeIds
    state.updatedAtMs = this.now()
    this.notifyReplicationGroupWatchers(state)
    return cloneReplicationGroupState(state)
  }

  async updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')
    const key = replicationGroupKey(input.clusterId, input.groupId)
    const state = this.replicationGroups.get(key)
    if (!state) {
      return null
    }
    if (!state.votingDataBearingNodeIds.includes(input.nodeId)) {
      throw new RangeError(`Node '${input.nodeId}' is not configured for the replication group`)
    }

    state.drainingNodeIds = setMembership(state.drainingNodeIds, input.nodeId, input.draining)
    state.repairingNodeIds = setMembership(state.repairingNodeIds, input.nodeId, input.repairing)
    state.faultedNodeIds = setMembership(state.faultedNodeIds, input.nodeId, input.faulted)
    if (input.draining === true || input.repairing === true || input.faulted === true) {
      state.inSyncNodeIds = removeNodeId(state.inSyncNodeIds, input.nodeId)
    }
    state.updatedAtMs = this.now()
    this.notifyReplicationGroupWatchers(state)
    return cloneReplicationGroupState(state)
  }

  async promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    const excludedNodeIds = new Set(input.excludeNodeIds ?? [])
    const state = this.replicationGroups.get(replicationGroupKey(input.clusterId, input.groupId))
    if (!state) {
      throw new NoSafePrimaryError(`No replication group '${input.groupId}' is registered`)
    }
    if (state.votingDataBearingNodeIds.length < MIN_AUTOMATIC_FAILOVER_VOTERS) {
      throw new NoSafePrimaryError(
        `Automatic promotion requires at least ${MIN_AUTOMATIC_FAILOVER_VOTERS} voting data-bearing nodes`,
      )
    }

    for (const nodeId of state.votingDataBearingNodeIds) {
      if (nodeId === state.currentPrimary?.nodeId || excludedNodeIds.has(nodeId)) {
        continue
      }
      const session = this.nodeSessions.get(nodeSessionKey(input.clusterId, nodeId))
      if (!this.isEligiblePromotionSession(state, nodeId, session)) {
        continue
      }

      movePrimary(state, session.endpoint ? { nodeId, endpoint: session.endpoint } : { nodeId })
      state.updatedAtMs = this.now()
      this.notifyReplicationGroupWatchers(state)
      return cloneReplicationGroupState(state)
    }

    throw new NoSafePrimaryError(`No safe primary is available for replication group '${input.groupId}'`)
  }

  private findLease(leaseId: string): CoordinatorLease | null {
    for (const lease of this.controllerLeases.values()) {
      if (lease.id === leaseId) {
        return lease
      }
    }
    for (const session of this.nodeSessions.values()) {
      if (session.lease.id === leaseId) {
        return session.lease
      }
    }
    return null
  }

  private isLeaseLive(lease: CoordinatorLease): boolean {
    return lease.expiresAtMs > this.now()
  }

  private notifyReplicationGroupWatchers(state: ReplicationGroupState): void {
    const watchers = this.replicationGroupWatchers.get(replicationGroupKey(state.clusterId, state.groupId))
    if (!watchers) return

    for (const watcher of watchers) {
      try {
        watcher(cloneReplicationGroupState(state))
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.onWatcherError?.(wrappedErr)
      }
    }
  }

  private isEligiblePromotionSession(
    state: ReplicationGroupState,
    nodeId: string,
    session: CoordinatorNodeSession | undefined,
  ): session is CoordinatorNodeSession {
    if (!session || !this.isLeaseLive(session.lease)) {
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
}

function cloneNodeSession(session: CoordinatorNodeSession): CoordinatorNodeSession {
  return {
    ...session,
    lease: cloneLease(session.lease),
    groupIds: [...session.groupIds],
    compatibility: cloneCompatibility(session.compatibility),
    metadata: cloneMetadata(session.metadata),
  }
}

function cloneReplicationGroupState(state: ReplicationGroupState): ReplicationGroupState {
  return {
    ...state,
    votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
    currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
    inSyncNodeIds: [...state.inSyncNodeIds],
    drainingNodeIds: [...state.drainingNodeIds],
    repairingNodeIds: [...state.repairingNodeIds],
    faultedNodeIds: [...state.faultedNodeIds],
    compatibility: cloneCompatibility(state.compatibility),
  }
}

function movePrimary(state: ReplicationGroupState, nextPrimary: { nodeId: string; endpoint?: string }): void {
  const displacedPrimaryId = state.currentPrimary?.nodeId
  state.primaryTerm += 1n
  state.currentPrimary = { ...nextPrimary }
  if (displacedPrimaryId && displacedPrimaryId !== nextPrimary.nodeId) {
    state.inSyncNodeIds = removeNodeId(state.inSyncNodeIds, displacedPrimaryId)
    state.repairingNodeIds = setMembership(state.repairingNodeIds, displacedPrimaryId, true)
  }
}

function cloneLease(lease: CoordinatorLease): CoordinatorLease {
  return {
    ...lease,
    metadata: cloneMetadata(lease.metadata),
  }
}

function cloneCompatibility(
  compatibility: CoordinatorCompatibilityMetadata | undefined,
): CoordinatorCompatibilityMetadata | undefined {
  return compatibility ? { ...compatibility } : undefined
}

function nodeSessionKey(clusterId: string, nodeId: string): string {
  return `${clusterId}\0${nodeId}`
}

function replicationGroupKey(clusterId: string, groupId: string): string {
  return `${clusterId}\0${groupId}`
}

function normaliseNodeIds(nodeIds: string[], name: string): string[] {
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

function setMembership(values: string[], nodeId: string, enabled: boolean | undefined): string[] {
  if (enabled === undefined) return values
  const next = values.filter(value => value !== nodeId)
  if (enabled) {
    next.push(nodeId)
  }
  return next
}

function removeNodeId(values: string[], nodeId: string): string[] {
  return values.filter(value => value !== nodeId)
}

function assertSubset(values: string[], allowed: string[], name: string): void {
  const allowedSet = new Set(allowed)
  for (const value of values) {
    if (!allowedSet.has(value)) {
      throw new RangeError(`${name} contains node id '${value}' that is not configured for the replication group`)
    }
  }
}

function assertPrimaryInGroup(primary: { nodeId: string } | null, votingDataBearingNodeIds: string[]): void {
  if (!primary) return
  assertNonEmpty(primary.nodeId, 'primary.nodeId')
  if (!votingDataBearingNodeIds.includes(primary.nodeId)) {
    throw new RangeError(`Primary node '${primary.nodeId}' is not configured for the replication group`)
  }
}

function assertNonNegativeTerm(term: bigint): void {
  if (term < 0n) {
    throw new RangeError('primaryTerm must not be negative')
  }
}

function cloneMetadata(metadata: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  return metadata ? { ...metadata } : undefined
}

function assertPositiveTtl(ttlMs: number): void {
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
    throw new RangeError('ttlMs must be a positive safe integer')
  }
}

function assertNonEmpty(value: string, name: string): void {
  if (value.length === 0) {
    throw new TypeError(`${name} must not be empty`)
  }
}
