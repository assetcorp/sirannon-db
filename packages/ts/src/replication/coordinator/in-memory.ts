import { randomUUID } from 'node:crypto'
import { NoSafePrimaryError } from '../errors.js'
import {
  assertNonEmpty,
  assertNonNegativeSeq,
  assertNonNegativeTerm,
  assertPositiveTtl,
  assertPrimaryInGroup,
  buildReplicationGroupState,
  cloneCompatibility,
  cloneMetadata,
  cloneReplicationGroupState,
  isEligiblePromotionSession,
  MIN_AUTOMATIC_FAILOVER_VOTERS,
  markDisplacedPrimaryForRepair,
  nextAdmittedInSyncState,
  nextInSyncSetState,
  nextMaintenanceState,
} from './group-rules.js'
import type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  AdmitNodeToInSyncSetInput,
  ClusterCoordinator,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
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
    const state = buildReplicationGroupState(input, this.now())
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
    if (input.durabilityPointSeq !== undefined) {
      assertNonNegativeSeq(input.durabilityPointSeq, 'durabilityPointSeq')
    }
    return this.applyGroupTransition(input.clusterId, input.groupId, state =>
      nextInSyncSetState(state, input, this.now()),
    )
  }

  async admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')
    assertNonEmpty(input.sourceNodeId, 'sourceNodeId')
    assertNonNegativeSeq(input.appliedSeq, 'appliedSeq')

    return this.applyGroupTransition(input.clusterId, input.groupId, state =>
      nextAdmittedInSyncState(state, input, this.now()),
    )
  }

  async updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')

    return this.applyGroupTransition(input.clusterId, input.groupId, state =>
      nextMaintenanceState(state, input, this.now()),
    )
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
      if (!this.isPromotable(state, nodeId, session)) {
        continue
      }

      movePrimary(state, session.endpoint ? { nodeId, endpoint: session.endpoint } : { nodeId })
      state.updatedAtMs = this.now()
      this.notifyReplicationGroupWatchers(state)
      return cloneReplicationGroupState(state)
    }

    throw new NoSafePrimaryError(`No safe primary is available for replication group '${input.groupId}'`)
  }

  private applyGroupTransition(
    clusterId: string,
    groupId: string,
    transition: (state: ReplicationGroupState) => ReplicationGroupState,
  ): ReplicationGroupState | null {
    const key = replicationGroupKey(clusterId, groupId)
    const state = this.replicationGroups.get(key)
    if (!state) {
      return null
    }
    const next = transition(state)
    if (next === state) {
      return cloneReplicationGroupState(state)
    }
    this.replicationGroups.set(key, next)
    this.notifyReplicationGroupWatchers(next)
    return cloneReplicationGroupState(next)
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

  private isPromotable(
    state: ReplicationGroupState,
    nodeId: string,
    session: CoordinatorNodeSession | undefined,
  ): session is CoordinatorNodeSession {
    if (!session || !this.isLeaseLive(session.lease)) {
      return false
    }
    return isEligiblePromotionSession(state, nodeId, session)
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

function cloneLease(lease: CoordinatorLease): CoordinatorLease {
  return {
    ...lease,
    metadata: cloneMetadata(lease.metadata),
  }
}

function movePrimary(state: ReplicationGroupState, nextPrimary: { nodeId: string; endpoint?: string }): void {
  const displacedPrimaryId = state.currentPrimary?.nodeId
  state.primaryTerm += 1n
  state.currentPrimary = { ...nextPrimary }
  markDisplacedPrimaryForRepair(state, displacedPrimaryId, nextPrimary.nodeId)
}

function nodeSessionKey(clusterId: string, nodeId: string): string {
  return `${clusterId}\0${nodeId}`
}

function replicationGroupKey(clusterId: string, groupId: string): string {
  return `${clusterId}\0${groupId}`
}
