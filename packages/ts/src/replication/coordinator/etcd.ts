import { Etcd3, type IOptions, type Lease, type Namespace, type Watcher } from 'etcd3'
import { CoordinatorError, NoSafePrimaryError } from '../errors.js'
import { compatibilityAllowsPromotion } from './compatibility.js'
import type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  AdmitNodeToInSyncSetInput,
  ClusterCoordinator,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  CoordinatorCompatibilityMetadata,
  CoordinatorLease,
  CoordinatorNodeSession,
  CoordinatorPrimary,
  CoordinatorWatchDisposer,
  PromoteEligibleReplicaInput,
  RegisterNodeSessionInput,
  ReplicationGroupState,
  ReplicationGroupWatcher,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './types.js'

const MIN_AUTOMATIC_FAILOVER_VOTERS = 3

export interface EtcdClusterCoordinatorOptions {
  hosts: string | string[]
  keyPrefix: string
  credentials?: IOptions['credentials']
  auth?: IOptions['auth']
  grpcOptions?: IOptions['grpcOptions']
  dialTimeoutMs?: number
  defaultCallTimeoutMs?: number
  allowInsecure?: boolean
  onWatcherError?: (error: Error) => void
}

interface LocalLeaseEntry {
  lease: Lease
  leaseId: string
  key: string
  ttlMs: number
  ttlSeconds: number
  kind: 'controller' | 'node-session'
  clusterId: string
  holderId: string
  metadata?: Record<string, unknown>
  nodeSession?: Omit<SerializedNodeSession, 'lease'>
}

interface SerializedLease {
  id: string
  kind: 'controller' | 'node-session'
  clusterId: string
  holderId: string
  ttlMs: number
  grantedAtMs: number
  expiresAtMs: number
  metadata?: Record<string, unknown>
}

interface SerializedNodeSession {
  clusterId: string
  nodeId: string
  lease: SerializedLease
  endpoint?: string
  groupIds: string[]
  dataBearing: boolean
  voting: boolean
  compatibility?: CoordinatorCompatibilityMetadata
  metadata?: Record<string, unknown>
}

interface SerializedReplicationGroupState {
  clusterId: string
  groupId: string
  votingDataBearingNodeIds: string[]
  currentPrimary: CoordinatorPrimary | null
  primaryTerm: string
  durabilityPointSeq?: string
  inSyncNodeIds: string[]
  drainingNodeIds: string[]
  repairingNodeIds: string[]
  faultedNodeIds: string[]
  compatibility?: CoordinatorCompatibilityMetadata
  updatedAtMs: number
}

interface CasGroupStateResult {
  updated: boolean
  state: ReplicationGroupState | null
}

export class EtcdClusterCoordinator implements ClusterCoordinator {
  private readonly client: Etcd3
  private readonly namespace: Namespace
  private readonly onWatcherError: ((error: Error) => void) | undefined
  private readonly leases = new Map<string, LocalLeaseEntry>()
  private readonly watchers = new Set<Watcher>()

  constructor(options: EtcdClusterCoordinatorOptions) {
    assertEtcdOptions(options)
    this.client = new Etcd3(toEtcdOptions(options))
    this.namespace = this.client.namespace(normaliseKeyPrefix(options.keyPrefix))
    this.onWatcherError = options.onWatcherError
  }

  async tryAcquireControllerLease(input: AcquireControllerLeaseInput): Promise<AcquireControllerLeaseResult> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.holderId, 'holderId')
    assertPositiveTtl(input.ttlMs)

    const key = controllerLeaseKey(input.clusterId)
    const lease = this.namespace.lease(ttlMsToSeconds(input.ttlMs))
    const leaseId = await lease.grant()
    const grantedAtMs = Date.now()
    const value = serializeLease({
      id: leaseId,
      kind: 'controller',
      clusterId: input.clusterId,
      holderId: input.holderId,
      ttlMs: input.ttlMs,
      grantedAtMs,
      expiresAtMs: grantedAtMs + input.ttlMs,
      metadata: cloneMetadata(input.metadata),
    })

    const result = await this.namespace
      .if(key, 'Create', '==', 0)
      .then(this.namespace.put(key).value(value).lease(leaseId))
      .commit()

    if (!result.succeeded) {
      await revokeLeaseQuietly(lease)
      const current = await this.getLeaseFromKey(key)
      return { acquired: false, lease: current }
    }

    this.trackLease(lease, {
      leaseId,
      key,
      ttlMs: input.ttlMs,
      ttlSeconds: ttlMsToSeconds(input.ttlMs),
      kind: 'controller',
      clusterId: input.clusterId,
      holderId: input.holderId,
      metadata: cloneMetadata(input.metadata),
    })

    const parsed = parseLease(value)
    return { acquired: true, lease: parsed }
  }

  async renewLease(leaseId: string, ttlMs: number): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')
    assertPositiveTtl(ttlMs)
    const entry = this.leases.get(leaseId)
    if (!entry) {
      return false
    }

    try {
      await entry.lease.keepaliveOnce()
      const renewedAtMs = Date.now()
      const leaseValue: SerializedLease = {
        id: leaseId,
        kind: entry.kind,
        clusterId: entry.clusterId,
        holderId: entry.holderId,
        ttlMs,
        grantedAtMs: renewedAtMs,
        expiresAtMs: renewedAtMs + ttlMs,
        metadata: cloneMetadata(entry.metadata),
      }
      const value =
        entry.kind === 'node-session' && entry.nodeSession
          ? JSON.stringify({ ...entry.nodeSession, lease: leaseValue })
          : serializeLease(leaseValue)
      await this.namespace.put(entry.key).value(value).ignoreLease()
      entry.ttlMs = ttlMs
      return true
    } catch {
      this.leases.delete(leaseId)
      return false
    }
  }

  async releaseLease(leaseId: string): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')
    const entry = this.leases.get(leaseId)
    if (!entry) {
      return false
    }

    this.leases.delete(leaseId)
    const currentValue = await this.namespace.get(entry.key).string()
    const currentLeaseId = currentValue ? parseLeaseIdForEntry(entry.kind, currentValue) : null
    if (currentLeaseId === leaseId) {
      await this.namespace.delete().key(entry.key)
    }
    await revokeLeaseQuietly(entry.lease)
    return currentLeaseId === leaseId
  }

  async registerNodeSession(input: RegisterNodeSessionInput): Promise<CoordinatorNodeSession> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.nodeId, 'nodeId')
    assertPositiveTtl(input.ttlMs)

    const key = nodeSessionKey(input.clusterId, input.nodeId)
    const lease = this.namespace.lease(ttlMsToSeconds(input.ttlMs))
    const leaseId = await lease.grant()
    const grantedAtMs = Date.now()
    const session: SerializedNodeSession = {
      clusterId: input.clusterId,
      nodeId: input.nodeId,
      lease: {
        id: leaseId,
        kind: 'node-session',
        clusterId: input.clusterId,
        holderId: input.nodeId,
        ttlMs: input.ttlMs,
        grantedAtMs,
        expiresAtMs: grantedAtMs + input.ttlMs,
        metadata: cloneMetadata(input.metadata),
      },
      endpoint: input.endpoint,
      groupIds: [...(input.groupIds ?? [])],
      dataBearing: input.dataBearing ?? true,
      voting: input.voting ?? true,
      compatibility: cloneCompatibility(input.compatibility),
      metadata: cloneMetadata(input.metadata),
    }

    await this.namespace.put(key).value(JSON.stringify(session)).lease(leaseId)
    this.trackLease(lease, {
      leaseId,
      key,
      ttlMs: input.ttlMs,
      ttlSeconds: ttlMsToSeconds(input.ttlMs),
      kind: 'node-session',
      clusterId: input.clusterId,
      holderId: input.nodeId,
      metadata: cloneMetadata(input.metadata),
      nodeSession: {
        clusterId: session.clusterId,
        nodeId: session.nodeId,
        endpoint: session.endpoint,
        groupIds: [...session.groupIds],
        dataBearing: session.dataBearing,
        voting: session.voting,
        compatibility: cloneCompatibility(session.compatibility),
        metadata: cloneMetadata(session.metadata),
      },
    })

    return parseNodeSession(JSON.stringify(session))
  }

  async getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')
    const value = await this.namespace.get(nodeSessionKey(clusterId, nodeId)).string()
    return value ? parseNodeSession(value) : null
  }

  async deregisterNodeSession(clusterId: string, nodeId: string): Promise<void> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')
    const key = nodeSessionKey(clusterId, nodeId)
    for (const [leaseId, entry] of this.leases) {
      if (entry.key === key) {
        await this.releaseLease(leaseId)
      }
    }
  }

  async setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState> {
    const state = buildReplicationGroupState(input)
    await this.namespace.put(replicationGroupKey(input.clusterId, input.groupId)).value(serializeGroupState(state))
    return cloneReplicationGroupState(state)
  }

  async getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(groupId, 'groupId')
    const value = await this.namespace.get(replicationGroupKey(clusterId, groupId)).string()
    return value ? parseGroupState(value) : null
  }

  async watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): Promise<CoordinatorWatchDisposer> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(groupId, 'groupId')
    const key = replicationGroupKey(clusterId, groupId)
    const etcdWatcher = await this.namespace.watch().key(key).create()
    this.watchers.add(etcdWatcher)
    etcdWatcher.on('put', kv => {
      try {
        watcher(parseGroupState(kv.value.toString('utf8')))
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.onWatcherError?.(wrappedErr)
      }
    })
    etcdWatcher.on('error', err => {
      this.onWatcherError?.(err instanceof Error ? err : new Error(String(err)))
    })
    return async () => {
      this.watchers.delete(etcdWatcher)
      await etcdWatcher.cancel()
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
    const currentRaw = await this.namespace.get(key).string()
    if (!currentRaw) {
      return { advanced: false, state: null }
    }
    const current = parseGroupState(currentRaw)
    if (current.primaryTerm !== input.expectedPrimaryTerm) {
      return { advanced: false, state: current }
    }
    assertPrimaryInGroup(input.nextPrimary, current.votingDataBearingNodeIds)

    const next: ReplicationGroupState = {
      ...current,
      currentPrimary: { ...input.nextPrimary },
      primaryTerm: current.primaryTerm + 1n,
      updatedAtMs: Date.now(),
    }
    markDisplacedPrimaryForRepair(next, current.currentPrimary?.nodeId, input.nextPrimary.nodeId)
    const nextRaw = serializeGroupState(next)
    const result = await this.namespace
      .if(key, 'Value', '==', currentRaw)
      .then(this.namespace.put(key).value(nextRaw))
      .commit()
    if (result.succeeded) {
      return { advanced: true, state: cloneReplicationGroupState(next) }
    }

    return { advanced: false, state: await this.getReplicationGroupState(input.clusterId, input.groupId) }
  }

  async updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    if (input.durabilityPointSeq !== undefined) {
      assertNonNegativeSeq(input.durabilityPointSeq, 'durabilityPointSeq')
    }
    const inSyncNodeIds = normaliseNodeIds(input.inSyncNodeIds, 'inSyncNodeIds')

    return this.updateGroupStateWithRetry(input.clusterId, input.groupId, state => {
      assertSubset(inSyncNodeIds, state.votingDataBearingNodeIds, 'inSyncNodeIds')
      assertNoInSyncAdditions(state.inSyncNodeIds, inSyncNodeIds)
      const durabilityPointSeq =
        input.durabilityPointSeq !== undefined && input.durabilityPointSeq > state.durabilityPointSeq
          ? input.durabilityPointSeq
          : state.durabilityPointSeq
      if (arraysEqual(inSyncNodeIds, state.inSyncNodeIds) && durabilityPointSeq === state.durabilityPointSeq) {
        return state
      }
      return { ...state, durabilityPointSeq, inSyncNodeIds, updatedAtMs: Date.now() }
    })
  }

  async admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')
    assertNonEmpty(input.sourceNodeId, 'sourceNodeId')
    assertNonNegativeSeq(input.appliedSeq, 'appliedSeq')

    return this.updateGroupStateWithRetry(input.clusterId, input.groupId, state => {
      if (!state.votingDataBearingNodeIds.includes(input.nodeId)) {
        throw new RangeError(`Node '${input.nodeId}' is not configured for the replication group`)
      }
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
      return { ...state, inSyncNodeIds, repairingNodeIds, updatedAtMs: Date.now() }
    })
  }

  async updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    assertNonEmpty(input.nodeId, 'nodeId')

    return this.updateGroupStateWithRetry(input.clusterId, input.groupId, state => {
      if (!state.votingDataBearingNodeIds.includes(input.nodeId)) {
        throw new RangeError(`Node '${input.nodeId}' is not configured for the replication group`)
      }
      return {
        ...state,
        drainingNodeIds: setMembership(state.drainingNodeIds, input.nodeId, input.draining),
        repairingNodeIds: setMembership(state.repairingNodeIds, input.nodeId, input.repairing),
        faultedNodeIds: setMembership(state.faultedNodeIds, input.nodeId, input.faulted),
        inSyncNodeIds:
          input.draining === true || input.repairing === true || input.faulted === true
            ? removeNodeId(state.inSyncNodeIds, input.nodeId)
            : state.inSyncNodeIds,
        updatedAtMs: Date.now(),
      }
    })
  }

  async promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.groupId, 'groupId')
    const excludedNodeIds = new Set(input.excludeNodeIds ?? [])

    for (let attempt = 0; attempt < 3; attempt++) {
      const state = await this.getReplicationGroupState(input.clusterId, input.groupId)
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
        const session = await this.getLiveNodeSession(input.clusterId, nodeId)
        if (!isEligiblePromotionSession(state, nodeId, session)) {
          continue
        }
        const advanced = await this.compareAndAdvancePrimaryTerm({
          clusterId: input.clusterId,
          groupId: input.groupId,
          expectedPrimaryTerm: state.primaryTerm,
          nextPrimary: session.endpoint ? { nodeId, endpoint: session.endpoint } : { nodeId },
        })
        if (advanced.advanced && advanced.state) {
          return advanced.state
        }
      }
    }

    throw new NoSafePrimaryError(`No safe primary is available for replication group '${input.groupId}'`)
  }

  async close(): Promise<void> {
    const watcherCancels: Promise<void>[] = []
    for (const watcher of this.watchers) {
      watcherCancels.push(watcher.cancel())
    }
    this.watchers.clear()
    await Promise.allSettled(watcherCancels)

    const leaseRevokes: Promise<void>[] = []
    for (const entry of this.leases.values()) {
      leaseRevokes.push(revokeLeaseQuietly(entry.lease))
    }
    this.leases.clear()
    await Promise.allSettled(leaseRevokes)
    this.client.close()
  }

  private async getLeaseFromKey(key: string): Promise<CoordinatorLease | null> {
    const value = await this.namespace.get(key).string()
    return value ? parseLease(value) : null
  }

  private trackLease(lease: Lease, entry: Omit<LocalLeaseEntry, 'lease'>): void {
    const fullEntry: LocalLeaseEntry = { ...entry, lease }
    this.leases.set(entry.leaseId, fullEntry)
    lease.on('lost', err => {
      this.leases.delete(entry.leaseId)
      this.onWatcherError?.(err instanceof Error ? err : new CoordinatorError(String(err)))
    })
  }

  private async updateGroupStateWithRetry(
    clusterId: string,
    groupId: string,
    mutate: (state: ReplicationGroupState) => ReplicationGroupState,
  ): Promise<ReplicationGroupState | null> {
    for (let attempt = 0; attempt < 5; attempt++) {
      const state = await this.getReplicationGroupState(clusterId, groupId)
      if (!state) {
        return null
      }
      const next = mutate(state)
      if (next === state) {
        return cloneReplicationGroupState(state)
      }
      const result = await this.casGroupState(state, next)
      if (result.updated) {
        return result.state
      }
    }
    throw new CoordinatorError(`Failed to update replication group '${groupId}' after concurrent coordinator writes`)
  }

  private async casGroupState(
    previous: ReplicationGroupState,
    next: ReplicationGroupState,
  ): Promise<CasGroupStateResult> {
    const key = replicationGroupKey(previous.clusterId, previous.groupId)
    const previousRaw = serializeGroupState(previous)
    const nextRaw = serializeGroupState(next)
    const result = await this.namespace
      .if(key, 'Value', '==', previousRaw)
      .then(this.namespace.put(key).value(nextRaw))
      .commit()
    if (result.succeeded) {
      return { updated: true, state: cloneReplicationGroupState(next) }
    }
    return { updated: false, state: await this.getReplicationGroupState(previous.clusterId, previous.groupId) }
  }
}

export function createEtcdCoordinator(options: EtcdClusterCoordinatorOptions): EtcdClusterCoordinator {
  return new EtcdClusterCoordinator(options)
}

function assertEtcdOptions(options: EtcdClusterCoordinatorOptions): void {
  const hosts = Array.isArray(options.hosts) ? options.hosts : [options.hosts]
  if (hosts.length === 0) {
    throw new TypeError('hosts must contain at least one etcd endpoint')
  }
  for (const host of hosts) {
    assertNonEmpty(host, 'hosts entry')
    if (!options.allowInsecure && !host.startsWith('https://')) {
      throw new TypeError('production coordinator access requires https etcd endpoints')
    }
  }
  assertNonEmpty(options.keyPrefix, 'keyPrefix')
  if (!options.allowInsecure && !options.credentials) {
    throw new TypeError('production coordinator access requires TLS credentials')
  }
  const hasMtlsIdentity = Boolean(options.credentials?.privateKey && options.credentials.certChain)
  const hasPasswordAuth = Boolean(options.auth?.username && options.auth.password)
  if (!options.allowInsecure && !hasMtlsIdentity && !hasPasswordAuth) {
    throw new TypeError('production coordinator access requires an authenticated Sirannon identity')
  }
}

function toEtcdOptions(options: EtcdClusterCoordinatorOptions): IOptions {
  const defaultCallTimeoutMs = options.defaultCallTimeoutMs
  const defaultCallOptions = defaultCallTimeoutMs ? () => ({ deadline: Date.now() + defaultCallTimeoutMs }) : undefined
  return {
    hosts: options.hosts,
    credentials: options.credentials,
    auth: options.auth,
    grpcOptions: options.grpcOptions,
    dialTimeout: options.dialTimeoutMs,
    defaultCallOptions,
  }
}

function normaliseKeyPrefix(prefix: string): string {
  const trimmed = prefix.replace(/^\/+/, '').replace(/\/+$/, '')
  if (trimmed.length === 0) {
    throw new TypeError('keyPrefix must not resolve to the etcd root')
  }
  return `${trimmed}/`
}

function controllerLeaseKey(clusterId: string): string {
  return `clusters/${encodeKey(clusterId)}/controller`
}

function nodeSessionKey(clusterId: string, nodeId: string): string {
  return `clusters/${encodeKey(clusterId)}/nodes/${encodeKey(nodeId)}`
}

function replicationGroupKey(clusterId: string, groupId: string): string {
  return `clusters/${encodeKey(clusterId)}/groups/${encodeKey(groupId)}`
}

function encodeKey(value: string): string {
  return encodeURIComponent(value)
}

function ttlMsToSeconds(ttlMs: number): number {
  return Math.max(1, Math.ceil(ttlMs / 1000))
}

function buildReplicationGroupState(input: SetReplicationGroupStateInput): ReplicationGroupState {
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
    updatedAtMs: Date.now(),
  }
}

function serializeLease(lease: SerializedLease): string {
  return JSON.stringify(lease)
}

function parseLease(raw: string): CoordinatorLease {
  const value = JSON.parse(raw) as SerializedLease
  return {
    ...value,
    metadata: cloneMetadata(value.metadata),
  }
}

function parseNodeSession(raw: string): CoordinatorNodeSession {
  const value = JSON.parse(raw) as SerializedNodeSession
  return {
    ...value,
    lease: parseLease(JSON.stringify(value.lease)),
    groupIds: [...value.groupIds],
    compatibility: cloneCompatibility(value.compatibility),
    metadata: cloneMetadata(value.metadata),
  }
}

function parseLeaseIdForEntry(kind: LocalLeaseEntry['kind'], raw: string): string | null {
  try {
    if (kind === 'node-session') {
      return parseNodeSession(raw).lease.id
    }
    return parseLease(raw).id
  } catch {
    return null
  }
}

function serializeGroupState(state: ReplicationGroupState): string {
  const serialized: SerializedReplicationGroupState = {
    ...state,
    currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
    votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
    primaryTerm: state.primaryTerm.toString(),
    durabilityPointSeq: state.durabilityPointSeq.toString(),
    inSyncNodeIds: [...state.inSyncNodeIds],
    drainingNodeIds: [...state.drainingNodeIds],
    repairingNodeIds: [...state.repairingNodeIds],
    faultedNodeIds: [...state.faultedNodeIds],
    compatibility: cloneCompatibility(state.compatibility),
  }
  return JSON.stringify(serialized)
}

function parseGroupState(raw: string): ReplicationGroupState {
  const value = JSON.parse(raw) as SerializedReplicationGroupState
  return {
    ...value,
    currentPrimary: value.currentPrimary ? { ...value.currentPrimary } : null,
    votingDataBearingNodeIds: [...value.votingDataBearingNodeIds],
    primaryTerm: BigInt(value.primaryTerm),
    durabilityPointSeq: value.durabilityPointSeq !== undefined ? BigInt(value.durabilityPointSeq) : 0n,
    inSyncNodeIds: [...value.inSyncNodeIds],
    drainingNodeIds: [...value.drainingNodeIds],
    repairingNodeIds: [...value.repairingNodeIds],
    faultedNodeIds: [...(value.faultedNodeIds ?? [])],
    compatibility: cloneCompatibility(value.compatibility),
  }
}

function cloneReplicationGroupState(state: ReplicationGroupState): ReplicationGroupState {
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

function isEligiblePromotionSession(
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

function markDisplacedPrimaryForRepair(
  state: ReplicationGroupState,
  displacedPrimaryId: string | undefined,
  nextPrimaryId: string,
): void {
  if (!displacedPrimaryId || displacedPrimaryId === nextPrimaryId) return
  state.inSyncNodeIds = removeNodeId(state.inSyncNodeIds, displacedPrimaryId)
  state.repairingNodeIds = setMembership(state.repairingNodeIds, displacedPrimaryId, true)
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

function assertNonNegativeSeq(seq: bigint, name: string): void {
  if (seq < 0n) {
    throw new RangeError(`${name} must not be negative`)
  }
}

function assertNoInSyncAdditions(previous: string[], next: string[]): void {
  const previousSet = new Set(previous)
  for (const nodeId of next) {
    if (!previousSet.has(nodeId)) {
      throw new RangeError(`Node '${nodeId}' cannot be added to the in-sync set without catch-up proof`)
    }
  }
}

function arraysEqual(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false
  for (let i = 0; i < left.length; i++) {
    if (left[i] !== right[i]) return false
  }
  return true
}

function cloneCompatibility(
  compatibility: CoordinatorCompatibilityMetadata | undefined,
): CoordinatorCompatibilityMetadata | undefined {
  return compatibility ? { ...compatibility } : undefined
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

async function revokeLeaseQuietly(lease: Lease): Promise<void> {
  try {
    await lease.revoke()
  } catch {
    lease.release()
  }
}
