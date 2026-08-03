import { Etcd3, type Lease, type Namespace, type Watcher } from 'etcd3'
import { CoordinatorError } from '../errors.js'
import {
  parseLease,
  parseLeaseIdForEntry,
  parseNodeSession,
  type SerializedLease,
  type SerializedNodeSession,
  serializeLease,
} from './etcd-codec.js'
import {
  assertEtcdOptions,
  controllerLeaseKey,
  type EtcdClusterCoordinatorOptions,
  nodeSessionKey,
  normaliseKeyPrefix,
  toEtcdOptions,
  ttlMsToSeconds,
} from './etcd-connection.js'
import { EtcdGroupStore } from './etcd-group-store.js'
import { assertNonEmpty, assertPositiveTtl, cloneCompatibility, cloneMetadata } from './group-rules.js'
import type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  AdmitNodeToInSyncSetInput,
  ClusterCoordinator,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  CoordinatorLease,
  CoordinatorNodeSession,
  CoordinatorWatchDisposer,
  PromoteEligibleReplicaInput,
  RegisterNodeSessionInput,
  ReplicationGroupState,
  ReplicationGroupWatcher,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './types.js'

export type { EtcdClusterCoordinatorOptions } from './etcd-connection.js'

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

/**
 * @public
 *
 * Stores primary authority, node sessions, group state, and the in-sync set in etcd.
 *
 * Build one with {@link createEtcdCoordinator}.
 */
export class EtcdClusterCoordinator implements ClusterCoordinator {
  private readonly client: Etcd3
  private readonly namespace: Namespace
  private readonly onWatcherError: ((error: Error) => void) | undefined
  private readonly leases = new Map<string, LocalLeaseEntry>()
  private readonly grantedNodeSessionLeaseIds = new Map<string, string>()
  private readonly watchers = new Set<Watcher>()
  private readonly groups: EtcdGroupStore

  constructor(options: EtcdClusterCoordinatorOptions) {
    assertEtcdOptions(options)
    this.client = new Etcd3(toEtcdOptions(options))
    this.namespace = this.client.namespace(normaliseKeyPrefix(options.keyPrefix))
    this.onWatcherError = options.onWatcherError
    this.groups = new EtcdGroupStore(this.namespace, this.watchers, this.onWatcherError)
  }

  /** Bids for the controller lease, and reports who holds it. */
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

  /** Extends a lease, and reports false once it has already lapsed. */
  async renewLease(leaseId: string, ttlMs: number): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')
    assertPositiveTtl(ttlMs)
    const entry = this.leases.get(leaseId)
    if (!entry) {
      return false
    }

    try {
      await entry.lease.keepaliveOnce()
    } catch {
      this.leases.delete(leaseId)
      return false
    }

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

    let refreshed: boolean
    try {
      const result = await this.namespace
        .if(entry.key, 'Lease', '==', leaseId)
        .then(this.namespace.put(entry.key).value(value).ignoreLease())
        .commit()
      refreshed = result.succeeded === true
    } catch {
      return true
    }

    if (!refreshed) {
      this.leases.delete(leaseId)
      return false
    }

    entry.ttlMs = ttlMs
    return true
  }

  /** Gives up a lease at once instead of waiting for it to lapse. */
  async releaseLease(leaseId: string): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')
    const entry = this.leases.get(leaseId)
    if (!entry) {
      return false
    }

    this.leases.delete(leaseId)
    const currentValue = await this.namespace.get(entry.key).string()
    const currentLeaseId = currentValue ? parseLeaseIdForEntry(entry.kind, currentValue) : null
    let released = false
    if (currentLeaseId === leaseId && currentValue) {
      const result = await this.namespace
        .if(entry.key, 'Value', '==', currentValue)
        .then(this.namespace.delete().key(entry.key))
        .commit()
      released = result.succeeded === true
    }
    await revokeLeaseQuietly(entry.lease)
    return released
  }

  /** Records one node as a live member of the cluster. */
  async registerNodeSession(input: RegisterNodeSessionInput): Promise<CoordinatorNodeSession> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.nodeId, 'nodeId')
    assertPositiveTtl(input.ttlMs)

    const key = nodeSessionKey(input.clusterId, input.nodeId)
    const existingRawSession = await this.namespace.get(key).string()
    if (existingRawSession) {
      const existingSession = parseNodeSession(existingRawSession)
      const supersedesOwnSession = this.grantedNodeSessionLeaseIds.get(key) === existingSession.lease.id
      if (existingSession.lease.expiresAtMs > Date.now() && !supersedesOwnSession) {
        throw new CoordinatorError(`Node session '${input.nodeId}' is already registered`)
      }
    }

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

    const rawSession = JSON.stringify(session)
    const transaction = existingRawSession
      ? this.namespace.if(key, 'Value', '==', existingRawSession)
      : this.namespace.if(key, 'Create', '==', 0)
    const result = await transaction.then(this.namespace.put(key).value(rawSession).lease(leaseId)).commit()
    if (!result.succeeded) {
      await revokeLeaseQuietly(lease)
      throw new CoordinatorError(`Node session '${input.nodeId}' registration conflicted with a concurrent write`)
    }

    await this.discardSupersededLeases(key, leaseId)
    this.grantedNodeSessionLeaseIds.set(key, leaseId)
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

    return parseNodeSession(rawSession)
  }

  /** Reads one node's session, and returns null once its lease has lapsed. */
  async getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')
    const value = await this.namespace.get(nodeSessionKey(clusterId, nodeId)).string()
    return value ? parseNodeSession(value) : null
  }

  /** Ends one node's membership at once. */
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

  /** Writes the group's state, which seeds a new group or replaces an existing one. */
  setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState> {
    return this.groups.setReplicationGroupState(input)
  }

  /** Reads the group's state, and returns null when the group is absent. */
  getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null> {
    return this.groups.getReplicationGroupState(clusterId, groupId)
  }

  /** Calls back on each change to the group's state, and returns a function that stops the watch. */
  watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): Promise<CoordinatorWatchDisposer> {
    return this.groups.watchReplicationGroup(clusterId, groupId, watcher)
  }

  /** Promotes a node only while the group is still at the term the caller read. */
  compareAndAdvancePrimaryTerm(input: CompareAndAdvancePrimaryTermInput): Promise<CompareAndAdvancePrimaryTermResult> {
    return this.groups.compareAndAdvancePrimaryTerm(input)
  }

  /** Replaces the group's in-sync set, and optionally moves its durability point. */
  updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null> {
    return this.groups.updateInSyncSet(input)
  }

  /** Adds one caught-up node to the in-sync set. */
  admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null> {
    return this.groups.admitNodeToInSyncSet(input)
  }

  /** Marks one node as being taken out of service, rebuilt, or quarantined. */
  updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null> {
    return this.groups.updateNodeMaintenance(input)
  }

  /** Promotes whichever in-sync replica is safe to write. */
  promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState> {
    return this.groups.promoteEligibleReplica(input)
  }

  /** Closes the etcd client and stops every watch. */
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
    this.grantedNodeSessionLeaseIds.clear()
    await Promise.allSettled(leaseRevokes)
    this.client.close()
  }

  private async discardSupersededLeases(key: string, keepLeaseId: string): Promise<void> {
    const superseded: LocalLeaseEntry[] = []
    for (const [leaseId, entry] of this.leases) {
      if (entry.key === key && leaseId !== keepLeaseId) {
        this.leases.delete(leaseId)
        superseded.push(entry)
      }
    }
    await Promise.allSettled(superseded.map(entry => revokeLeaseQuietly(entry.lease)))
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
}

/**
 * @public
 *
 * Builds a coordinator backed by etcd.
 *
 * @param options - etcd endpoints, key prefix, credentials, and timeouts.
 * @returns The coordinator, ready to pass to a replication engine.
 */
export function createEtcdCoordinator(options: EtcdClusterCoordinatorOptions): EtcdClusterCoordinator {
  return new EtcdClusterCoordinator(options)
}

async function revokeLeaseQuietly(lease: Lease): Promise<void> {
  try {
    await lease.revoke()
  } catch {
    lease.release()
  }
}
