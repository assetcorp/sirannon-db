import type { Watcher } from 'etcd3'
import { CoordinatorError } from '../errors.js'
import {
  parseLeaseIdForEntry,
  parseNodeSession,
  type SerializedLease,
  type SerializedNodeSession,
  serializeLease,
} from './etcd-codec.js'
import {
  assertEtcdOptions,
  type EtcdClusterCoordinatorOptions,
  type EtcdConnection,
  nodeSessionKey,
  normaliseKeyPrefix,
  toEtcdOptions,
  ttlMsToSeconds,
} from './etcd-connection.js'
import { acquireEtcdControllerLease, watchEtcdControllerLease } from './etcd-controller-lease.js'
import { EtcdGroupStore } from './etcd-group-store.js'
import { EtcdLeaseRegistry, revokeLeaseQuietly } from './etcd-lease-registry.js'
import { loadEtcd3Module } from './etcd-loader.js'
import { watchEtcdNodeSessions } from './etcd-session-watch.js'
import { assertNonEmpty, assertPositiveTtl, cloneCompatibility, cloneMetadata } from './group-rules.js'
import type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  AdmitNodeToInSyncSetInput,
  ClusterCoordinator,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  ControllerLeaseWatcher,
  CoordinatorNodeSession,
  CoordinatorWatchDisposer,
  NodeSessionWatcher,
  PromoteEligibleReplicaInput,
  RegisterNodeSessionInput,
  ReplicationGroupState,
  ReplicationGroupWatcher,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './types.js'

export type { EtcdClusterCoordinatorOptions } from './etcd-connection.js'

/**
 * Stores primary authority, node sessions, group state, and the in-sync set in etcd.
 *
 * Build one with {@link createEtcdCoordinator}. The coordinator loads the `etcd3` package the first time that one of its methods connects to etcd. When this process cannot load that package, the call fails with a `SirannonError` whose code is `COORDINATOR_DEPENDENCY_MISSING`.
 *
 * @public
 */
export class EtcdClusterCoordinator implements ClusterCoordinator {
  private readonly options: EtcdClusterCoordinatorOptions
  private connection: Promise<EtcdConnection> | null = null
  private readonly onWatcherError: ((error: Error) => void) | undefined
  private readonly leases: EtcdLeaseRegistry
  private readonly grantedNodeSessionLeaseIds = new Map<string, string>()
  private readonly watchers = new Set<Watcher>()

  constructor(options: EtcdClusterCoordinatorOptions) {
    assertEtcdOptions(options)
    this.options = options
    this.onWatcherError = options.onWatcherError
    this.leases = new EtcdLeaseRegistry(this.onWatcherError)
  }

  /** Tries to acquire the controller lease, and returns whether it succeeded along with the current lease. */
  async tryAcquireControllerLease(input: AcquireControllerLeaseInput): Promise<AcquireControllerLeaseResult> {
    assertNonEmpty(input.clusterId, 'clusterId')
    assertNonEmpty(input.holderId, 'holderId')
    assertPositiveTtl(input.ttlMs)

    const { namespace } = await this.connect()
    return acquireEtcdControllerLease(namespace, this.leases, input)
  }

  /** Calls `watcher` with the current controller lease when the watch starts and after each change, and returns a function that stops the watch. */
  async watchControllerLease(clusterId: string, watcher: ControllerLeaseWatcher): Promise<CoordinatorWatchDisposer> {
    assertNonEmpty(clusterId, 'clusterId')
    const { namespace } = await this.connect()
    return watchEtcdControllerLease(namespace, clusterId, watcher, this.watchers, this.onWatcherError)
  }

  /** Extends a lease, and returns false for a lease that is expired or unknown to this coordinator. */
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
      this.leases.forget(leaseId)
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

    const { namespace } = await this.connect()
    let refreshed: boolean
    try {
      const result = await namespace
        .if(entry.key, 'Lease', '==', leaseId)
        .then(namespace.put(entry.key).value(value).ignoreLease())
        .commit()
      refreshed = result.succeeded === true
    } catch {
      return true
    }

    if (!refreshed) {
      this.leases.forget(leaseId)
      return false
    }

    entry.ttlMs = ttlMs
    return true
  }

  /** Deletes a lease's key if the key still refers to that lease, then revokes the lease, and returns whether the delete succeeded. */
  async releaseLease(leaseId: string): Promise<boolean> {
    assertNonEmpty(leaseId, 'leaseId')
    const entry = this.leases.get(leaseId)
    if (!entry) {
      return false
    }

    this.leases.forget(leaseId)
    const { namespace } = await this.connect()
    const currentValue = await namespace.get(entry.key).string()
    const currentLeaseId = currentValue ? parseLeaseIdForEntry(entry.kind, currentValue) : null
    let released = false
    if (currentLeaseId === leaseId && currentValue) {
      const result = await namespace
        .if(entry.key, 'Value', '==', currentValue)
        .then(namespace.delete().key(entry.key))
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

    const { namespace } = await this.connect()
    const key = nodeSessionKey(input.clusterId, input.nodeId)
    const existingRawSession = await namespace.get(key).string()
    if (existingRawSession) {
      const existingSession = parseNodeSession(existingRawSession)
      const supersedesOwnSession = this.grantedNodeSessionLeaseIds.get(key) === existingSession.lease.id
      if (existingSession.lease.expiresAtMs > Date.now() && !supersedesOwnSession) {
        throw new CoordinatorError(`Node session '${input.nodeId}' is already registered`)
      }
    }

    const lease = namespace.lease(ttlMsToSeconds(input.ttlMs))
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
      ? namespace.if(key, 'Value', '==', existingRawSession)
      : namespace.if(key, 'Create', '==', 0)
    const result = await transaction.then(namespace.put(key).value(rawSession).lease(leaseId)).commit()
    if (!result.succeeded) {
      await revokeLeaseQuietly(lease)
      throw new CoordinatorError(`Node session '${input.nodeId}' registration conflicted with a concurrent write`)
    }

    await this.leases.discardSuperseded(key, leaseId)
    this.grantedNodeSessionLeaseIds.set(key, leaseId)
    this.leases.track(lease, {
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

  /** Returns one node's session, or null once its lease expires. */
  async getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')
    const { namespace } = await this.connect()
    const value = await namespace.get(nodeSessionKey(clusterId, nodeId)).string()
    return value ? parseNodeSession(value) : null
  }

  /** Calls `watcher` with the IDs of every node that holds a live session, and returns a function that stops the watch. */
  async watchNodeSessions(clusterId: string, watcher: NodeSessionWatcher): Promise<CoordinatorWatchDisposer> {
    assertNonEmpty(clusterId, 'clusterId')
    const { namespace } = await this.connect()
    return watchEtcdNodeSessions(namespace, clusterId, watcher, this.watchers, this.onWatcherError)
  }

  /** Ends one node's session at once by releasing its leases. */
  async deregisterNodeSession(clusterId: string, nodeId: string): Promise<void> {
    assertNonEmpty(clusterId, 'clusterId')
    assertNonEmpty(nodeId, 'nodeId')
    for (const leaseId of this.leases.leaseIdsForKey(nodeSessionKey(clusterId, nodeId))) {
      await this.releaseLease(leaseId)
    }
  }

  /** Writes the group's state, which creates a new group or replaces an existing one. */
  async setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState> {
    const { groups } = await this.connect()
    return groups.setReplicationGroupState(input)
  }

  /** Returns the group's state, or null when etcd has no state for the group. */
  async getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null> {
    const { groups } = await this.connect()
    return groups.getReplicationGroupState(clusterId, groupId)
  }

  /** Calls `watcher` with the new state after each change to the group, and returns a function that stops the watch. */
  async watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): Promise<CoordinatorWatchDisposer> {
    const { groups } = await this.connect()
    return groups.watchReplicationGroup(clusterId, groupId, watcher)
  }

  /** Makes a node primary and advances the term, but only while the group is still at the term that the caller expects. */
  async compareAndAdvancePrimaryTerm(
    input: CompareAndAdvancePrimaryTermInput,
  ): Promise<CompareAndAdvancePrimaryTermResult> {
    const { groups } = await this.connect()
    return groups.compareAndAdvancePrimaryTerm(input)
  }

  /** Replaces the group's in-sync set with a subset of it, and can advance the durability point. */
  async updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null> {
    const { groups } = await this.connect()
    return groups.updateInSyncSet(input)
  }

  /** Adds one caught-up node to the in-sync set. */
  async admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null> {
    const { groups } = await this.connect()
    return groups.admitNodeToInSyncSet(input)
  }

  /** Sets or clears one node's draining, repairing, and faulted flags. */
  async updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null> {
    const { groups } = await this.connect()
    return groups.updateNodeMaintenance(input)
  }

  /** Promotes an eligible in-sync replica, and throws a `NoSafePrimaryError` when no replica qualifies. */
  async promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState> {
    const { groups } = await this.connect()
    return groups.promoteEligibleReplica(input)
  }

  /** Stops every watch, revokes every lease that this coordinator granted, and closes the etcd client. */
  async close(): Promise<void> {
    const watcherCancels: Promise<void>[] = []
    for (const watcher of this.watchers) {
      watcherCancels.push(watcher.cancel())
    }
    this.watchers.clear()
    await Promise.allSettled(watcherCancels)

    this.grantedNodeSessionLeaseIds.clear()
    await this.leases.revokeAll()
    const connection = this.connection
    if (!connection) return
    const opened = await connection.catch(() => null)
    opened?.client.close()
  }

  private connect(): Promise<EtcdConnection> {
    if (!this.connection) {
      this.connection = loadEtcd3Module().then(
        ({ Etcd3 }) => {
          const client = new Etcd3(toEtcdOptions(this.options))
          const namespace = client.namespace(normaliseKeyPrefix(this.options.keyPrefix))
          const groups = new EtcdGroupStore(namespace, this.watchers, this.onWatcherError)
          return { client, namespace, groups }
        },
        (err: unknown) => {
          this.connection = null
          throw err
        },
      )
    }
    return this.connection
  }
}

/**
 * Returns a new coordinator that stores its state in etcd.
 *
 * @param options - The etcd endpoints, key prefix, credentials, and timeouts.
 * @returns The coordinator, ready to pass to a replication engine.
 *
 * @public
 */
export function createEtcdCoordinator(options: EtcdClusterCoordinatorOptions): EtcdClusterCoordinator {
  return new EtcdClusterCoordinator(options)
}
