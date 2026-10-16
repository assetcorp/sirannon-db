import type {
  AdmitNodeToInSyncSetInput,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  PromoteEligibleReplicaInput,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './group-inputs.js'

export type {
  AdmitNodeToInSyncSetInput,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  PromoteEligibleReplicaInput,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
}

/**
 * Names what a lease grants, which is either the controller role or one node's membership session.
 *
 * @public
 */
export type CoordinatorLeaseKind = 'controller' | 'node-session'

/**
 * Stops a coordinator watch. Every watch method returns one of these functions.
 *
 * @public
 */
export type CoordinatorWatchDisposer = () => void | Promise<void>

/**
 * Describes a time-limited claim that one node holds, which expires unless the node renews it.
 *
 * @public
 */
export interface CoordinatorLease {
  /** Identifies the lease, and the holder passes this ID to renew or release it. */
  id: string
  /** Names what the lease grants. */
  kind: CoordinatorLeaseKind
  /** Identifies the cluster that the lease covers. */
  clusterId: string
  /** Identifies the node that holds the lease. */
  holderId: string
  /** Holds how many milliseconds the lease lasts after each renewal. */
  ttlMs: number
  /** Holds the time, in milliseconds since the Unix epoch, at which the coordinator granted or last renewed the lease. */
  grantedAtMs: number
  /** Holds the time, in milliseconds since the Unix epoch, after which the lease expires. */
  expiresAtMs: number
  /** Holds any other data that the holder attaches. */
  metadata?: Record<string, unknown>
}

/**
 * Describes a node's request for the controller lease.
 *
 * @public
 */
export interface AcquireControllerLeaseInput {
  /** Identifies the cluster that the lease covers. */
  clusterId: string
  /** Sets the ID that the node holds the lease under. */
  holderId: string
  /** Sets how many milliseconds the lease lasts. */
  ttlMs: number
  /** Holds any other data to record with the lease. */
  metadata?: Record<string, unknown>
}

/**
 * Reports whether the request acquired the controller lease, along with the current lease either way.
 *
 * @public
 */
export type AcquireControllerLeaseResult =
  | {
      /** Is true when this request acquired the lease. */
      acquired: true
      /** Holds the lease that this request acquired, with its holder and expiry. */
      lease: CoordinatorLease
    }
  | {
      /** Is false when another lease on the controller role is still live. */
      acquired: false
      /** Holds the current holder's lease, or null when no node holds the lease. */
      lease: CoordinatorLease | null
    }

/**
 * Holds the versions that a node publishes. A node can be promoted, or accept writes as primary, only when its major
 * versions match every version that the group requires.
 *
 * @public
 */
export interface CoordinatorCompatibilityMetadata {
  /** Holds the version of the package that the node has installed. */
  packageVersion?: string
  /** Holds the version of the specification that the node implements. */
  specVersion?: string
  /** Holds the version of the replication protocol that the node uses. */
  protocolVersion?: string
}

/**
 * Describes a node to the coordinator when the node registers its session.
 *
 * @public
 */
export interface RegisterNodeSessionInput {
  /** Identifies the cluster that the node joins. */
  clusterId: string
  /** Identifies the node. */
  nodeId: string
  /** Sets how many milliseconds the session lasts after each renewal. */
  ttlMs: number
  /** Sets the address that clients use to reach this node. */
  endpoint?: string
  /** Lists the replication groups that the node serves. */
  groupIds?: string[]
  /** Is true when the node stores a copy of the data. */
  dataBearing?: boolean
  /** Is true when the node counts towards the majority. */
  voting?: boolean
  /** Holds the versions that the node publishes. */
  compatibility?: CoordinatorCompatibilityMetadata
  /** Holds any other data to record with the session. */
  metadata?: Record<string, unknown>
}

/**
 * Describes one node's live membership of a cluster, which ends when its lease expires.
 *
 * @public
 */
export interface CoordinatorNodeSession {
  /** Identifies the cluster that the node is a member of. */
  clusterId: string
  /** Identifies the node. */
  nodeId: string
  /** Holds the lease whose expiry ends this session. */
  lease: CoordinatorLease
  /** Holds the address that clients use to reach this node. */
  endpoint?: string
  /** Lists the replication groups that the node serves. */
  groupIds: string[]
  /** Is true when the node stores a copy of the data. */
  dataBearing: boolean
  /** Is true when the node counts towards the majority. */
  voting: boolean
  /** Holds the versions that the node publishes. */
  compatibility?: CoordinatorCompatibilityMetadata
  /** Holds any other data that the node records with the session. */
  metadata?: Record<string, unknown>
}

/**
 * Identifies a group's primary node and its address.
 *
 * @public
 */
export interface CoordinatorPrimary {
  /** Identifies the primary. */
  nodeId: string
  /** Holds the address that clients use to reach the primary. */
  endpoint?: string
}

/**
 * Records which node writes for the group, which nodes are in sync, and how
 * far the data is durable. The controller reads and updates this record during
 * failover.
 *
 * @public
 */
export interface ReplicationGroupState {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Lists the nodes that count towards the majority. */
  votingDataBearingNodeIds: string[]
  /** Identifies the current primary, or is null when the group has none. */
  currentPrimary: CoordinatorPrimary | null
  /** Holds the current primary's term, which rises by one with each promotion. */
  primaryTerm: bigint
  /** Holds the position in the current primary's change log up to which a majority of voting nodes stores the data durably. Each promotion sets it to 0, because the new primary numbers its change log on its own, and the new primary then records its own position. */
  durabilityPointSeq: bigint
  /** Lists the nodes that are in sync with the primary. */
  inSyncNodeIds: string[]
  /** Lists the draining nodes, which are leaving service. */
  drainingNodeIds: string[]
  /** Lists the repairing nodes, which are rebuilding their copy of the data. */
  repairingNodeIds: string[]
  /** Lists the faulted nodes, which stay out of service until an `updateNodeMaintenance` call clears the flag. */
  faultedNodeIds: string[]
  /** Holds the versions that the group requires of its members. */
  compatibility?: CoordinatorCompatibilityMetadata
  /** Holds the time, in milliseconds since the Unix epoch, of the last change to this state. */
  updatedAtMs: number
}

/**
 * Receives the group state each time it changes.
 *
 * @public
 */
export type ReplicationGroupWatcher = (state: ReplicationGroupState) => void

/**
 * Receives the IDs of every node that holds a live session, when the watch starts and after each session change.
 *
 * @public
 */
export type NodeSessionWatcher = (liveNodeIds: readonly string[]) => void

/**
 * Receives the controller lease when the watch starts and after each change, or null while no node holds the lease.
 *
 * @public
 */
export type ControllerLeaseWatcher = (lease: CoordinatorLease | null) => void

/**
 * Stores primary authority, node sessions, group state, and the in-sync set
 * outside the database nodes, so that the record stays intact when any one
 * database node fails. Sirannon includes an etcd coordinator, and you can
 * implement this interface to store the record elsewhere.
 *
 * @public
 */
export interface ClusterCoordinator {
  /** Tries to acquire the controller lease, and returns whether it succeeded along with the current lease. */
  tryAcquireControllerLease(input: AcquireControllerLeaseInput): Promise<AcquireControllerLeaseResult>
  /** Calls `watcher` with the current controller lease when the watch starts and after each change, and returns a function that stops the watch. A coordinator without this watch can omit the method, and every node's controller loop then tries to acquire the lease on each tick. */
  watchControllerLease?(
    clusterId: string,
    watcher: ControllerLeaseWatcher,
  ): CoordinatorWatchDisposer | Promise<CoordinatorWatchDisposer>
  /** Extends a lease, and returns false for a lease that is expired or unknown to this coordinator. */
  renewLease(leaseId: string, ttlMs: number): Promise<boolean>
  /** Releases a lease at once, and returns whether the release succeeded. */
  releaseLease(leaseId: string): Promise<boolean>
  /** Records one node as a live member of the cluster, and replaces any session that an earlier process of the same node left behind. */
  registerNodeSession(input: RegisterNodeSessionInput): Promise<CoordinatorNodeSession>
  /** Returns one node's session, or null once its lease expires. */
  getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null>
  /** Ends one node's session at once. */
  deregisterNodeSession(clusterId: string, nodeId: string): Promise<void>
  /** Calls `watcher` with the IDs of every node that holds a live session, and returns a function that stops the watch. A coordinator without this watch can omit the method, and a node then lists read endpoints from the group state alone. */
  watchNodeSessions?(
    clusterId: string,
    watcher: NodeSessionWatcher,
  ): CoordinatorWatchDisposer | Promise<CoordinatorWatchDisposer>
  /** Writes the group's state, which creates a new group or replaces an existing one. */
  setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState>
  /** Returns the group's state, or null when the coordinator has no state for the group. */
  getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null>
  /** Calls `watcher` with the new state after each change to the group, and returns a function that stops the watch. */
  watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): CoordinatorWatchDisposer | Promise<CoordinatorWatchDisposer>
  /** Makes a node primary and advances the term, but only while the group is still at the term that the caller expects. */
  compareAndAdvancePrimaryTerm(input: CompareAndAdvancePrimaryTermInput): Promise<CompareAndAdvancePrimaryTermResult>
  /** Replaces the group's in-sync set with a subset of it, and can advance the durability point. When the input names a term or a primary that the group no longer holds, the method changes nothing and returns the group as it stands. */
  updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null>
  /** Adds one caught-up node to the in-sync set. When the input names a term that the group no longer holds, the method changes nothing and returns the group as it stands. */
  admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null>
  /** Sets or clears one node's draining, repairing, and faulted flags. */
  updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null>
  /** Promotes an eligible in-sync replica while the group has no primary or while its primary is one of `excludeNodeIds`. When another node already holds primary duty, the method returns the group unchanged. When no replica qualifies, it throws a `NoSafePrimaryError`. */
  promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState>
  /** Releases every connection, watch, and lease that the coordinator keeps open. */
  close?(): Promise<void>
}
