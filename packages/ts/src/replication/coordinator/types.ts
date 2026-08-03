/**
 * What a lease grants: the controller role, or one node's membership session.
 *
 * @public
 */
export type CoordinatorLeaseKind = 'controller' | 'node-session'

/**
 * Stops a group watch when called.
 *
 * @public
 */
export type CoordinatorWatchDisposer = () => void | Promise<void>

/**
 * A time-limited claim one node holds, which lapses unless the node renews it.
 *
 * @public
 */
export interface CoordinatorLease {
  /** Identifier of the lease, which the holder renews and releases it by. */
  id: string
  /** What the lease grants. */
  kind: CoordinatorLeaseKind
  /** Cluster the lease belongs to. */
  clusterId: string
  /** Identifier of the node holding it. */
  holderId: string
  /** Milliseconds the lease lasts from each renewal. */
  ttlMs: number
  /** Milliseconds since the Unix epoch, taken when the lease was granted. */
  grantedAtMs: number
  /** Milliseconds since the Unix epoch, after which the lease lapses. */
  expiresAtMs: number
  /** Anything else the holder attached. */
  metadata?: Record<string, unknown>
}

/**
 * A node's bid for the controller lease.
 *
 * @public
 */
export interface AcquireControllerLeaseInput {
  /** Cluster the lease covers. */
  clusterId: string
  /** Identifier the node claims the lease under. */
  holderId: string
  /** Milliseconds the lease should last. */
  ttlMs: number
  /** Anything else to record against the lease. */
  metadata?: Record<string, unknown>
}

/**
 * Whether the bid won the controller lease, and who holds it either way.
 *
 * @public
 */
export type AcquireControllerLeaseResult =
  | { acquired: true; lease: CoordinatorLease }
  | { acquired: false; lease: CoordinatorLease | null }

/**
 * Versions a node publishes, so the group refuses a peer it cannot work with.
 *
 * @public
 */
export interface CoordinatorCompatibilityMetadata {
  /** Version of the package the node runs. */
  packageVersion?: string
  /** Version of the specification the node implements. */
  specVersion?: string
  /** Version of the replication protocol the node speaks. */
  protocolVersion?: string
}

/**
 * What a node tells the coordinator about itself when it joins.
 *
 * @public
 */
export interface RegisterNodeSessionInput {
  /** Cluster the node joins. */
  clusterId: string
  /** Identifier of the node. */
  nodeId: string
  /** Milliseconds the session lasts from each renewal. */
  ttlMs: number
  /** Address clients reach this node at. */
  endpoint?: string
  /** Replication groups the node serves. */
  groupIds?: string[]
  /** Whether the node holds a copy of the data. */
  dataBearing?: boolean
  /** Whether the node counts towards majority. */
  voting?: boolean
  /** Versions the node publishes. */
  compatibility?: CoordinatorCompatibilityMetadata
  /** Anything else to record against the session. */
  metadata?: Record<string, unknown>
}

/**
 * One node's live membership of a cluster, which lapses when its lease does.
 *
 * @public
 */
export interface CoordinatorNodeSession {
  /** Cluster the node belongs to. */
  clusterId: string
  /** Identifier of the node. */
  nodeId: string
  /** Lease keeping this session alive. */
  lease: CoordinatorLease
  /** Address clients reach this node at. */
  endpoint?: string
  /** Replication groups the node serves. */
  groupIds: string[]
  /** Whether the node holds a copy of the data. */
  dataBearing: boolean
  /** Whether the node counts towards majority. */
  voting: boolean
  /** Versions the node published. */
  compatibility?: CoordinatorCompatibilityMetadata
  /** Anything else recorded against the session. */
  metadata?: Record<string, unknown>
}

/**
 * The node a group currently names as its primary.
 *
 * @public
 */
export interface CoordinatorPrimary {
  /** Identifier of the primary. */
  nodeId: string
  /** Address clients reach it at. */
  endpoint?: string
}

/**
 * The group's shared record of who writes, who is in sync, and how far the
 * data is durable. Failover reads and advances it.
 *
 * @public
 */
export interface ReplicationGroupState {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Nodes that count towards majority. */
  votingDataBearingNodeIds: string[]
  /** The node currently named primary, or null when the group has none. */
  currentPrimary: CoordinatorPrimary | null
  /** Term the current primary holds, which rises with each promotion. */
  primaryTerm: bigint
  /** Change-log position a majority of voting nodes has durably stored. */
  durabilityPointSeq: bigint
  /** Nodes the group counts as in sync. */
  inSyncNodeIds: string[]
  /** Nodes being taken out of service. */
  drainingNodeIds: string[]
  /** Nodes being rebuilt. */
  repairingNodeIds: string[]
  /** Nodes the group has quarantined. */
  faultedNodeIds: string[]
  /** Versions the group requires of its members. */
  compatibility?: CoordinatorCompatibilityMetadata
  /** Milliseconds since the Unix epoch, taken at the last change to this state. */
  updatedAtMs: number
}

/**
 * The group state to write, which seeds a new group or replaces an existing one.
 *
 * @public
 */
export interface SetReplicationGroupStateInput {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Nodes that count towards majority. */
  votingDataBearingNodeIds: string[]
  /** The node to name primary, or null to leave the group without one. */
  currentPrimary?: CoordinatorPrimary | null
  /** Term to record for that primary. */
  primaryTerm?: bigint
  /** Change-log position a majority has durably stored. */
  durabilityPointSeq?: bigint
  /** Nodes to record as in sync. */
  inSyncNodeIds?: string[]
  /** Nodes to record as being taken out of service. */
  drainingNodeIds?: string[]
  /** Nodes to record as being rebuilt. */
  repairingNodeIds?: string[]
  /** Nodes to record as quarantined. */
  faultedNodeIds?: string[]
  /** Versions the group requires of its members. */
  compatibility?: CoordinatorCompatibilityMetadata
}

/**
 * Promotes a node only while the group is still at the term the caller read,
 * so two candidates cannot both promote themselves.
 *
 * @public
 */
export interface CompareAndAdvancePrimaryTermInput {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Term the caller last read. The promotion fails when the group has moved past it. */
  expectedPrimaryTerm: bigint
  /** The node to promote. */
  nextPrimary: CoordinatorPrimary
}

/**
 * Whether the promotion took effect, and the group state that resulted.
 *
 * @public
 */
export interface CompareAndAdvancePrimaryTermResult {
  /** True when the group moved to the next term under the named primary. */
  advanced: boolean
  /** The group state as it now stands, or null when the group is absent. */
  state: ReplicationGroupState | null
}

/**
 * Replaces the group's in-sync set, and optionally moves its durability point.
 *
 * @public
 */
export interface UpdateInSyncSetInput {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Nodes to record as in sync. */
  inSyncNodeIds: string[]
  /** Change-log position a majority has durably stored. */
  durabilityPointSeq?: bigint
}

/**
 * Adds one caught-up node to the in-sync set, naming the progress that earned it.
 *
 * @public
 */
export interface AdmitNodeToInSyncSetInput {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Node to admit. */
  nodeId: string
  /** Node whose changes it has applied. */
  sourceNodeId: string
  /** Change-log position it has applied up to. */
  appliedSeq: bigint
}

/**
 * Marks one node as being taken out of service, rebuilt, or quarantined.
 *
 * @public
 */
export interface UpdateNodeMaintenanceInput {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Node whose state changes. */
  nodeId: string
  /** Whether the node is being taken out of service. */
  draining?: boolean
  /** Whether the node is being rebuilt. */
  repairing?: boolean
  /** Whether the group quarantines the node. */
  faulted?: boolean
}

/**
 * Asks the coordinator to promote whichever in-sync replica is safe to write.
 *
 * @public
 */
export interface PromoteEligibleReplicaInput {
  /** Cluster the group belongs to. */
  clusterId: string
  /** Identifier of the group. */
  groupId: string
  /** Nodes to pass over, such as the primary that has just failed. */
  excludeNodeIds?: string[]
}

/**
 * Receives the group state each time it changes.
 *
 * @public
 */
export type ReplicationGroupWatcher = (state: ReplicationGroupState) => void

/**
 * Stores primary authority, node sessions, group state, and the in-sync set
 * outside the database nodes, so failover has a source of truth no single node
 * owns. The package includes an etcd adapter; build your own to store this
 * elsewhere.
 *
 * @public
 */
export interface ClusterCoordinator {
  /** Bids for the controller lease, and reports who holds it. */
  tryAcquireControllerLease(input: AcquireControllerLeaseInput): Promise<AcquireControllerLeaseResult>
  /** Extends a lease, and reports false once it has already lapsed. */
  renewLease(leaseId: string, ttlMs: number): Promise<boolean>
  /** Gives up a lease at once instead of waiting for it to lapse. */
  releaseLease(leaseId: string): Promise<boolean>
  /** Records one node as a live member of the cluster. */
  registerNodeSession(input: RegisterNodeSessionInput): Promise<CoordinatorNodeSession>
  /** Reads one node's session, and returns null once its lease has lapsed. */
  getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null>
  /** Ends one node's membership at once. */
  deregisterNodeSession(clusterId: string, nodeId: string): Promise<void>
  /** Writes the group's state, which seeds a new group or replaces an existing one. */
  setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState>
  /** Reads the group's state, and returns null when the group is absent. */
  getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null>
  /** Calls back on each change to the group's state, and returns a function that stops the watch. */
  watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): CoordinatorWatchDisposer | Promise<CoordinatorWatchDisposer>
  /** Promotes a node only while the group is still at the term the caller read. */
  compareAndAdvancePrimaryTerm(input: CompareAndAdvancePrimaryTermInput): Promise<CompareAndAdvancePrimaryTermResult>
  /** Replaces the group's in-sync set, and optionally moves its durability point. */
  updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null>
  /** Adds one caught-up node to the in-sync set. */
  admitNodeToInSyncSet(input: AdmitNodeToInSyncSetInput): Promise<ReplicationGroupState | null>
  /** Marks one node as being taken out of service, rebuilt, or quarantined. */
  updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null>
  /** Promotes whichever in-sync replica is safe to write. */
  promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState>
  /** Releases whatever the coordinator holds open. */
  close?(): Promise<void>
}
