import type {
  ClusterCoordinator,
  CoordinatorCompatibilityMetadata,
  CoordinatorPrimary,
  ReplicationGroupState,
} from './types.js'

/**
 * Describes the group state to write, which creates a new group or replaces an existing one.
 *
 * @public
 */
export interface SetReplicationGroupStateInput {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Lists the nodes that count towards the majority. */
  votingDataBearingNodeIds: string[]
  /** Identifies the node to make primary, or null to leave the group without a primary. */
  currentPrimary?: CoordinatorPrimary | null
  /** Sets the term to record for that primary, which defaults to 0. */
  primaryTerm?: bigint
  /** Sets the change-log position up to which a majority stores the data durably. */
  durabilityPointSeq?: bigint
  /** Lists the nodes to record as in sync. */
  inSyncNodeIds?: string[]
  /** Lists the nodes to record as draining. */
  drainingNodeIds?: string[]
  /** Lists the nodes to record as repairing. */
  repairingNodeIds?: string[]
  /** Lists the nodes to record as faulted. */
  faultedNodeIds?: string[]
  /** Sets the versions that the group requires of its members. */
  compatibility?: CoordinatorCompatibilityMetadata
}

/**
 * Describes a promotion that takes effect only while the group is still at the
 * term that the caller expects, so that two candidates cannot both promote
 * themselves. The promotion sets the durability point to 0.
 *
 * @public
 */
export interface CompareAndAdvancePrimaryTermInput {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Holds the term that the caller last read. The promotion fails when the group's term differs. */
  expectedPrimaryTerm: bigint
  /** Identifies the node to promote. */
  nextPrimary: CoordinatorPrimary
}

/**
 * Reports whether the promotion took effect, along with the group state afterwards.
 *
 * @public
 */
export interface CompareAndAdvancePrimaryTermResult {
  /** Is true when the group moves to the next term under the new primary. */
  advanced: boolean
  /** Holds the current group state, or null when the coordinator has no state for the group. */
  state: ReplicationGroupState | null
}

/**
 * Describes a new in-sync set for the group, and optionally a new durability point. To add a node, call
 * {@link ClusterCoordinator.admitNodeToInSyncSet}, because this update throws a `RangeError` for any node outside the
 * current set. A primary names its term and its own node ID in the update, so that the coordinator applies the update
 * only while that node is still the primary in that term.
 *
 * @public
 */
export interface UpdateInSyncSetInput {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Lists the nodes to record as in sync. */
  inSyncNodeIds: string[]
  /** Sets the change-log position up to which a majority stores the data durably, and the coordinator keeps the higher of this value and the current one. */
  durabilityPointSeq?: bigint
  /** Holds the primary term under which the caller acts. When it is set, the coordinator applies the update only while the group is still at this term, and it returns the group unchanged once the term has moved on. */
  expectedPrimaryTerm?: bigint
  /** Identifies the primary under which the caller acts. When it is set, the coordinator applies the update only while this node is still the group's primary, and it returns the group unchanged once another node holds primary duty. */
  expectedPrimaryNodeId?: string
}

/**
 * Describes one caught-up node to add to the in-sync set, with the progress that qualifies it. The coordinator admits
 * the node only when `sourceNodeId` is the current primary, `appliedSeq` reaches the group's durability point, and the
 * node is neither draining nor faulted. It admits a repairing node only when `rejoinSyncCompleted` is true, and it then
 * clears that node's repairing flag.
 *
 * @public
 */
export interface AdmitNodeToInSyncSetInput {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Identifies the node to admit. */
  nodeId: string
  /** Identifies the node whose changes the admitted node applied. */
  sourceNodeId: string
  /** Holds the change-log position up to which the node applied those changes. */
  appliedSeq: bigint
  /** Holds the primary term in whose change log `appliedSeq` counts. When it is set, the coordinator admits the node only while the group is still at this term. */
  expectedPrimaryTerm?: bigint
  /** Is true when the node has finished the full copy that rejoins it after a repair, which the coordinator requires before it admits a repairing node. */
  rejoinSyncCompleted?: boolean
}

/**
 * Sets or clears one node's draining, repairing, and faulted flags. A flag left
 * undefined keeps its current value, and when any flag turns on, the
 * coordinator removes the node from the in-sync set.
 *
 * @public
 */
export interface UpdateNodeMaintenanceInput {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Identifies the node to update. */
  nodeId: string
  /** Sets whether the node is draining, which takes it out of service. */
  draining?: boolean
  /** Sets whether the node is repairing, which means that it rebuilds its copy of the data. */
  repairing?: boolean
  /** Sets whether the node is faulted, which keeps it out of service. */
  faulted?: boolean
}

/**
 * Describes a request to promote an eligible in-sync replica, which is one with
 * a live, compatible session that is neither draining, repairing, nor faulted.
 *
 * @public
 */
export interface PromoteEligibleReplicaInput {
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the group. */
  groupId: string
  /** Lists the nodes to skip, such as the primary that has just failed. */
  excludeNodeIds?: string[]
}
