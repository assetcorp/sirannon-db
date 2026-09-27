/**
 * The node of a replication group that takes its backups.
 *
 * With `'replica'`, Sirannon picks an eligible replica, and it picks the
 * primary where the group has no eligible replica. With `'primary'`, Sirannon
 * picks the primary, and no node takes backups while the primary is not
 * eligible. An object names one node directly, and each node compares its own
 * identifier against that name without reading the membership.
 *
 * @public
 */
export type BackupNodePreference = 'replica' | 'primary' | { nodeId: string }

/**
 * The primary of a replication group, and the nodes that hold data current
 * enough to back up.
 *
 * @public
 */
export interface BackupGroupMembership {
  /** The identifier of the primary, or null while the group has no primary. */
  primaryNodeId: string | null
  /** The identifiers of the nodes that are eligible to take the backup. */
  nodeIds: string[]
}

/**
 * The source of the identity of this node and the membership of its
 * replication group. Without a group source, the cycle backs up the database on
 * every turn, as a single-node deployment needs.
 *
 * `coordinatorBackupGroup` in the replication entry point builds a group source
 * from a cluster coordinator. Write your own where another system holds the
 * membership.
 *
 * @public
 */
export interface BackupGroupSource {
  /** The identifier of this node within the group. */
  readonly nodeId: string
  /**
   * Reads the current membership of the group.
   *
   * @returns The primary of the group, and the nodes that are eligible.
   */
  readMembership(): Promise<BackupGroupMembership>
}

/**
 * The reason that one turn of the cycle writes nothing.
 *
 * `'not-preferred'` means that another node, or no node, takes the backups of
 * this group. `'group-unavailable'` means that Sirannon cannot read the
 * membership of the group. `'previous-run-active'` means that the previous turn
 * is still in progress.
 *
 * @public
 */
export type BackupSkipReason = 'not-preferred' | 'group-unavailable' | 'previous-run-active'

/**
 * One turn that the cycle skips, and the reason for the skip.
 *
 * @public
 */
export interface BackupSkip {
  /** The condition that applies. */
  reason: BackupSkipReason
  /** A sentence for the log of the operator that describes the skip. */
  message: string
  /** The identifier of this node, where a group source supplies one. */
  nodeId?: string
  /** The identifier of the node that takes the backups, where Sirannon picks one. */
  preferredNodeId?: string
  /**
   * The size in bytes of the write-ahead log on this node at the skip. An alert
   * on this value as it rises warns you that the log on a node is growing, well
   * before `maxUncapturedLogBytes` ends its chain.
   */
  uncapturedLogBytes?: number
}

/**
 * Returns the node of a replication group that takes its backups.
 *
 * Every node of the group computes this from the same membership, so that only
 * the node that the answer names takes the turn. Sirannon sorts the eligible
 * nodes first, so that every node reaches the same answer.
 *
 * @param membership - The primary of the group, and the nodes that are eligible.
 * @param preference - The node that the operator wants the backups taken on.
 * @returns The identifier of that node, or null where no node qualifies.
 *
 * @internal
 */
export function preferredBackupNode(
  membership: BackupGroupMembership,
  preference: BackupNodePreference,
): string | null {
  if (typeof preference !== 'string') {
    return preference.nodeId
  }

  const primaryNodeId = membership.primaryNodeId
  const eligible = [...membership.nodeIds].sort()
  const primary = primaryNodeId !== null && eligible.includes(primaryNodeId) ? primaryNodeId : null
  if (preference === 'primary') {
    return primary
  }

  const replica = eligible.find(nodeId => nodeId !== primaryNodeId)
  return replica ?? primary
}
