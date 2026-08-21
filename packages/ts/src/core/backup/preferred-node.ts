/**
 * Which node of a replication group takes its backups.
 *
 * `'replica'` prefers a replica and falls back to the primary where the group
 * has no other node to offer. `'primary'` puts the backups on the node the
 * group names primary, and leaves them to no node at all while that node is out
 * of service. An object names one node outright. A node matching itself against that name
 * needs no coordinator to answer.
 *
 * @public
 */
export type BackupNodePreference = 'replica' | 'primary' | { nodeId: string }

/**
 * Who a replication group names primary, and which of its nodes hold data
 * current enough to back up.
 *
 * @public
 */
export interface BackupGroupMembership {
  /** Identifier of the primary, or null while the group names none. */
  primaryNodeId: string | null
  /** Identifiers of the nodes eligible to take the backup. */
  nodeIds: string[]
}

/**
 * Where the cycle reads this node's identity and its replication group's
 * membership. A database opened without one backs itself up every turn, which
 * is the answer a single-node deployment wants.
 *
 * `coordinatorBackupGroup` in the replication entry builds one of these over a
 * cluster coordinator. Write your own where the membership you trust lives
 * somewhere else.
 *
 * @public
 */
export interface BackupGroupSource {
  /** Identifier this node is known by inside the group. */
  readonly nodeId: string
  /**
   * Reads the group's membership as it stands right now.
   *
   * @returns Who the group names primary, and which nodes are eligible.
   */
  readMembership(): Promise<BackupGroupMembership>
}

/**
 * Why one turn of the cycle wrote nothing.
 *
 * `'not-preferred'` means another node takes this group's backups.
 * `'group-unavailable'` means this node could not read the membership it
 * decides from. `'previous-run-active'` means the turn before this one had yet
 * to finish.
 *
 * @public
 */
export type BackupSkipReason = 'not-preferred' | 'group-unavailable' | 'previous-run-active'

/**
 * One turn the cycle skipped, and what it skipped for.
 *
 * @public
 */
export interface BackupSkip {
  /** Which of the three conditions held. */
  reason: BackupSkipReason
  /** What happened, in a sentence an operator can read in a log. */
  message: string
  /** Identifier of this node, where a group source named one. */
  nodeId?: string
  /** Identifier of the node whose turn it was, where the group named one. */
  preferredNodeId?: string
}

/**
 * Works out which node of a replication group takes its backups.
 *
 * Every node of the group computes this from the same membership, so one of
 * them finds its own identifier in the answer and the rest stand down.
 * Sirannon sorts the eligible nodes first, which is what makes that answer the
 * same wherever it runs.
 *
 * @param membership - Who the group names primary, and which nodes are eligible.
 * @param preference - Which node the operator wants the backups taken on.
 * @returns Identifier of that node, or null where the group offers none.
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
