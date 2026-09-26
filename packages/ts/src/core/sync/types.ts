/** Describes a hybrid logical clock reading, decoded into its three parts.
 * @public
 */
export interface HLCTimestamp {
  /** The wall-clock milliseconds since the Unix epoch. */
  wallMs: number
  /** The counter that orders events within one wall-clock millisecond. */
  logical: number
  /** The ID of the node that took the reading, which orders two readings with the same time and counter. */
  nodeId: string
}

/** Describes one replicated row change or one replicated schema statement.
 * @public
 */
export interface ReplicationChange {
  /** The table that the change modifies. */
  table: string
  /** The row operation, or `'ddl'` for a schema statement. */
  operation: 'insert' | 'update' | 'delete' | 'ddl'
  /** The primary key of the changed row, encoded as a string. */
  rowId: string
  /** The primary key of the changed row, keyed by column. */
  primaryKey: Record<string, unknown>
  /** The hybrid logical clock stamp that the authoring node gave this change. */
  hlc: string
  /** The ID of the transaction that made this change. */
  txId: string
  /** The ID of the node that wrote the change. */
  nodeId: string
  /** The row after the change, or null for a delete. */
  newData: Record<string, unknown> | null
  /** The row before the change, or null for an insert. */
  oldData: Record<string, unknown> | null
  /** The schema statement to replay, set only when the operation is `'ddl'`. */
  ddlStatement?: string
}

/** Describes a checksummed run of changes that one node sends to another.
 * @public
 */
export interface ReplicationBatch {
  /** The ID of the node that sent the batch. */
  sourceNodeId: string
  /** The batch ID, which the receiving node acknowledges. */
  batchId: string
  /** The change-log sequence number of the first change in the batch. */
  fromSeq: bigint
  /** The change-log sequence number of the last change in the batch. */
  toSeq: bigint
  /** The lowest and highest hybrid logical clock stamps among the batch's changes. */
  hlcRange: { min: string; max: string }
  /** The changes, in ascending sequence order. */
  changes: ReplicationChange[]
  /** The checksum of the changes, which the receiving node verifies before it applies them. */
  checksum: string
  /** The ID of the sending node's replication group. */
  groupId?: string
  /** The sending node's primary term at the time it built the batch. */
  primaryTerm?: bigint
}

/** The local and incoming versions of one row, which Sirannon passes to a conflict resolver.
 * @public
 */
export interface ConflictContext {
  /** The table that holds the row. */
  table: string
  /** The row's primary key, encoded as a string. */
  rowId: string
  /** The local version of the row as a change, or null when the receiving node has none. */
  localChange: ReplicationChange | null
  /** The change from the sending node. */
  remoteChange: ReplicationChange
  /** The hybrid logical clock stamp of the local version, or null when the local row has no stamp. */
  localHlc: string | null
  /** The hybrid logical clock stamp of the incoming version. */
  remoteHlc: string
}

/** Describes a conflict resolver's decision for one row.
 * @public
 */
export interface ConflictResolution {
  /** Whether to take the incoming row, keep the local one, or write the merged row. */
  action: 'accept_remote' | 'keep_local' | 'merge'
  /** The row to write, which a `'merge'` action requires. */
  mergedData?: Record<string, unknown>
}

/** Chooses the version of a row to keep when an incoming change targets a row that already exists locally.
 * @public
 */
export interface ConflictResolver {
  /** Chooses between the local and the incoming version of one row. */
  resolve(ctx: ConflictContext): ConflictResolution | Promise<ConflictResolution>
}

/** Describes the outcome of applying one batch of changes.
 * @public
 */
export interface ApplyResult {
  /** The number of changes that the receiving node applied. */
  applied: number
  /** The number of changes that the receiving node skipped, such as the changes of an already applied batch, an update to a missing row, or a change that lost to the local row. */
  skipped: number
  /** The number of changes that a conflict resolver settled. */
  conflicts: number
  /** The tables that the batch's `DROP TABLE` statements removed, so that the caller can prune them from the change tracker. */
  droppedTables: string[]
}

/** Describes what a source node reports about one table at the end of the first sync, so that the joining node can verify the table.
 * @public
 */
export interface SyncTableManifest {
  /** The table name. */
  table: string
  /** The number of rows in that table on the source node. */
  rowCount: number
  /** A hash of every primary key in the table, in order. */
  pkHash?: string
  /** The chained digest of the batches that the source node streamed for this table. */
  batchDigest?: string
}
