/** A hybrid logical clock reading, decoded into its three parts.
 * @public
 */
export interface HLCTimestamp {
  /** Wall-clock milliseconds since the Unix epoch. */
  wallMs: number
  /** Counter that orders events sharing one wall-clock millisecond. */
  logical: number
  /** Identifier of the node that took the reading, which breaks remaining ties. */
  nodeId: string
}

/** One replicated row change, or one replicated schema statement.
 * @public
 */
export interface ReplicationChange {
  /** Table the change belongs to. */
  table: string
  /** What the change does to the row, or 'ddl' for a schema statement. */
  operation: 'insert' | 'update' | 'delete' | 'ddl'
  /** Primary key of the changed row, encoded as a string. */
  rowId: string
  /** Primary key of the changed row, by column. */
  primaryKey: Record<string, unknown>
  /** Hybrid logical clock stamp the authoring node gave this change. */
  hlc: string
  /** Identifier of the transaction that produced this change. */
  txId: string
  /** Identifier of the node that authored the change. */
  nodeId: string
  /** The row after the change, or null for a delete. */
  newData: Record<string, unknown> | null
  /** The row before the change, or null for an insert. */
  oldData: Record<string, unknown> | null
  /** The schema statement to replay, present only when the operation is 'ddl'. */
  ddlStatement?: string
}

/** A checksummed run of changes one node sends to another.
 * @public
 */
export interface ReplicationBatch {
  /** Identifier of the node that sent the batch. */
  sourceNodeId: string
  /** Identifier of this batch, which the receiver acknowledges. */
  batchId: string
  /** Change-log position of the first change in the batch. */
  fromSeq: bigint
  /** Change-log position of the last change in the batch. */
  toSeq: bigint
  /** Lowest and highest hybrid logical clock stamps the batch carries. */
  hlcRange: { min: string; max: string }
  /** The changes themselves, in ascending order. */
  changes: ReplicationChange[]
  /** Checksum of the changes, which the receiver verifies before it applies them. */
  checksum: string
  /** Identifier of the replication group the sender belongs to. */
  groupId?: string
  /** Primary term the sender held when it built the batch. */
  primaryTerm?: bigint
}

/** The local and incoming versions of one row, handed to a conflict resolver.
 * @public
 */
export interface ConflictContext {
  /** Table the row belongs to. */
  table: string
  /** Primary key of the row, encoded as a string. */
  rowId: string
  /** The change already applied locally, or null when the receiver recorded none. */
  localChange: ReplicationChange | null
  /** The change that arrived from the sending node. */
  remoteChange: ReplicationChange
  /** Hybrid logical clock stamp of the local version, or null when there is none. */
  localHlc: string | null
  /** Hybrid logical clock stamp of the incoming version. */
  remoteHlc: string
}

/** What a conflict resolver decided to do with one row.
 * @public
 */
export interface ConflictResolution {
  /** Whether to take the incoming row, keep the local one, or write the merged row. */
  action: 'accept_remote' | 'keep_local' | 'merge'
  /** The row to write, required when the action is 'merge'. */
  mergedData?: Record<string, unknown>
}

/** Resolves which version of a row wins when a receiver already holds that row.
 * @public
 */
export interface ConflictResolver {
  /** Chooses between the local and the incoming version of one row. */
  resolve(ctx: ConflictContext): ConflictResolution | Promise<ConflictResolution>
}

/** What applying one batch of changes did.
 * @public
 */
export interface ApplyResult {
  /** Number of changes the receiver wrote. */
  applied: number
  /** Number of changes the receiver had already seen. */
  skipped: number
  /** Number of rows that went through a conflict resolver. */
  conflicts: number
  /** Tables the batch dropped, named so callers can stop watching them. */
  droppedTables: string[]
}

/** What a source node reports about one table at the end of first sync, so the joiner can verify it.
 * @public
 */
export interface SyncTableManifest {
  /** Name of the table. */
  table: string
  /** Number of rows the source held in that table. */
  rowCount: number
  /** Hash of every primary key in the table, in order. */
  pkHash?: string
  /** Chained digest of the batches the source streamed for this table. */
  batchDigest?: string
}
