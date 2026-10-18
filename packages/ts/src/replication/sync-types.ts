import type { SyncTableManifest } from '../core/sync/types.js'

/**
 * Names how far a joining node is through first sync.
 *
 * @public
 */
export type SyncPhase = 'pending' | 'syncing' | 'catching-up' | 'ready'

/**
 * Describes a joining node's progress through first sync.
 *
 * @public
 */
export interface SyncState {
  /** Names how far the node is through first sync. */
  phase: SyncPhase
  /** Identifies the peer that streams the copy, or is null when no sync is in progress. */
  sourcePeerId: string | null
  /** Holds the change-log position at which the source took the copy. */
  snapshotSeq: bigint | null
  /** Lists the tables that the node already holds in full. */
  completedTables: string[]
  /** Counts the tables that the copy covers. */
  totalTables: number
  /** Holds the time, in milliseconds since the Unix epoch, at which the sync started. */
  startedAt: number | null
  /** Holds the reason that the last sync request failed, or null when no failure is recorded. */
  error: string | null
}

/**
 * Names one transaction in a replication group's history by the timestamp and the transaction ID that every node
 * stores with its changes.
 *
 * @public
 */
export interface SyncHistoryPoint {
  /** Holds the transaction's encoded HLC timestamp. */
  hlc: string
  /** Holds the transaction's ID. */
  txId: string
}

/**
 * Asks a peer to stream a full copy of the database, or, when the request names the joiner's newest history
 * transaction, to resume the joiner's history from the last transaction that both nodes hold.
 *
 * @public
 */
export interface SyncRequest {
  /** Identifies this sync, and every message of the sync repeats it. */
  requestId: string
  /** Identifies the node that asks for the copy. */
  joinerNodeId: string
  /** Lists the tables that the joiner already holds in full, so that the source skips them when the sync resumes. */
  completedTables: string[]
  /** Is true when the joiner can verify the stream with chained batch digests. */
  supportsStreamVerification?: boolean
  /** Names the newest transaction in the joiner's history, which a coordinator-mode source looks for in its own history before it streams a full copy. */
  historyTail?: SyncHistoryPoint
  /** Identifies the joiner's replication group. */
  groupId?: string
  /** Holds the primary term that the joiner reports as current. */
  primaryTerm?: bigint
}

/**
 * Holds one page of first-sync table data.
 *
 * @public
 */
export interface SyncBatch {
  /** Identifies the sync that this page is part of. */
  requestId: string
  /** Names the table that the rows come from. */
  table: string
  /** Holds the position of this page in the table's stream, counting from zero. */
  batchIndex: number
  /** Holds the rows. */
  rows: Record<string, unknown>[]
  /** Holds the schema statements, which the source sends in a `__schema__` page before any table, so that the joiner can create the tables. */
  schema?: string[]
  /** Holds the checksum of the rows, which the joiner verifies before it writes them. */
  checksum: string
  /** Is true on the last page of a table. */
  isLastBatchForTable: boolean
  /** Counts the tables that the whole copy covers. */
  totalTables?: number
  /** Identifies the source's replication group. */
  groupId?: string
  /** Holds the source's primary term. */
  primaryTerm?: bigint
}

/**
 * Tells a joining node that first sync is complete, and gives it the manifests that it checks the copy against.
 *
 * @public
 */
export interface SyncComplete {
  /** Identifies the sync that finished. */
  requestId: string
  /** Holds the change-log position at which the source took the copy, which the joiner resumes replication from. */
  snapshotSeq: bigint
  /** Holds one manifest per table, so that the joiner can check the rows that it received. */
  manifests: SyncTableManifest[]
  /** Names the newest history transaction that the copy holds, or, when `resumed` is true, the last transaction that both nodes hold. */
  historyPoint?: SyncHistoryPoint
  /** Is true when the source streamed no copy, so that the joiner discards its history after `historyPoint` and resumes replication from `snapshotSeq`. */
  resumed?: boolean
  /** Identifies the source's replication group. */
  groupId?: string
  /** Holds the source's primary term. */
  primaryTerm?: bigint
}

/**
 * Tells the source whether a joining node stored one first-sync page, and gives the reason for a failure.
 *
 * @public
 */
export interface SyncAck {
  /** Identifies the sync that this acknowledgement is part of. */
  requestId: string
  /** Identifies the joining node. */
  joinerNodeId: string
  /** Names the table that the page came from. */
  table: string
  /** Holds the position of the page in that table's stream. */
  batchIndex: number
  /** Is true when the joiner stored the page. */
  success: boolean
  /** Holds the reason that the joiner could not store the page. */
  error?: string
  /** Identifies the joiner's replication group. */
  groupId?: string
  /** Holds the primary term that the joiner reports as current. */
  primaryTerm?: bigint
}
