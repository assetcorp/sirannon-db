import type { SyncTableManifest } from '../core/sync/types.js'

/**
 * How far a joining node has got through first sync.
 *
 * @public
 */
export type SyncPhase = 'pending' | 'syncing' | 'catching-up' | 'ready'

/**
 * Where a joining node stands in first sync.
 *
 * @public
 */
export interface SyncState {
  /** How far the node has got. */
  phase: SyncPhase
  /** Peer streaming the copy, or null when no sync is running. */
  sourcePeerId: string | null
  /** Change-log position the copy was taken at. */
  snapshotSeq: bigint | null
  /** Tables already copied in full. */
  completedTables: string[]
  /** Tables the copy covers. */
  totalTables: number
  /** Milliseconds since the Unix epoch, taken when the sync started. */
  startedAt: number | null
  /** Why the sync failed, or null while it is going well. */
  error: string | null
}

/**
 * Asks a peer to stream a full copy of the database.
 *
 * @public
 */
export interface SyncRequest {
  /** Identifier every message of this sync carries. */
  requestId: string
  /** Identifier of the node asking for the copy. */
  joinerNodeId: string
  /** Tables the joiner already holds in full, so a resumed sync skips them. */
  completedTables: string[]
  /** Whether the joiner verifies the stream with chained batch digests. */
  supportsStreamVerification?: boolean
  /** Replication group the joiner belongs to. */
  groupId?: string
  /** Primary term the joiner reports as current. */
  primaryTerm?: bigint
}

/**
 * One page of first-sync table data.
 *
 * @public
 */
export interface SyncBatch {
  /** Identifier of the sync this page belongs to. */
  requestId: string
  /** Table these rows come from. */
  table: string
  /** Position of this page in the table's stream, counting from zero. */
  batchIndex: number
  /** The rows themselves. */
  rows: Record<string, unknown>[]
  /** Schema statements, sent with the first page so the joiner can build the tables. */
  schema?: string[]
  /** Checksum of the rows, which the joiner verifies before it writes them. */
  checksum: string
  /** Set on the last page of a table. */
  isLastBatchForTable: boolean
  /** Tables the whole copy covers. */
  totalTables?: number
  /** Replication group the source belongs to. */
  groupId?: string
  /** Primary term the source held. */
  primaryTerm?: bigint
}

/**
 * Tells a joining node that first sync has finished, and carries what it needs to verify the copy.
 *
 * @public
 */
export interface SyncComplete {
  /** Identifier of the sync that finished. */
  requestId: string
  /** Change-log position the copy was taken at, which the joiner resumes from. */
  snapshotSeq: bigint
  /** One manifest per table, so the joiner can check what it received. */
  manifests: SyncTableManifest[]
  /** Replication group the source belongs to. */
  groupId?: string
  /** Primary term the source held. */
  primaryTerm?: bigint
}

/**
 * Confirms to the source that a joining node stored one first-sync page, or says why it could not.
 *
 * @public
 */
export interface SyncAck {
  /** Identifier of the sync this acknowledgement belongs to. */
  requestId: string
  /** Identifier of the node that received the page. */
  joinerNodeId: string
  /** Table the page came from. */
  table: string
  /** Position of the page in that table's stream. */
  batchIndex: number
  /** Whether the joiner stored the page. */
  success: boolean
  /** Why the joiner could not store it. */
  error?: string
  /** Replication group the joiner belongs to. */
  groupId?: string
  /** Primary term the joiner reports as current. */
  primaryTerm?: bigint
}
