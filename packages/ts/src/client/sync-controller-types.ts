import type { ConflictResolver } from '../core/sync/types.js'
import type { ChangeEvent } from '../core/types.js'
import type { SnapshotProgress } from './snapshot-loader.js'

/**
 * How a device keeps its local database in step with a server.
 *
 * @public
 */
export interface SyncControllerOptions {
  /** Address of the server this device syncs with. */
  url: string
  /** Identifier of the database on that server. */
  databaseId: string
  /** Tables this device syncs. */
  tables: readonly string[]
  /** Headers attached to every request and to the WebSocket upgrade. */
  headers?: Record<string, string>
  /** Changes sent in one push. */
  batchSize?: number
  /** Milliseconds between pushes of locally recorded changes. */
  pushIntervalMs?: number
  /** Milliseconds between acknowledgements of pulled changes. */
  ackIntervalMs?: number
  /** Longest delay, in milliseconds, between retries of a failed push. */
  maxPushRetryDelayMs?: number
  /** Milliseconds a single request may take. */
  requestTimeout?: number
  /** Downloads a fresh snapshot on its own when the server says the cursor is too old. Default: true. */
  autoResync?: boolean
  /** Milliseconds before the first retry of a failed snapshot download. */
  snapshotRetryDelayMs?: number
  /** Longest delay, in milliseconds, between snapshot retries. */
  maxSnapshotRetryDelayMs?: number
  /** Rows requested per snapshot page. */
  snapshotPageSize?: number
  /** Changes after which the device acknowledges at once instead of waiting for the interval. */
  immediateAckAfterChanges?: number
  /** Resolves which version of a row wins when a pulled change meets a local one. */
  resolver?: ConflictResolver | ((table: string) => ConflictResolver)
  /** Called for each change this device pulls. */
  onChange?: (event: ChangeEvent) => void
  /** Called when the server says the device must download a fresh snapshot. */
  onResyncRequired?: () => void
  /** Called as each snapshot page arrives. */
  onSnapshotProgress?: (progress: SnapshotProgress) => void
  /** Called once a snapshot download settles, whether it succeeded or failed. */
  onSnapshotComplete?: (outcome: SnapshotOutcome) => void
}

/**
 * How a snapshot download settled, and whether the local database is usable afterwards.
 *
 * @public
 */
export type SnapshotOutcome =
  | { ok: true; error: null; databaseUsable: true; retrying: false }
  | { ok: false; error: { code: string; message: string }; databaseUsable: boolean; retrying: boolean }

/**
 * What a sync controller is doing right now.
 *
 * @public
 */
export type SyncState = 'stopped' | 'starting' | 'running' | 'paused' | 'snapshotting'

/**
 * Settings for one snapshot download.
 *
 * @public
 */
export interface SnapshotOptions {
  /** Rows requested per page. */
  pageSize?: number
  /** Called as each page arrives. */
  onProgress?: (progress: SnapshotProgress) => void
}

/**
 * Where a device stands against its server.
 *
 * @public
 */
export interface SyncStatus {
  /** What the controller is doing right now. */
  state: SyncState
  /** Identifier this device is known by on the server. */
  deviceId: string | null
  /** Capabilities the server announced, or null before the device has asked. */
  serverCapabilities: string[] | null
  /** Schema version the local database is at. */
  schemaVersion: number | null
  /** Local changes waiting to be pushed. */
  pendingPushCount: number
  /** Highest local change-log position the server has accepted. */
  lastPushedSeq: bigint
  /** Highest server change-log position this device has applied. */
  lastPulledSeq: bigint | null
  /** Whether every local change has reached the server. */
  pushCaughtUp: boolean
  /** Whether the server has told this device to download a fresh snapshot. */
  resyncRequired: boolean
  /** The most recent failure, or null when the last try succeeded. */
  lastError: { code: string; message: string } | null
}
