import type { ConflictResolver } from '../core/sync/types.js'
import type { ChangeEvent } from '../core/types.js'
import type { SnapshotProgress } from './snapshot-loader.js'

/**
 * Settings that control how a device keeps its local database in step with a server.
 *
 * @public
 */
export interface SyncControllerOptions {
  /** The address of the server that this device syncs with. */
  url: string
  /** The identifier of the database on that server. */
  databaseId: string
  /** The tables that this device syncs. */
  tables: readonly string[]
  /** Headers that the controller adds to every HTTP request, and to the pull subscription's WebSocket upgrade in a runtime whose WebSocket constructor accepts handshake headers. */
  headers?: Record<string, string>
  /** Subprotocols that the controller offers on the pull subscription's WebSocket upgrade, which is how a browser device sends a credential. The controller offers `sirannon.v1` ahead of them. */
  webSocketProtocols?: string | string[]
  /** The number of changes that the controller reads from the outbox for one push. */
  batchSize?: number
  /** Milliseconds between pushes of locally recorded changes. */
  pushIntervalMs?: number
  /** Milliseconds between acknowledgements of pulled changes. */
  ackIntervalMs?: number
  /** The longest delay, in milliseconds, between retries of a failed push. */
  maxPushRetryDelayMs?: number
  /** The time, in milliseconds, that the controller allows for a single request. */
  requestTimeout?: number
  /** Whether the controller downloads a fresh snapshot automatically when it marks the device for a resync. Defaults to true. */
  autoResync?: boolean
  /** Milliseconds before the first retry of a failed snapshot download. */
  snapshotRetryDelayMs?: number
  /** The longest delay, in milliseconds, between snapshot retries. */
  maxSnapshotRetryDelayMs?: number
  /** The number of rows that the controller requests per snapshot page. */
  snapshotPageSize?: number
  /** The number of unacknowledged pulled changes above which the device acknowledges at once, ahead of `ackIntervalMs`. */
  immediateAckAfterChanges?: number
  /** The conflict resolver that the controller applies when a pulled change conflicts with a local change, or a function that returns one for each table. */
  resolver?: ConflictResolver | ((table: string) => ConflictResolver)
  /** Called with each change that this device pulls. */
  onChange?: (event: ChangeEvent) => void
  /** Called with this device's status when the controller changes state, pushes a batch, applies a pulled batch, marks the device for a resync, or records or clears an error. */
  onStatusChange?: (status: SyncStatus) => void
  /** Called when the controller marks the device for a fresh snapshot download. */
  onResyncRequired?: () => void
  /** Called after the controller loads each snapshot page that contains rows. */
  onSnapshotProgress?: (progress: SnapshotProgress) => void
  /** Called with the outcome when a snapshot download succeeds or fails. */
  onSnapshotComplete?: (outcome: SnapshotOutcome) => void
}

/**
 * The result of a snapshot download, and whether the local database is usable afterwards.
 *
 * @public
 */
export type SnapshotOutcome =
  | {
      /** True when the download finishes and its rows are in place. */
      ok: true
      /** Null when the download finishes. */
      error: null
      /** True, because the local database is ready to read after a download finishes. */
      databaseUsable: true
      /** False, because the controller schedules no retry after a download finishes. */
      retrying: false
    }
  | {
      /** False when the download fails. */
      ok: false
      /** The code and message of the error that stopped the download. */
      error: { code: string; message: string }
      /** Whether the local database still accepts reads and writes after the failure. */
      databaseUsable: boolean
      /** True when the controller has another attempt scheduled. */
      retrying: boolean
    }

/**
 * The current state of a sync controller.
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
  /** The number of rows that the controller requests per page. */
  pageSize?: number
  /** Called after the controller loads each page that contains rows. */
  onProgress?: (progress: SnapshotProgress) => void
}

/**
 * The sync status of a device against its server.
 *
 * @public
 */
export interface SyncStatus {
  /** The controller's current state. */
  state: SyncState
  /** The identifier that this device uses with the server, or null before the controller starts. */
  deviceId: string | null
  /** The capabilities that the server announces, or null before the controller fetches them. */
  serverCapabilities: string[] | null
  /** The schema version of the local database. */
  schemaVersion: number | null
  /** The number of local changes that the controller has not yet pushed. */
  pendingPushCount: number
  /** The highest local change-log position that the server acknowledges. */
  lastPushedSeq: bigint
  /** The server change-log position up to which the local database is current, or null before the device first subscribes. */
  lastPulledSeq: bigint | null
  /** True when the pending push count is 0. */
  pushCaughtUp: boolean
  /** Whether this device must download a fresh snapshot before it can pull changes again. */
  resyncRequired: boolean
  /** The most recent failure, or null when there is none or the controller clears it after a later success. */
  lastError: { code: string; message: string } | null
}
