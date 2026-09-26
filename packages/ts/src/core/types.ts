import type { BackupCycleOptions } from './backup/cycle-options.js'
import type { BackupFileReport } from './backup/report.js'
import type { SQLiteDriver, SynchronousLevel } from './driver/types.js'
import type { HookConfig } from './hook-types.js'
import type { MetricsConfig } from './metrics-types.js'
import type { MigrationSource } from './migrations/types.js'
import type { ReadConcernLevel } from './query-types.js'

/** One node that a client can read from, and the read concerns that the node meets now.
 * @public
 */
export interface ClusterReadEndpointInfo {
  /** The identifier of the node at this endpoint. */
  nodeId: string
  /** The address that a client sends its reads to. */
  endpoint: string
  /** The read concerns that this node meets now. */
  readConcerns: ReadConcernLevel[]
}

/** Every value that a node can report as its health state.
 * @public
 */
export const NODE_HEALTH_STATES = [
  'healthy',
  'degraded',
  'failing_over',
  'repairing',
  'syncing',
  'unavailable',
] as const

/** The word that names what a node can do now.
 * @public
 */
export type NodeHealthState = (typeof NODE_HEALTH_STATES)[number]

/** The condition behind a {@link NodeHealthState}.
 * @public
 */
export type NodeHealthReason = (typeof NODE_HEALTH_REASONS)[number]

/** Every value that a node can report as the reason for its health state.
 * @public
 */
export const NODE_HEALTH_REASONS = [
  'in-sync',
  'lagging',
  'coordinator-unreachable',
  'draining',
  'repairing',
  'faulted',
  'sync-pending',
  'no-group-state',
] as const

/**
 * The health of the one node that reports it.
 *
 * `canRead` and `canWrite` are true when that node accepts reads and writes
 * now, and `state` and `reason` name the condition behind them.
 *
 * @public
 */
export interface NodeHealth {
  /** What the node can do now. */
  state: NodeHealthState
  /** The condition behind that state. */
  reason: NodeHealthReason
  /** Whether the node serves reads now. */
  canRead: boolean
  /** Whether the node accepts writes now. */
  canWrite: boolean
}

/** What one node reports about its replication group, which `GET /db/{id}/cluster` returns.
 * @public
 */
export interface ClusterStatusInfo {
  /** The identifier of the database that this status describes. */
  databaseId: string
  /** The identifier of the node's replication group. */
  replicationGroupId?: string
  /** Whether this node is the primary, which accepts writes, or a replica, which serves reads. */
  role?: 'primary' | 'replica'
  /** The primary that this node reports as current, or null when the node reports none. */
  currentPrimary?: { nodeId: string; endpoint?: string } | null
  /** The primary term that this node reports as current. */
  primaryTerm?: bigint
  /** Every node that a client can read from, with the read concerns that each one meets. */
  readEndpoints?: ClusterReadEndpointInfo[]
  /** What this node can do now. */
  health: NodeHealthState
  /** The condition behind that health state. */
  healthReason: NodeHealthReason
}

/** The settings for opening and closing databases automatically.
 * @public
 */
export interface LifecycleConfig {
  /** Opens a database the first time that a caller asks {@link Sirannon.resolve} for an identifier that has no open database. */
  autoOpen?: {
    resolver: (id: string) => { path: string; options?: DatabaseOptions } | undefined
  }
  /** The number of idle milliseconds after which the registry closes a database; 0 turns idle closing off. */
  idleTimeout?: number
  /** The most databases open at once before an automatic open evicts the least recently used one; 0 sets no limit. */
  maxOpen?: number
}

/** The options for opening one database.
 * @public
 */
export interface DatabaseOptions {
  /** Whether to open the database in read-only mode. */
  readOnly?: boolean
  /** The number of read connections in the pool; the default is 4. */
  readPoolSize?: number
  /** Whether to open the database in WAL mode; the default is true. */
  walMode?: boolean
  /**
   * The writer's `PRAGMA synchronous` level; the default is 'normal'. Sirannon
   * restores this level after every bulk load.
   */
  synchronous?: SynchronousLevel
  /** How often Sirannon polls the change log, in milliseconds; the default is 50. */
  cdcPollInterval?: number
  /** How long Sirannon keeps change-log entries, in milliseconds; the default is 3_600_000, one hour. */
  cdcRetention?: number
  /**
   * How long, in milliseconds, Sirannon keeps changes for a device's cursor.
   * Sirannon drops the cursor when the cursor goes this long without an update,
   * or when the oldest change that the device still needs reaches this age. The
   * default is 2_592_000_000, 30 days.
   */
  deviceCursorRetention?: number
  /**
   * The most changes that Sirannon keeps for one device's cursor. Sirannon
   * drops the cursor of a device that falls further behind than this, and that
   * device then downloads the database again when it reconnects. 0 sets no
   * limit, and the default is 0.
   */
  maxChangesHeldForDevice?: number
  /**
   * Whether to execute writes on a dedicated worker thread, so that disk
   * flushes never block the thread that serves connections, while reads stay
   * on the calling thread. The driver needs a worker entry, which the
   * `better-sqlite3` and `node` drivers have, and with any other driver the open
   * throws `WRITER_WORKER_UNSUPPORTED`. The default is off.
   */
  writerWorker?: boolean | WriterWorkerOptions
  /**
   * Captures this database's write-ahead log to a destination that you supply,
   * on an interval. Apart from the full copies, each capture sends only the log
   * frames written since the previous capture, so its size follows how much
   * the database changed.
   *
   * With this option, Sirannon turns off SQLite's automatic checkpoint and
   * checkpoints the log itself after each capture, because a checkpoint lets
   * SQLite overwrite log frames that no capture has read yet. The default is
   * off.
   */
  backups?: BackupCycleOptions
}

/** The limits and recovery settings for the writer worker thread.
 * @public
 */
export interface WriterWorkerOptions {
  /** The number of writes allowed in flight before Sirannon rejects new writes with a `WriteOverloadError`; the default is 1024. */
  maxPendingWrites?: number
  /** The deadline for each operation, in milliseconds; the default is 30000, and 0 turns it off. At the deadline, Sirannon asks the worker to cancel the operation, and when the worker gives no answer within a second deadline, Sirannon rejects the caller's promise while the worker continues, so the outcome of that write is unknown. */
  writeTimeoutMs?: number
  /** How many times Sirannon restarts the worker after the worker crashes on its own, before writes fail permanently; the default is 5. */
  maxRestarts?: number
}

/** The options for a Sirannon database registry.
 * @public
 */
export interface SirannonOptions {
  /** The SQLite driver that opens every database in this registry. */
  driver: SQLiteDriver
  /** The hooks that Sirannon calls for every database in this registry. */
  hooks?: HookConfig
  /** The callbacks that receive Sirannon's metrics. */
  metrics?: MetricsConfig
  /** The settings for automatic opening, idle closing, and the limit on open databases. */
  lifecycle?: LifecycleConfig
  /** The migrations that Sirannon applies to each writable database in this registry as it opens. */
  migrations?: MigrationSource
  /** The default writer-worker setting for the databases that this registry opens. */
  writerWorker?: boolean | WriterWorkerOptions
  /** The default change-log retention, in milliseconds, for the databases that this registry opens. */
  cdcRetention?: number
  /** The default device-cursor retention, in milliseconds, for the databases that this registry opens. */
  deviceCursorRetention?: number
  /** The default limit on the changes that Sirannon keeps for one device's cursor, for the databases that this registry opens. */
  maxChangesHeldForDevice?: number
}

/** The options for scheduled backups.
 * @public
 */
export interface BackupScheduleOptions {
  /** The cron expression, such as '0 * * * *' for every hour. */
  cron: string
  /** The directory that Sirannon writes the backup files to. */
  destDir: string
  /** The most backup files that Sirannon keeps; the default is 5. */
  maxFiles?: number
  /**
   * The IANA time zone, such as 'America/New_York', in which Sirannon evaluates
   * the cron expression. When you omit it, Sirannon uses the host's local time
   * zone and its daylight saving rules.
   */
  timezone?: string
  /**
   * Sirannon calls this after each copy that the schedule finishes, with the
   * file that it wrote and the copy's report. Use it to send the file to durable
   * storage, or to record that the schedule is still working.
   *
   * Sirannon awaits the promise that this returns and deletes the older files
   * only once that promise settles, so that it deletes no copy that your code
   * is still uploading. Sirannon passes a failure of this callback, like a
   * failed copy, to {@link BackupScheduleOptions.onError}, and
   * {@link BackupScheduleOptions.onBackupTimeoutMs} limits the wait.
   */
  onBackup?: (report: BackupFileReport) => void | Promise<void>
  /**
   * How many milliseconds {@link BackupScheduleOptions.onBackup} may take
   * before Sirannon stops waiting for it; the default is 600_000, ten minutes,
   * and 0 waits with no limit.
   *
   * Sirannon deletes the older files and takes the next copy once the callback
   * settles or this deadline passes, so a callback that hangs on a socket
   * delays the schedule by this long at most. Past the deadline, Sirannon
   * reports the timeout through {@link BackupScheduleOptions.onError} and
   * continues the schedule while your callback keeps working. Sirannon then
   * counts that copy among the files that it may delete, so set this longer
   * than your slowest upload.
   */
  onBackupTimeoutMs?: number
  /** Sirannon calls this when a scheduled backup fails, and discards the error when you omit it. */
  onError?: (error: Error) => void
}

export type {
  AfterQueryHook,
  AfterQueryHookContext,
  BeforeConnectHook,
  BeforePushHook,
  BeforeQueryHook,
  BeforeSnapshotHook,
  BeforeSubscribeHook,
  ConnectionHookContext,
  DatabaseCloseHook,
  DatabaseOpenHook,
  HookConfig,
  QueryHookContext,
} from './hook-types.js'
export type { CDCMetrics, ConnectionMetrics, MetricsConfig, QueryMetrics } from './metrics-types.js'
export * from './operation-registry.js'
export type {
  ChangeEvent,
  ChangeOperation,
  ExecuteResult,
  Params,
  QueryOptions,
  ReadConcern,
  ReadConcernLevel,
  Subscription,
  SubscriptionBuilder,
  SubscriptionOptions,
  WriteConcern,
  WriteConcernLevel,
} from './query-types.js'
export * from './server-options.js'
