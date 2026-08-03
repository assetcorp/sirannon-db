import type { SQLiteDriver, SynchronousLevel } from './driver/types.js'
import type { HookConfig } from './hook-types.js'
import type { MetricsConfig } from './metrics-types.js'
import type { MigrationSource } from './migrations/types.js'
import type { ReadConcernLevel } from './query-types.js'

/** One node a client can read from, and the read concerns it currently serves.
 * @public
 */
export interface ClusterReadEndpointInfo {
  /** Identifier of the node behind this endpoint. */
  nodeId: string
  /** Address a client sends its reads to. */
  endpoint: string
  /** Read concerns this node meets right now. */
  readConcerns: ReadConcernLevel[]
}

/** The single word describing what a node can do right now.
 * @public
 */
export type NodeHealthState = 'healthy' | 'degraded' | 'failing_over' | 'repairing' | 'syncing' | 'unavailable'

/** The condition that produced a {@link NodeHealthState}.
 * @public
 */
export type NodeHealthReason =
  | 'in-sync'
  | 'lagging'
  | 'coordinator-unreachable'
  | 'draining'
  | 'repairing'
  | 'faulted'
  | 'sync-pending'
  | 'no-group-state'

/**
 * The health of one node, covering only the node that reports it.
 *
 * `canRead` and `canWrite` are what that node will accept at this moment;
 * `state` and `reason` name the condition behind them.
 *
 * @public
 */
export interface NodeHealth {
  /** What the node can do right now. */
  state: NodeHealthState
  /** The condition behind that state. */
  reason: NodeHealthReason
  /** Whether the node serves reads at this moment. */
  canRead: boolean
  /** Whether the node accepts writes at this moment. */
  canWrite: boolean
}

/** What one node reports about its replication group, as served by `GET /db/{id}/cluster`.
 * @public
 */
export interface ClusterStatusInfo {
  /** Identifier of the database this status describes. */
  databaseId: string
  /** Identifier of the replication group the node belongs to. */
  replicationGroupId?: string
  /** Whether this node accepts writes or serves reads. */
  role?: 'primary' | 'replica'
  /** The primary this node reports as current, or null when it has none. */
  currentPrimary?: { nodeId: string; endpoint?: string } | null
  /** The primary term this node reports as current. */
  primaryTerm?: bigint
  /** Every node a client can read from, with the read concerns each one serves. */
  readEndpoints?: ClusterReadEndpointInfo[]
  /** What this node can do right now. */
  health: NodeHealthState
  /** The condition behind that health. */
  healthReason: NodeHealthReason
}

/** Configuration for automatic database lifecycle management.
 * @public
 */
export interface LifecycleConfig {
  /** Opens a database the first time someone asks for an identifier the registry has not seen. */
  autoOpen?: {
    resolver: (id: string) => { path: string; options?: DatabaseOptions } | undefined
  }
  /** Milliseconds before an idle database is closed. 0 = disabled. */
  idleTimeout?: number
  /** Maximum number of concurrently open databases. 0 = unlimited. */
  maxOpen?: number
}

/** Options for opening a single database.
 * @public
 */
export interface DatabaseOptions {
  /** Open the database in read-only mode. */
  readOnly?: boolean
  /** Number of read connections in the pool. Default: 4. */
  readPoolSize?: number
  /** Enable WAL mode. Default: true. */
  walMode?: boolean
  /**
   * Writer durability (`PRAGMA synchronous`). Default: 'normal'. This is the
   * level restored after every bulk load, whatever the load relaxed it to.
   */
  synchronous?: SynchronousLevel
  /** CDC polling interval in milliseconds. Default: 50. */
  cdcPollInterval?: number
  /** CDC retention period in milliseconds. Default: 3_600_000 (1 hour). */
  cdcRetention?: number
  /**
   * Run writes on a dedicated worker thread so disk flushes never block the
   * thread serving connections; reads stay on the calling thread. Requires a
   * driver with a worker entry (the `better-sqlite3` and `node` drivers have
   * one), otherwise opening throws. Default: off.
   */
  writerWorker?: boolean | WriterWorkerOptions
}

/** Limits and recovery settings for the thread that runs writes.
 * @public
 */
export interface WriterWorkerOptions {
  /** Writes allowed in flight before new writes are rejected with a busy signal. Default: 1024. */
  maxPendingWrites?: number
  /** Per-operation deadline in ms; when an operation stalls past it, its caller is rejected loudly while the worker keeps running, so a stalled write's outcome is indeterminate. 0 disables it. Default: 30000. */
  writeTimeoutMs?: number
  /** Restarts the worker this many times after it crashes on its own before writes fail permanently. Default: 5. */
  maxRestarts?: number
}

/** Top-level options for the Sirannon database registry.
 * @public
 */
export interface SirannonOptions {
  /** SQLite driver every database in this registry opens through. */
  driver: SQLiteDriver
  /** Lifecycle hooks that run for every database in this registry. */
  hooks?: HookConfig
  /** Callbacks that receive statement, connection, and change-capture metrics. */
  metrics?: MetricsConfig
  /** Automatic opening, idle eviction, and the limit on concurrently open databases. */
  lifecycle?: LifecycleConfig
  /** Migrations every database in this registry applies when it opens. */
  migrations?: MigrationSource
  /** Default writer-worker setting for the databases this registry opens. */
  writerWorker?: boolean | WriterWorkerOptions
}

/** Options for scheduled backups.
 * @public
 */
export interface BackupScheduleOptions {
  /** Cron expression (e.g., '0 * * * *' for hourly). */
  cron: string
  /** Directory to store backup files. */
  destDir: string
  /** Maximum number of backup files to keep. Default: 5. */
  maxFiles?: number
  /**
   * Sirannon evaluates the cron expression in this IANA time zone (e.g. 'America/New_York').
   * When omitted, it uses the host's local time zone, which also sets the daylight saving rules that apply.
   */
  timezone?: string
  /** Called when a scheduled backup fails. Without this, errors are silently discarded. */
  onError?: (error: Error) => void
}

export type {
  AfterQueryHook,
  BeforeConnectHook,
  BeforeQueryHook,
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
  WriteConcern,
  WriteConcernLevel,
} from './query-types.js'
export * from './server-options.js'
