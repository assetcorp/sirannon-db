import type { SQLiteDriver, SynchronousLevel } from './driver/types.js'
import type { MigrationSource } from './migrations/types.js'

/** Query parameter types: named (object) or positional (array). */
export type Params = Record<string, unknown> | unknown[]

export type WriteConcernLevel = 'local' | 'majority' | 'all'
export interface WriteConcern {
  level: WriteConcernLevel
  timeoutMs?: number
}
export type ReadConcernLevel = 'local' | 'majority' | 'linearizable'
export interface ReadConcern {
  level: ReadConcernLevel
}
export interface QueryOptions {
  writeConcern?: WriteConcern
  readConcern?: ReadConcern
}

export interface ClusterReadEndpointInfo {
  nodeId: string
  endpoint: string
  readConcerns: ReadConcernLevel[]
}

export interface ClusterStatusInfo {
  databaseId: string
  replicationGroupId?: string
  role?: 'primary' | 'replica'
  currentPrimary?: { nodeId: string; endpoint?: string } | null
  primaryTerm?: bigint
  readEndpoints?: ClusterReadEndpointInfo[]
  health: 'healthy' | 'degraded' | 'failing_over' | 'unavailable' | 'repairing' | 'syncing'
}

/** Result returned by mutation statements (INSERT, UPDATE, DELETE). */
export interface ExecuteResult {
  changes: number
  lastInsertRowId: number | bigint
}

/** CDC operation type. */
export type ChangeOperation = 'insert' | 'update' | 'delete'

/** Event emitted when a watched table row changes. */
export interface ChangeEvent<T = Record<string, unknown>> {
  type: ChangeOperation
  table: string
  row: T
  oldRow?: T
  seq: bigint
  timestamp: number
  hlc?: string
  origin?: string
}

/** Context passed to query hooks. */
export interface QueryHookContext {
  databaseId: string
  sql: string
  params?: Params
  metadata?: Record<string, unknown>
  writeConcern?: WriteConcern
  readConcern?: ReadConcern
}

/** Hook invoked before a query is executed. Throw to deny. */
export type BeforeQueryHook = (ctx: QueryHookContext) => void | Promise<void>

/** Hook invoked after a query is executed. */
export type AfterQueryHook = (ctx: QueryHookContext & { durationMs: number }) => void | Promise<void>

/** Context passed to connection hooks. */
export interface ConnectionHookContext {
  databaseId: string
  path: string
}

/** Hook invoked before a database connection is established. */
export type BeforeConnectHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked when a database is opened. */
export type DatabaseOpenHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked when a database is closed. */
export type DatabaseCloseHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked before a subscription is created. Throw to deny. */
export type BeforeSubscribeHook = (ctx: {
  databaseId: string
  table: string
  filter?: Record<string, unknown>
}) => void | Promise<void>

/** Aggregated hook configuration. */
export interface HookConfig {
  onBeforeQuery?: BeforeQueryHook | BeforeQueryHook[]
  onAfterQuery?: AfterQueryHook | AfterQueryHook[]
  onBeforeConnect?: BeforeConnectHook | BeforeConnectHook[]
  onDatabaseOpen?: DatabaseOpenHook | DatabaseOpenHook[]
  onDatabaseClose?: DatabaseCloseHook | DatabaseCloseHook[]
  onBeforeSubscribe?: BeforeSubscribeHook | BeforeSubscribeHook[]
}

/** Metrics emitted after a query completes. */
export interface QueryMetrics {
  databaseId: string
  sql: string
  durationMs: number
  rowsReturned?: number
  changes?: number
  error?: boolean
}

/** Metrics emitted when a connection opens or closes. */
export interface ConnectionMetrics {
  databaseId: string
  path: string
  readerCount: number
  event: 'open' | 'close'
}

/** Metrics emitted when a CDC event is dispatched. */
export interface CDCMetrics {
  databaseId: string
  table: string
  operation: ChangeOperation
  subscriberCount: number
}

/** Callbacks for metrics collection. */
export interface MetricsConfig {
  onQueryComplete?: (metrics: QueryMetrics) => void
  onConnectionOpen?: (metrics: ConnectionMetrics) => void
  onConnectionClose?: (metrics: ConnectionMetrics) => void
  onCDCEvent?: (metrics: CDCMetrics) => void
}

/** Configuration for automatic database lifecycle management. */
export interface LifecycleConfig {
  autoOpen?: {
    resolver: (id: string) => { path: string; options?: DatabaseOptions } | undefined
  }
  /** Milliseconds before an idle database is closed. 0 = disabled. */
  idleTimeout?: number
  /** Maximum number of concurrently open databases. 0 = unlimited. */
  maxOpen?: number
}

/** Options for opening a single database. */
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

export interface WriterWorkerOptions {
  /** Writes allowed in flight before new writes are rejected with a busy signal. Default: 1024. */
  maxPendingWrites?: number
  /** Per-operation deadline in ms; when an operation stalls past it, its caller is rejected loudly while the worker keeps running, so a stalled write's outcome is indeterminate. 0 disables it. Default: 30000. */
  writeTimeoutMs?: number
  /** Restarts the worker this many times after it crashes on its own before writes fail permanently. Default: 5. */
  maxRestarts?: number
}

/** Top-level options for the Sirannon database registry. */
export interface SirannonOptions {
  driver: SQLiteDriver
  hooks?: HookConfig
  metrics?: MetricsConfig
  lifecycle?: LifecycleConfig
  migrations?: MigrationSource
  writerWorker?: boolean | WriterWorkerOptions
}

/** Options for scheduled backups. */
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

/** Builder for creating CDC subscriptions with optional filters. */
export interface SubscriptionBuilder {
  filter(conditions: Record<string, unknown>): SubscriptionBuilder
  subscribe(callback: (event: ChangeEvent) => void): Subscription
}

/** Handle for an active subscription. */
export interface Subscription {
  unsubscribe(): void
}

export * from './server-options.js'
