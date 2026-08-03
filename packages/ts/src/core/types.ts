import type { SQLiteDriver, SynchronousLevel } from './driver/types.js'
import type { MigrationSource } from './migrations/types.js'

/** Query parameter types: named (object) or positional (array).
 * @public
 */
export type Params = Record<string, unknown> | unknown[]

/** How many nodes must acknowledge a write before it returns.
 * @public
 */
export type WriteConcernLevel = 'local' | 'majority' | 'all'

/** How many nodes must acknowledge a write, and how long the caller waits for them.
 * @public
 */
export interface WriteConcern {
  /** Number of acknowledgements the write waits for. */
  level: WriteConcernLevel
  /** Milliseconds to wait for those acknowledgements before the write fails. */
  timeoutMs?: number
}

/** How current a read has to be before the node will serve it.
 * @public
 */
export type ReadConcernLevel = 'local' | 'majority' | 'linearizable'

/** How current a read has to be before the node will serve it.
 * @public
 */
export interface ReadConcern {
  /** Currency the node must prove before it answers. */
  level: ReadConcernLevel
}

/** Per-statement settings you pass alongside the SQL and its parameters.
 * @public
 */
export interface QueryOptions {
  /** Acknowledgements a write waits for. Coordinator mode applies 'majority' when you omit it. */
  writeConcern?: WriteConcern
  /** Currency a read requires. Coordinator mode enforces it and static mode ignores it. */
  readConcern?: ReadConcern
}

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
  /** The primary the node believes is current, or null when it knows of none. */
  currentPrimary?: { nodeId: string; endpoint?: string } | null
  /** The primary term the node believes is current. */
  primaryTerm?: bigint
  /** Every node a client can read from, with the read concerns each one serves. */
  readEndpoints?: ClusterReadEndpointInfo[]
  /** What this node can do right now. */
  health: NodeHealthState
  /** The condition behind that health. */
  healthReason: NodeHealthReason
}

/** Result returned by mutation statements (INSERT, UPDATE, DELETE).
 * @public
 */
export interface ExecuteResult {
  /** Number of rows the statement inserted, updated, or deleted. */
  changes: number
  /** Row id SQLite assigned to the last inserted row. */
  lastInsertRowId: number | bigint
}

/** CDC operation type.
 * @public
 */
export type ChangeOperation = 'insert' | 'update' | 'delete'

/** Event emitted when a watched table row changes.
 * @public
 */
export interface ChangeEvent<T = Record<string, unknown>> {
  /** Whether the row was inserted, updated, or deleted. */
  type: ChangeOperation
  /** Table the row belongs to. */
  table: string
  /** The row as it stands after the change. A delete carries the row as it was. */
  row: T
  /** The row as it stood before an update or a delete. */
  oldRow?: T
  /** Position of this change in the database's change log. Subscribers resume from it. */
  seq: bigint
  /** Milliseconds since the Unix epoch, taken when the change was recorded. */
  timestamp: number
  /** Hybrid logical clock stamp the writing node gave this change. */
  hlc?: string
  /** Identifier of the node that authored the change. */
  origin?: string
  /** Primary key of the changed row, encoded as a string. */
  rowId?: string
  /** Identifier of the transaction that produced this change. */
  txId?: string
  /** Set on the last change of a transaction, so a consumer applies the whole transaction at once. */
  txEnd?: boolean
}

/** Context passed to query hooks.
 * @public
 */
export interface QueryHookContext {
  /** Identifier of the database the statement runs against. */
  databaseId: string
  /** The statement about to run, or the one that just ran. */
  sql: string
  /** Parameters bound to the statement. */
  params?: Params
  /** Values a caller attached to the request for its own hooks to read. */
  metadata?: Record<string, unknown>
  /** Acknowledgements this write waits for. */
  writeConcern?: WriteConcern
  /** Currency this read requires. */
  readConcern?: ReadConcern
}

/** Hook invoked before a query is executed. Throw to deny.
 * @public
 */
export type BeforeQueryHook = (ctx: QueryHookContext) => void | Promise<void>

/** Hook invoked after a query is executed.
 * @public
 */
export type AfterQueryHook = (ctx: QueryHookContext & { durationMs: number }) => void | Promise<void>

/** Context passed to connection hooks.
 * @public
 */
export interface ConnectionHookContext {
  /** Identifier of the database being opened or closed. */
  databaseId: string
  /** File path of the SQLite database. */
  path: string
}

/** Hook invoked before a database connection is established.
 * @public
 */
export type BeforeConnectHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked when a database is opened.
 * @public
 */
export type DatabaseOpenHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked when a database is closed.
 * @public
 */
export type DatabaseCloseHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked before a subscription is created. Throw to deny.
 * @public
 */
export type BeforeSubscribeHook = (ctx: {
  databaseId: string
  table: string
  filter?: Record<string, unknown>
}) => void | Promise<void>

/** Aggregated hook configuration.
 * @public
 */
export interface HookConfig {
  /** Runs before each statement. Throw to refuse it. */
  onBeforeQuery?: BeforeQueryHook | BeforeQueryHook[]
  /** Runs after each statement, with the time it took. */
  onAfterQuery?: AfterQueryHook | AfterQueryHook[]
  /** Runs before a database connection opens. */
  onBeforeConnect?: BeforeConnectHook | BeforeConnectHook[]
  /** Runs once a database is open. */
  onDatabaseOpen?: DatabaseOpenHook | DatabaseOpenHook[]
  /** Runs once a database is closed. */
  onDatabaseClose?: DatabaseCloseHook | DatabaseCloseHook[]
  /** Runs before a change subscription starts. Throw to refuse it. */
  onBeforeSubscribe?: BeforeSubscribeHook | BeforeSubscribeHook[]
}

/** Metrics emitted after a query completes.
 * @public
 */
export interface QueryMetrics {
  /** Identifier of the database the statement ran against. */
  databaseId: string
  /** The statement that ran. */
  sql: string
  /** How long the statement took, in milliseconds. */
  durationMs: number
  /** Number of rows a read returned. */
  rowsReturned?: number
  /** Number of rows a write changed. */
  changes?: number
  /** Set when the statement threw. */
  error?: boolean
}

/** Metrics emitted when a connection opens or closes.
 * @public
 */
export interface ConnectionMetrics {
  /** Identifier of the database whose connection opened or closed. */
  databaseId: string
  /** File path of the SQLite database. */
  path: string
  /** Number of read connections the pool holds. */
  readerCount: number
  /** Whether the connection opened or closed. */
  event: 'open' | 'close'
}

/** Metrics emitted when a CDC event is dispatched.
 * @public
 */
export interface CDCMetrics {
  /** Identifier of the database the change came from. */
  databaseId: string
  /** Table the changed row belongs to. */
  table: string
  /** Whether the row was inserted, updated, or deleted. */
  operation: ChangeOperation
  /** Number of subscribers the event reached. */
  subscriberCount: number
}

/** Callbacks for metrics collection.
 * @public
 */
export interface MetricsConfig {
  /** Called once each statement finishes, whether it succeeded or threw. */
  onQueryComplete?: (metrics: QueryMetrics) => void
  /** Called when a database connection opens. */
  onConnectionOpen?: (metrics: ConnectionMetrics) => void
  /** Called when a database connection closes. */
  onConnectionClose?: (metrics: ConnectionMetrics) => void
  /** Called each time a change event reaches its subscribers. */
  onCDCEvent?: (metrics: CDCMetrics) => void
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

/** Builder for creating CDC subscriptions with optional filters.
 * @public
 */
export interface SubscriptionBuilder {
  /** Narrows the subscription to rows whose columns equal the given values. */
  filter(conditions: Record<string, unknown>): SubscriptionBuilder
  /** Starts the subscription and calls back on each change. */
  subscribe(callback: (event: ChangeEvent) => void): Subscription
}

/** Handle for an active subscription.
 * @public
 */
export interface Subscription {
  /** Ends the subscription, so the callback receives no further events. */
  unsubscribe(): void
}

export * from './operation-registry.js'
export * from './server-options.js'
