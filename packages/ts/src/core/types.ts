import type { SQLiteDriver, SynchronousLevel } from './driver/types.js'
import type { Transaction } from './transaction.js'

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

/** Context passed to the onRequest middleware hook. */
export interface RequestContext {
  headers: Record<string, string>
  method: string
  path: string
  databaseId?: string
  remoteAddress: string
}

/** Return this from an onRequest hook to deny the request with a custom response. */
export interface RequestDenial {
  status: number
  code: string
  message: string
}

/** Middleware hook for auth, rate limiting, and request validation. */
export type OnRequestHook = (ctx: RequestContext) => undefined | RequestDenial | Promise<undefined | RequestDenial>

/**
 * Durability level in force while a bulk load runs. SQLite sanctions 'off' for
 * a from-scratch load that the operator can re-run after a power loss; 'off'
 * gives up corruption safety, so it fits only a load that starts from nothing.
 * 'normal' keeps WAL-mode corruption safety and suits loads into a database
 * that already holds data the operator cannot afford to lose.
 */
export type BulkLoadDurability = 'off' | 'normal'

export interface BulkLoadOptions {
  /** Durability during the load. Default: 'off'. */
  durability?: BulkLoadDurability
  /**
   * Whether this load ends with a WAL checkpoint. Default: true. Set it false
   * on every load but the last of a multi-batch import so the one fsyncing
   * checkpoint is paid once at the end instead of once per batch; the
   * configured durability is still restored after each batch regardless, so an
   * abandoned import never leaves the writer at the relaxed level.
   */
  checkpoint?: boolean
}

/** Aggregate outcome of a bulk load. Summed rather than per-row so a
 * million-row load never holds a million result objects in memory. */
export interface BulkLoadResult {
  rowsLoaded: number
  changes: number
}

export interface ServerExecutionTarget {
  query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]>
  /**
   * Optional single-pass read that returns rows already encoded for the wire
   * (safe-range integers as plain numbers, larger integers and BLOBs as tagged
   * envelopes). When present the server uses it instead of {@link query}
   * followed by a separate tag-encoding walk. A target that omits it stays
   * correct: the server falls back to encoding {@link query} rows itself.
   */
  queryForWire?(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]>
  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult>
  transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T>
  /**
   * Optional bulk-load entry point. Targets that proxy to a remote primary
   * may omit it; the server rejects load requests for such targets instead
   * of silently degrading to per-statement writes.
   */
  bulkLoad?(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult>
}

export type ServerExecutionTargetResolver = (
  databaseId: string,
) => ServerExecutionTarget | null | undefined | Promise<ServerExecutionTarget | null | undefined>

/** Options for the standalone HTTP + WS server. */
export interface ServerOptions {
  host?: string
  port?: number
  cors?: boolean | CorsOptions
  /**
   * Maximum HTTP request body and WebSocket message size in bytes. Applied
   * identically to both transports. Must be a positive, finite integer no
   * larger than 4_294_967_295 (the unsigned 32-bit ceiling uWebSockets.js can
   * store; larger values would silently wrap modulo 2^32).
   * Default: 1_048_576 (1 MB), matching the general web default and acting as
   * a denial-of-service guard on a memory-limited server.
   */
  maxBodyBytes?: number
  /**
   * Maximum bytes buffered per WebSocket connection before the server stops
   * absorbing backpressure. A single frame can be as large as `maxBodyBytes`,
   * so this must hold several of them; the resolved value is raised to at
   * least `maxBodyBytes` and, like `maxBodyBytes`, must not exceed
   * 4_294_967_295. When the buffer is exceeded the server closes the
   * connection so the client reconnects rather than losing a frame silently.
   * Default: the larger of 16 MB and `maxBodyBytes`.
   */
  maxWebSocketBackpressureBytes?: number
  /**
   * How long, in milliseconds, change events are retained for WebSocket CDC
   * subscriptions. Retention bounds both on-disk growth of the change log and
   * how far back a reconnecting subscriber can resume. Default: 3_600_000
   * (one hour).
   */
  cdcRetentionMs?: number
  onRequest?: OnRequestHook
  resolveExecutionTarget?: ServerExecutionTargetResolver
  getReplicationStatus?: () => ReplicationStatusInfo | null
  getClusterStatus?: (databaseId: string) => ClusterStatusInfo | null
}

export interface ReplicationStatusInfo {
  role: string
  writeForwarding: boolean
  peers: number
  localSeq: bigint
  replicationGroupId?: string
  primaryTerm?: bigint
  currentPrimary?: string
  coordinator?: {
    connected: boolean
    authority: boolean
  }
  controller?: {
    state: 'disabled' | 'standby' | 'active' | 'lost'
  }
  inSyncReplicas?: string[]
  laggingReplicas?: string[]
  syncState?: string
  readAvailability?: 'available' | 'unavailable'
  writeAvailability?: 'available' | 'unavailable'
}

/** CORS configuration. */
export interface CorsOptions {
  origin?: string | string[]
  methods?: string[]
  headers?: string[]
}

/** Options for the mountable WebSocket handler. */
export interface WSHandlerOptions {
  /** Maximum message size in bytes. Default: 1_048_576 (1 MB). */
  maxPayloadLength?: number
  /** Change-log retention for CDC subscriptions in milliseconds. Default: 3_600_000. */
  cdcRetentionMs?: number
  resolveExecutionTarget?: ServerExecutionTargetResolver
}

/** Options for the client SDK. */
export interface ClientOptions {
  /** Transport to use. Default: 'websocket'. */
  transport?: 'websocket' | 'http'
  /** Custom headers for HTTP requests. */
  headers?: Record<string, string>
  /** WebSocket subprotocols sent during the browser-compatible handshake. */
  webSocketProtocols?: string | string[]
  /** Reconnect on WebSocket disconnect. Default: true. */
  autoReconnect?: boolean
  /** Reconnect interval in ms. Default: 1000. */
  reconnectInterval?: number
  /**
   * Per-request timeout in milliseconds for the WebSocket transport. A bulk
   * load or batch of tens of millions of rows can legitimately run longer than
   * the default, so raise this for large writes. Set to 0 to wait indefinitely.
   * Default: 30000.
   */
  requestTimeout?: number
}
