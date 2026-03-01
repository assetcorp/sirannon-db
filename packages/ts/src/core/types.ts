/** Query parameter types: named (object) or positional (array). */
export type Params = Record<string, unknown> | unknown[]

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
  /** CDC polling interval in milliseconds. Default: 50. */
  cdcPollInterval?: number
  /** CDC retention period in milliseconds. Default: 3_600_000 (1 hour). */
  cdcRetention?: number
}

/** Top-level options for the Sirannon database registry. */
export interface SirannonOptions {
  hooks?: HookConfig
  metrics?: MetricsConfig
  lifecycle?: LifecycleConfig
}

/** A single migration file descriptor. */
export interface MigrationFile {
  version: number
  name: string
  sql: string
}

/** Result of running migrations. */
export interface MigrationResult {
  applied: MigrationFile[]
  skipped: number
}

/** Options for scheduled backups. */
export interface BackupScheduleOptions {
  /** Cron expression (e.g., '0 * * * *' for hourly). */
  cron: string
  /** Directory to store backup files. */
  destDir: string
  /** Maximum number of backup files to keep. Default: 5. */
  maxFiles?: number
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

/** Options for the standalone HTTP + WS server. */
export interface ServerOptions {
  host?: string
  port?: number
  cors?: boolean | CorsOptions
  auth?: (req: { headers: Record<string, string> }) => boolean | Promise<boolean>
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
}

/** Options for the client SDK. */
export interface ClientOptions {
  /** Transport to use. Default: 'websocket'. */
  transport?: 'websocket' | 'http'
  /** Custom headers for HTTP requests. */
  headers?: Record<string, string>
  /** Reconnect on WebSocket disconnect. Default: true. */
  autoReconnect?: boolean
  /** Reconnect interval in ms. Default: 1000. */
  reconnectInterval?: number
}
