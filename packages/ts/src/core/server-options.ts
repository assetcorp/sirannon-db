import type { Transaction } from './transaction.js'
import type { ClusterStatusInfo, ExecuteResult, Params, QueryOptions } from './types.js'

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
   * Optional entry point for a transaction whose statements are all known
   * before it starts, which lets concurrent transactions share one commit. A
   * target that omits it stays correct: the server falls back to
   * {@link transaction} and runs the statements one at a time.
   */
  executeTransaction?(
    statements: readonly { sql: string; params?: Params }[],
    options?: QueryOptions,
  ): Promise<ExecuteResult[]>
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
