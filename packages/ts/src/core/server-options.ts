import type { OperationRegistry } from './operation-registry.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
import type { AppliedMigrationRow } from './system-catalog/index.js'
import type { Transaction } from './transaction.js'
import type { ClusterStatusInfo, ExecuteResult, NodeHealth, Params, QueryOptions } from './types.js'

/** Context passed to the authenticate hook.
 * @public
 */
export interface RequestContext {
  /** Request headers, with every name lower-cased. */
  headers: Record<string, string>
  /** HTTP method of the request, or the method of a WebSocket upgrade. */
  method: string
  /** Path the request arrived on. */
  path: string
  /** Identifier of the database the route addresses. */
  databaseId?: string
  /** Address the request came from. */
  remoteAddress: string
}

/**
 * Identifies the caller behind a request. Return the identity registered
 * operations then read, and throw a {@link RequestDeniedError} to refuse the
 * request with a status of your own.
 *
 * @public
 */
export type AuthenticateHook<Identity = unknown> = (
  ctx: RequestContext,
) => Identity | undefined | Promise<Identity | undefined>

/** Decides whether a caller may read the addresses of every node in the group.
 * @public
 */
export type ClusterStatusAuthorizer = (ctx: RequestContext) => boolean | Promise<boolean>

/**
 * Durability level in force while a bulk load runs. SQLite sanctions 'off' for
 * a from-scratch load that the operator can re-run after a power loss; 'off'
 * gives up corruption safety, so it fits only a load that starts from nothing.
 * 'normal' keeps WAL-mode corruption safety and suits loads into a database
 * that already holds data the operator cannot afford to lose.
 *
 * @public
 */
export type BulkLoadDurability = 'off' | 'normal'

/** Settings for one bulk load.
 * @public
 */
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
 * million-row load never holds a million result objects in memory.
 * @public
 */
export interface BulkLoadResult {
  /** Number of parameter sets the load applied. */
  rowsLoaded: number
  /** Number of rows the load inserted, updated, or deleted. */
  changes: number
}

/**
 * What the server runs statements against for one database. A local
 * `Database` satisfies it, and so does a proxy that forwards to another node.
 *
 * @public
 */
export interface ServerExecutionTarget {
  /** Runs a read and returns the rows. */
  query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]>
  /**
   * Optional single-pass read that returns rows already encoded for the wire
   * (safe-range integers as plain numbers, larger integers and BLOBs as tagged
   * envelopes). When present the server uses it instead of {@link ServerExecutionTarget.query}
   * followed by a separate tag-encoding walk. A target that omits it stays
   * correct: the server falls back to encoding {@link ServerExecutionTarget.query} rows itself.
   */
  queryForWire?(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]>
  /** Runs one write and returns the change count and last inserted row id. */
  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult>
  /** Runs a function inside one transaction. */
  transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T>
  /**
   * Optional entry point for a transaction whose statements are all known
   * before it starts, which lets concurrent transactions share one commit. A
   * target that omits it stays correct: the server falls back to
   * {@link ServerExecutionTarget.transaction} and runs the statements one at a time.
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
  /** Optional device-sync entry point that applies a batch of changes a device pushed. */
  applyChanges?(
    batch: ReplicationBatch,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult>
  /** Optional listing of the migrations this database has applied. */
  appliedMigrations?(): Promise<AppliedMigrationRow[]>
}

/**
 * Finds what the server should run a database's statements against.
 *
 * @public
 */
export type ServerExecutionTargetResolver = (
  databaseId: string,
) => ServerExecutionTarget | null | undefined | Promise<ServerExecutionTarget | null | undefined>

/** Options for the standalone HTTP + WS server.
 * @public
 */
export interface ServerOptions<Identity = unknown> {
  /** Address the server binds to. Default: '0.0.0.0'. */
  host?: string
  /** Port the server binds to. Default: 3000. */
  port?: number
  /** Cross-origin rules the server answers browser requests with. */
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
  /** How long, in milliseconds, a device's sync cursor is kept after its last contact. */
  deviceCursorRetentionMs?: number
  /** Changes a device may leave unacknowledged before the server stops sending more. */
  maxUnacknowledgedChanges?: number
  /** Runs before every database route and every WebSocket upgrade, and names the caller. */
  authenticate?: AuthenticateHook<Identity>
  /** Statements callers may invoke by name. Without these, only SQL routes serve reads and writes. */
  operations?: OperationRegistry<Identity>
  /** Opens the five statement routes and their WebSocket messages. Default: false. */
  acceptSql?: boolean
  /** Finds what the server runs a database's statements against. */
  resolveExecutionTarget?: ServerExecutionTargetResolver
  /** Supplies the replication figures the readiness endpoint reports. */
  getReplicationStatus?: () => ReplicationStatusInfo | null
  /** Supplies what `GET /db/{id}/cluster` reports for one database. */
  getClusterStatus?: (databaseId: string) => ClusterStatusInfo | null
  /** Decides whether a caller may read the addresses of every node in the group. */
  authorizeClusterStatus?: ClusterStatusAuthorizer
}

/** Replication figures one node reports through its readiness endpoint.
 * @public
 */
export interface ReplicationStatusInfo {
  /** Whether this node accepts writes or serves reads. */
  role: string
  /** Whether this node forwards writes to the primary. */
  writeForwarding: boolean
  /** Number of peers the node is connected to. */
  peers: number
  /** Highest change-log position this node has recorded locally. */
  localSeq: bigint
  /** What the node can do right now, and the condition behind it. */
  health: NodeHealth
  /** Identifier of the replication group the node belongs to. */
  replicationGroupId?: string
  /** The primary term the node believes is current. */
  primaryTerm?: bigint
  /** Identifier of the primary the node believes is current. */
  currentPrimary?: string
  /** Whether the node reaches its cluster coordinator, and whether it holds write authority. */
  coordinator?: {
    connected: boolean
    authority: boolean
  }
  /** Whether this node runs the group's controller loop. */
  controller?: {
    state: 'disabled' | 'standby' | 'active' | 'lost'
  }
  /** Identifiers of the replicas the group counts as in sync. */
  inSyncReplicas?: string[]
  /** Identifiers of the replicas that have fallen behind. */
  laggingReplicas?: string[]
  /** Where this node stands in first sync. */
  syncState?: string
}

/** CORS configuration.
 * @public
 */
export interface CorsOptions {
  /** Origins the server allows. */
  origin?: string | string[]
  /** Methods the server allows. */
  methods?: string[]
  /** Request headers the server allows. */
  headers?: string[]
}

/**
 * Options for the mountable WebSocket handler.
 *
 * @internal
 */
export interface WSHandlerOptions<Identity = unknown> {
  /** Maximum message size in bytes. Default: 1_048_576 (1 MB). */
  maxPayloadLength?: number
  /** Change-log retention for CDC subscriptions in milliseconds. Default: 3_600_000. */
  cdcRetentionMs?: number
  deviceCursorRetentionMs?: number
  maxUnacknowledgedChanges?: number
  acceptSql?: boolean
  operations?: OperationRegistry<Identity>
  resolveExecutionTarget?: ServerExecutionTargetResolver
}

/** Options for the client SDK.
 * @public
 */
export interface ClientOptions {
  /** Transport to use. Default: 'websocket'. */
  transport?: 'websocket' | 'http'
  /**
   * Custom headers for HTTP requests, and for the WebSocket upgrade in a
   * runtime whose WebSocket carries a handshake header, which Node and Bun do
   * and a browser does not. Constructing a WebSocket-transport client with
   * headers and no {@link ClientOptions.webSocketProtocols} in a runtime that
   * carries none fails with `INVALID_ARGUMENT`. Pass both when a browser client
   * needs each one, and the headers still reach every HTTP request.
   */
  headers?: Record<string, string>
  /**
   * WebSocket subprotocols offered during the handshake, which is how a browser
   * carries a short-lived credential. The client offers the `sirannon.v1`
   * identifier alongside them and the server selects that identifier, so the
   * credential never comes back in the handshake response.
   */
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
