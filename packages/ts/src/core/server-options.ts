import type { OperationRegistry } from './operation-registry.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
import type { AppliedMigrationRow } from './system-catalog/index.js'
import type { Transaction } from './transaction.js'
import type { ClusterStatusInfo, ExecuteResult, NodeHealth, Params, QueryOptions } from './types.js'

/** The request details that the server passes to the authenticate hook.
 * @public
 */
export interface RequestContext {
  /** The request headers, with every name in lower case. */
  headers: Record<string, string>
  /** The HTTP method of the request, or of the WebSocket upgrade request. */
  method: string
  /** The URL path of the request. */
  path: string
  /** The identifier of the database that the route names. */
  databaseId?: string
  /** The network address of the client that sent the request. */
  remoteAddress: string
}

/**
 * Identifies the caller behind a request. Return the identity, which the
 * server passes to the other hooks and uses to fill the `fromIdentity`
 * arguments of registered operations, or throw a {@link RequestDeniedError} to
 * reject the request with a status of your own.
 *
 * @public
 */
export type AuthenticateHook<Identity = unknown> = (
  ctx: RequestContext,
) => Identity | undefined | Promise<Identity | undefined>

/** Returns whether a caller may read the addresses of every node in the group.
 * @public
 */
export type ClusterStatusAuthorizer = (ctx: RequestContext) => boolean | Promise<boolean>

/**
 * The synchronous level during a bulk load. At 'off', SQLite documents that a
 * power loss or an operating-system crash can corrupt the database, so 'off'
 * suits only a load that the operator can re-run from scratch. At 'normal', a
 * WAL-mode database stays safe from corruption, which suits a load into a
 * database whose existing data the operator must keep.
 *
 * @public
 */
export type BulkLoadDurability = 'off' | 'normal'

/** The settings for one bulk load.
 * @public
 */
export interface BulkLoadOptions {
  /** The synchronous level during the load; the default is 'off'. */
  durability?: BulkLoadDurability
  /**
   * Whether this load ends with a WAL checkpoint; the default is true. Set it
   * to false on every load except the last of a multi-batch import, so that the
   * import pays for one fsyncing checkpoint at the end. Sirannon still restores
   * the configured level after each load, so an abandoned import leaves the
   * writer at that level.
   */
  checkpoint?: boolean
}

/** The totals for a bulk load, which Sirannon sums across rows so that a large load keeps no per-row results in memory.
 * @public
 */
export interface BulkLoadResult {
  /** The number of parameter sets that the load applied. */
  rowsLoaded: number
  /** The number of rows that the load inserted, updated, or deleted. */
  changes: number
}

/**
 * The object that the server executes one database's statements against. A
 * local `Database` fits this interface, and so does a proxy that forwards
 * statements to another node.
 *
 * @public
 */
export interface ServerExecutionTarget {
  /** Executes a read and returns the rows. */
  query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]>
  /**
   * Executes a read and returns rows already encoded for the wire, with
   * safe-range integers as plain numbers and larger integers and BLOBs as
   * tagged envelopes. When the target has this method, the server calls it and
   * encodes nothing itself; otherwise the server calls
   * {@link ServerExecutionTarget.query} and encodes the rows in a second pass.
   */
  queryForWire?(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]>
  /** Executes one write and returns the change count and the row id of the last inserted row. */
  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult>
  /** Calls a function inside one transaction. */
  transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T>
  /**
   * Executes a transaction whose statements are all known before it starts,
   * which lets concurrent transactions share one commit. When the target has no
   * such method, the server calls {@link ServerExecutionTarget.transaction} and
   * executes the statements one at a time.
   */
  executeTransaction?(
    statements: readonly { sql: string; params?: Params }[],
    options?: QueryOptions,
  ): Promise<ExecuteResult[]>
  /**
   * Executes a bulk load. A target that proxies to a remote primary can omit
   * this method, and the server then rejects bulk-load requests for that target
   * with `BULK_LOAD_UNSUPPORTED`.
   */
  bulkLoad?(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult>
  /** Applies a batch of changes that a device pushed; when the target has no such method, the server rejects device pushes with `SYNC_UNSUPPORTED`. */
  applyChanges?(
    batch: ReplicationBatch,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult>
  /** Lists the migrations that this database has applied. */
  appliedMigrations?(): Promise<AppliedMigrationRow[]>
}

/**
 * Returns the target that the server executes a database's statements
 * against, or null or undefined, to which the server responds with
 * `DATABASE_NOT_FOUND`.
 *
 * @public
 */
export type ServerExecutionTargetResolver = (
  databaseId: string,
) => ServerExecutionTarget | null | undefined | Promise<ServerExecutionTarget | null | undefined>

/** The options for the standalone HTTP and WebSocket server.
 * @public
 */
export interface ServerOptions<Identity = unknown> {
  /** The address that the server binds to; the default is '127.0.0.1'. */
  host?: string
  /** The port that the server binds to; the default is 9876. */
  port?: number
  /** The cross-origin rules that the server applies to browser requests. */
  cors?: boolean | CorsOptions
  /**
   * The largest HTTP request body and WebSocket message, in bytes, which the
   * server applies to both transports. The value must be a positive integer no
   * larger than 4_294_967_295, the largest limit that uWebSockets.js stores in
   * its unsigned 32-bit field; the server throws `INVALID_MAX_BODY_BYTES` for
   * any other value. The default is 1_048_576 (1 MB), which caps the
   * memory that the server spends on one request.
   */
  maxBodyBytes?: number
  /**
   * The most bytes that the server buffers for one WebSocket connection before
   * it closes the connection, so that the client learns of the overflow and
   * reconnects. The value must lie between `maxBodyBytes`, so that one frame
   * fits, and 4_294_967_295; the server throws `INVALID_WS_BACKPRESSURE` for
   * any other value. The default is the larger of 16 MB and `maxBodyBytes`.
   */
  maxWebSocketBackpressureBytes?: number
  /**
   * How long, in milliseconds, Sirannon keeps change events for WebSocket change
   * subscriptions. The retention limits both the size of the change log on disk
   * and how far back a reconnecting subscriber can resume. The default is
   * 3_600_000, one hour.
   */
  cdcRetentionMs?: number
  /**
   * How long, in milliseconds, Sirannon keeps the changes that a device's sync
   * cursor still needs before it drops the cursor; a database opened with its
   * own `deviceCursorRetention` uses that value. The default is 2_592_000_000,
   * 30 days.
   */
  deviceCursorRetentionMs?: number
  /**
   * The most changes that Sirannon keeps for one device's cursor before it
   * drops the cursor; a database opened with its own `maxChangesHeldForDevice`
   * uses that value. 0 sets no limit, and the default is 0.
   */
  maxChangesHeldForDevice?: number
  /** The number of changes that a device may leave unacknowledged before the server pauses sending more; the default is 1_000. */
  maxUnacknowledgedChanges?: number
  /** The server calls this before every database route and every WebSocket upgrade, to identify the caller. */
  authenticate?: AuthenticateHook<Identity>
  /** The statements that callers may invoke by name; with none registered, only the SQL routes serve reads and writes. */
  operations?: OperationRegistry<Identity>
  /** Enables the five statement routes and their WebSocket messages, and WebSocket subscriptions to a table's changes on a registry without an `onBeforeSubscribe` hook; the default is false. */
  acceptSql?: boolean
  /**
   * Enables the route that rebuilds a database from its backups; the default is
   * false. A restore replaces a database that is serving traffic, so the server
   * keeps the route closed until you enable it here, and the server constructor
   * throws `INVALID_BACKUP_RESTORE` when you enable it without an `authenticate`
   * hook.
   */
  acceptBackupRestore?: boolean
  /** Enables device sync, which covers the push, snapshot, and migration-list routes, device subscriptions, and acknowledgements; the default is false. The server constructor throws `INVALID_DEVICE_SYNC` when you enable it without an `authenticate` hook. */
  acceptDeviceSync?: boolean
  /** Returns the target that the server executes a database's statements against; without it, the server uses the databases in its own registry. */
  resolveExecutionTarget?: ServerExecutionTargetResolver
  /** Returns the replication figures that the readiness endpoint reports. */
  getReplicationStatus?: () => ReplicationStatusInfo | null
  /** Returns the status that `GET /db/{id}/cluster` reports for one database. */
  getClusterStatus?: (databaseId: string) => ClusterStatusInfo | null
  /** Returns whether a caller may read the addresses of every node in the group. */
  authorizeClusterStatus?: ClusterStatusAuthorizer
}

/** The replication figures that one node reports through its readiness endpoint.
 * @public
 */
export interface ReplicationStatusInfo {
  /** The node's role in the replication group, which sets whether it accepts writes or serves reads. */
  role: string
  /** Whether this node forwards writes to the primary. */
  writeForwarding: boolean
  /** The number of peers that the node is connected to. */
  peers: number
  /** The highest change-log position that this node has recorded locally. */
  localSeq: bigint
  /** What the node can do now, and the reason for that state. */
  health: NodeHealth
  /** The identifier of the node's replication group. */
  replicationGroupId?: string
  /** The primary term that this node reports as current. */
  primaryTerm?: bigint
  /** The identifier of the primary that this node reports as current. */
  currentPrimary?: string
  /** Whether the node is connected to its cluster coordinator, and whether it holds write authority. */
  coordinator?: {
    connected: boolean
    authority: boolean
  }
  /** The state of this node's controller loop for the group. */
  controller?: {
    state: 'disabled' | 'standby' | 'active' | 'lost'
  }
  /** The identifiers of the replicas that the group counts as in sync. */
  inSyncReplicas?: string[]
  /** The identifiers of the replicas that are behind the primary. */
  laggingReplicas?: string[]
  /** The node's progress through its first sync. */
  syncState?: string
}

/** The cross-origin (CORS) settings for the server.
 * @public
 */
export interface CorsOptions {
  /** The origins that the server allows. */
  origin?: string | string[]
  /** The methods that the server allows. */
  methods?: string[]
  /** The request headers that the server allows. */
  headers?: string[]
}

/**
 * The options for the mountable WebSocket handler.
 *
 * @internal
 */
export interface WSHandlerOptions<Identity = unknown> {
  /** The largest message, in bytes; the default is 1_048_576 (1 MB). */
  maxPayloadLength?: number
  /** The outbound bytes that a socket may buffer before the server pauses a device stream; the default is 16 MB. */
  maxBackpressureBytes?: number
  /** The change-log retention for change subscriptions, in milliseconds; the default is 3_600_000. */
  cdcRetentionMs?: number
  deviceCursorRetentionMs?: number
  maxChangesHeldForDevice?: number
  maxUnacknowledgedChanges?: number
  acceptSql?: boolean
  acceptDeviceSync?: boolean
  operations?: OperationRegistry<Identity>
  resolveExecutionTarget?: ServerExecutionTargetResolver
}

/** The options for the client SDK.
 * @public
 */
export interface ClientOptions {
  /** The transport to use; the default is 'websocket'. */
  transport?: 'websocket' | 'http'
  /**
   * Custom headers for HTTP requests, and for the WebSocket upgrade in a
   * runtime whose WebSocket client can send handshake headers, as Node and Bun
   * can and a browser cannot. In a runtime without that ability, constructing a
   * WebSocket-transport client with headers and no
   * {@link ClientOptions.webSocketProtocols} fails with `INVALID_ARGUMENT`. A
   * browser client can pass both, and the client still sends the headers on
   * every HTTP request.
   */
  headers?: Record<string, string>
  /**
   * The WebSocket subprotocols that the client offers during the handshake,
   * which let a browser send a short-lived credential. The client also offers
   * the `sirannon.v1` identifier, and the server selects that identifier, so the
   * handshake response names `sirannon.v1` and leaves the credential out.
   */
  webSocketProtocols?: string | string[]
  /** Whether the client reconnects when the WebSocket disconnects; the default is true. */
  autoReconnect?: boolean
  /** The delay before each reconnection attempt, in milliseconds; the default is 1000. */
  reconnectInterval?: number
  /**
   * The timeout for each request on the WebSocket transport, in milliseconds;
   * the default is 30000, and 0 waits with no limit. Raise it for a bulk load or
   * a batch large enough to take longer than the timeout.
   */
  requestTimeout?: number
}
