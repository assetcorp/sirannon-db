import type { ResultOp } from '../core/live/types.js'
import type { BulkLoadDurability, ChangeEvent, Params, ReadConcern, WriteConcern } from '../core/types.js'
import type {
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from '../server/protocol.js'

/** Optional behaviours for a CDC subscription. */
export interface SubscribeOptions {
  /**
   * Invoked when a reconnect cannot replay missed changes because they fell
   * outside the server's retained history. The subscription continues live
   * from the current moment; treat any prior state as stale and re-read.
   */
  onReset?: () => void
  deviceId?: string
  tables?: readonly string[]
  schemaVersion?: number
  getResumeSeq?: () => bigint | undefined
  sinceSeq?: bigint
  epoch?: string
  /**
   * Declares that this device stages pulled changes durably and
   * acknowledges staged sequences so that the server may pack several events
   * per frame and pace the delivery window continuously. Send only to a
   * server that announces the `sync.staged-stream` capability.
   */
  stagedStream?: boolean
  onSubscribed?: (info: {
    seq: bigint | undefined
    epoch: string | undefined
    resync: boolean
    maxUnacknowledgedChanges: number | undefined
  }) => void
}

/**
 * Callbacks a transport delivers a live query's updates through.
 *
 * @public
 */
export interface LiveHandlers<T = Record<string, unknown>> {
  /** Receives a complete replacement result set. */
  onRows(rows: T[]): void
  /** Receives the individual edits that move the result set to its new state. */
  onOps(ops: ResultOp<T>[]): void
  /** Called when the server starts re-reading the query. */
  onRevalidating(): void
  /** Called when the query fails. */
  onError(error: RemoteError): void
}

/**
 * Reads the digest of the server's operation registry so that a client notices when
 * the registered operations behind a live query change.
 *
 * @public
 */
export type RegistryDigestSource = (refresh: boolean) => Promise<string | undefined>

/**
 * Transport layer for communicating with a sirannon-db server.
 * Each transport instance is bound to a specific database.
 *
 * @public
 */
export interface Transport {
  /**
   * Whether a read concern passed to {@link Transport.query} or
   * {@link Transport.queryNamed} reaches the server. Topology routing applies
   * the client-wide setting when it chooses a node, so it leaves this unset and
   * a caller asking for a per-read concern is refused rather than served a read
   * at another level.
   */
  readonly carriesReadConcern?: boolean
  /** Sends a read and returns its rows. */
  query(sql: string, params?: Params, readConcern?: ReadConcern): Promise<QueryResponse>
  /** Sends one write. */
  execute(sql: string, params?: Params): Promise<ExecuteResponse>
  /** Sends several statements the server runs in one transaction. */
  transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse>
  /** Sends one statement over many parameter sets, which the server runs in one transaction. */
  batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse>
  /** Sends a bulk load, which the server runs at relaxed durability. */
  load(sql: string, paramsBatch: Params[], durability?: BulkLoadDurability, checkpoint?: boolean): Promise<LoadResponse>
  /** Runs a registered read by name and returns its rows. */
  queryNamed(name: string, args?: Record<string, unknown>, readConcern?: ReadConcern): Promise<QueryResponse>
  /** Runs a registered write by name. */
  executeNamed(name: string, args?: Record<string, unknown>, writeConcern?: WriteConcern): Promise<TransactionResponse>
  /** Opens a live query on a registered read and delivers its updates to the handlers. */
  liveSubscribe(
    name: string,
    args: Record<string, unknown> | undefined,
    handlers: LiveHandlers,
    registryDigest?: RegistryDigestSource,
  ): Promise<RemoteSubscription>
  /** Opens a change subscription on a watched table. */
  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription>
  /** Closes the transport and every subscription running on it. */
  close(): void
}

/** Handle for an active remote subscription.
 * @public
 */
export interface RemoteSubscription {
  /** Ends the subscription, so the callback receives no further events. */
  unsubscribe(): void
}

/** Builder for creating remote CDC subscriptions with optional filters.
 * @public
 */
export interface RemoteSubscriptionBuilder {
  /** Narrows the subscription to rows whose columns equal the given values. */
  filter(conditions: Record<string, unknown>): RemoteSubscriptionBuilder
  /** Starts the subscription and calls back on each change. */
  subscribe(callback: (event: ChangeEvent) => void, options?: SubscribeOptions): Promise<RemoteSubscription>
}

/**
 * Error originating from a remote sirannon-db server.
 * Carries the machine-readable error code from the server's error response.
 *
 * @public
 */
export class RemoteError extends Error {
  /** Machine-readable code the server sent with the error. */
  readonly code: string

  constructor(code: string, message: string) {
    super(message)
    this.name = 'RemoteError'
    this.code = code
  }
}
