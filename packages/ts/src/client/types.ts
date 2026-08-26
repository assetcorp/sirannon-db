import type { ResultOp } from '../core/live/types.js'
import type { BulkLoadDurability, ChangeEvent, Params, ReadConcern, WriteConcern } from '../core/types.js'
import type {
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from '../server/protocol.js'

/** Optional behaviours for a CDC subscription.
 * @public
 */
export interface SubscribeOptions {
  /**
   * Receives the failure of a change callback, and the error raised by a change frame the
   * client cannot decode. The subscription never waits for what the callback returns, so a
   * throw and a rejection both arrive here. Sirannon drops whatever this reporter itself throws.
   */
  onError?: (error: Error) => void
  /**
   * Invoked when a reconnect cannot replay missed changes because they fell
   * outside the server's retained history. The subscription continues live
   * from the current moment; treat any prior state as stale and re-read.
   */
  onReset?: () => void
  /** Identity of the device this subscription belongs to, which the server uses to withhold that device's own writes. */
  deviceId?: string
  /** Tables one subscription covers, so a transaction spanning them arrives as one ascending stream. */
  tables?: readonly string[]
  /** Highest migration version this device has applied, which the server gates the subscription on. */
  schemaVersion?: number
  /** Returns the sequence to resume from, read afresh on each reconnect so a durable cursor stays current. */
  getResumeSeq?: () => bigint | undefined
  /** Sequence to resume after on the first subscribe. Where it is absent, the subscription starts live from now. */
  sinceSeq?: bigint
  /** Change-log epoch the resume cursor belongs to, so a cursor from another database file forces a resync. */
  epoch?: string
  /**
   * Declares that this device stages pulled changes durably and
   * acknowledges staged sequences so that the server may pack several events
   * per frame and pace the delivery window continuously. Send only to a
   * server that announces the `sync.staged-stream` capability.
   */
  stagedStream?: boolean
  /** Receives what the server confirmed on subscribe: the baseline sequence, the epoch, whether a resync is due, and the delivery window. */
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
  /**
   * Narrows the subscription to rows whose columns equal the given values.
   *
   * The filter reports membership of the matching set, so an update that moves a row
   * into the set arrives as an insert carrying no `oldRow`, and one that moves a row
   * out arrives as a delete carrying the old row and an empty `row`. An update that
   * leaves the row in the set arrives unchanged, and one that never touches the set
   * is not delivered. A synthesised event is indistinguishable from a real insert or
   * delete, so read `type` as the row's arrival or departure from the filter.
   *
   * The subscriber chooses this filter, so it decides how much the server delivers,
   * and an operator who needs to bound what a caller may read does that in the
   * `authenticate` hook.
   */
  filter(conditions: Record<string, unknown>): RemoteSubscriptionBuilder
  /**
   * Starts the subscription and calls back on each change.
   *
   * The subscription never waits for what your callback returns, so two calls to an
   * asynchronous callback can overlap. Chain the work onto one promise where each change
   * has to finish before the next one starts. A throw, and a rejection of what the callback
   * returns, both reach `options.onError`.
   *
   * @typeParam T - Shape of the rows this table holds, which types `row` and `oldRow`.
   * @param callback - Receives each change this subscription matches.
   * @param options - Carries `onError` and the device-sync fields.
   * @returns A handle whose `unsubscribe` ends the subscription.
   */
  subscribe<T = Record<string, unknown>>(
    callback: (event: ChangeEvent<T>) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription>
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
