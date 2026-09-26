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
   * Receives the error when the change callback, `onReset`, or `onSubscribed` throws or
   * rejects, and the error for a change frame that the client cannot decode. The subscription
   * does not await a callback, so this handler receives both a throw and a rejection.
   * Sirannon discards any error that this handler throws itself.
   */
  onError?: (error: Error) => void
  /**
   * Called when the server answers a subscribe with `resync: true` and skips the replay,
   * because the changes after the resume point are older than the history that it
   * retains, the resume cursor comes from another change-log epoch, or its replay fails.
   * The subscription continues from the server's current position, so treat any earlier
   * state as stale and read it again.
   */
  onReset?: () => void
  /** The identifier of the device that opens this subscription, so that the server can leave that device's own writes out of the stream. */
  deviceId?: string
  /** The tables that one subscription covers, so the client receives a transaction that spans them as one stream in ascending order. */
  tables?: readonly string[]
  /** The highest migration version in the device's local database, which the server checks before it accepts the subscription. */
  schemaVersion?: number
  /** Returns the sequence to resume from; the transport calls it again on each reconnect, so that the resume point matches the durable cursor. */
  getResumeSeq?: () => bigint | undefined
  /** The sequence after which the first subscribe resumes. When it is absent, the subscription starts from the server's current position. */
  sinceSeq?: bigint
  /** The change-log epoch of the resume cursor, so that the server answers with a resync when the cursor comes from another database file. */
  epoch?: string
  /**
   * Set to true when this device stages pulled changes durably and
   * acknowledges staged sequences, so that the server can pack several events
   * into each frame and pace the delivery window continuously. Set it only for
   * a server that announces the `sync.staged-stream` capability.
   */
  stagedStream?: boolean
  /** Receives the values that the server confirms on subscribe, which are the baseline sequence, the epoch, whether a resync is due, and the delivery window. */
  onSubscribed?: (info: {
    seq: bigint | undefined
    epoch: string | undefined
    resync: boolean
    maxUnacknowledgedChanges: number | undefined
  }) => void
}

/**
 * The callbacks through which a transport passes a live query's updates.
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
 * Returns the digest of the server's operation registry, so that the client can detect
 * a change to the registered operations that a live query uses.
 *
 * @public
 */
export type RegistryDigestSource = (refresh: boolean) => Promise<string | undefined>

/**
 * The connection through which a client sends requests to a sirannon-db server.
 * Each transport sends requests for one database.
 *
 * @public
 */
export interface Transport {
  /**
   * Whether the transport sends a read concern passed to {@link Transport.query} or
   * {@link Transport.queryNamed} to the server. Topology routing applies the
   * client-wide setting when it chooses a node, so it leaves this unset, and the
   * client throws for a per-read concern, so that it sends a read only at the
   * level that the caller asked for.
   */
  readonly carriesReadConcern?: boolean
  /** Sends a read and returns its rows. */
  query(sql: string, params?: Params, readConcern?: ReadConcern): Promise<QueryResponse>
  /** Sends one write. */
  execute(sql: string, params?: Params): Promise<ExecuteResponse>
  /** Sends several statements that the server executes in one transaction. */
  transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse>
  /** Sends one statement with many parameter sets, which the server executes in one transaction. */
  batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse>
  /** Sends a bulk load, which the server executes at relaxed durability. */
  load(sql: string, paramsBatch: Params[], durability?: BulkLoadDurability, checkpoint?: boolean): Promise<LoadResponse>
  /** Executes a registered read by name and returns its rows. */
  queryNamed(name: string, args?: Record<string, unknown>, readConcern?: ReadConcern): Promise<QueryResponse>
  /** Executes a registered write by name. */
  executeNamed(name: string, args?: Record<string, unknown>, writeConcern?: WriteConcern): Promise<TransactionResponse>
  /** Opens a live query on a registered read and passes its updates to the handlers. */
  liveSubscribe(
    name: string,
    args: Record<string, unknown> | undefined,
    handlers: LiveHandlers,
    registryDigest?: RegistryDigestSource,
  ): Promise<RemoteSubscription>
  /** Opens a change subscription on a table. */
  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription>
  /** Closes the transport and ends every subscription on it. */
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
   * The subscription reports changes to the set of matching rows. When an update moves
   * a row into the set, your callback receives an insert with no `oldRow`, and when an
   * update moves a row out, it receives a delete with the old row in `oldRow` and an
   * empty `row`. It receives an update that keeps the row in the set as an ordinary
   * update, and nothing for an update outside the set. These insert and delete events look the
   * same as real ones, so read `type` as the row entering or leaving the set.
   *
   * The subscriber sets this filter, so the subscriber controls which changes the server sends. An
   * operator who needs to limit which rows a caller can subscribe to checks the table
   * and the filter in an `onBeforeSubscribe` hook.
   */
  filter(conditions: Record<string, unknown>): RemoteSubscriptionBuilder
  /**
   * Starts the subscription and calls your callback with each change.
   *
   * The subscription does not await your callback, so two calls to an asynchronous
   * callback can overlap. When each change has to finish before the next one starts,
   * chain the work onto one promise. `options.onError` receives both a throw and a
   * rejection from the callback.
   *
   * @typeParam T - The shape of this table's rows, which sets the type of `row` and `oldRow`.
   * @param callback - Receives each change that matches this subscription.
   * @param options - The `onError` handler and the device-sync fields.
   * @returns A handle whose `unsubscribe` ends the subscription.
   */
  subscribe<T = Record<string, unknown>>(
    callback: (event: ChangeEvent<T>) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription>
}

/**
 * An error from a sirannon-db server, or from the client when it cannot complete a request.
 * Its `code` holds the machine-readable code from the server's error response, or a code
 * that the client sets, such as `TIMEOUT` or `CONNECTION_ERROR`.
 *
 * @public
 */
export class RemoteError extends Error {
  /** The machine-readable error code, from the server's error response or from the client. */
  readonly code: string

  constructor(code: string, message: string) {
    super(message)
    this.name = 'RemoteError'
    this.code = code
  }
}
