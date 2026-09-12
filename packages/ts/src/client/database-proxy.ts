import type { LiveQuery, LiveQueryOptions } from '../core/live/types.js'
import type { OperationArguments, OperationRef } from '../core/operation-registry.js'
import type {
  BulkLoadDurability,
  BulkLoadResult,
  Params,
  QueryOptions,
  ReadConcern,
  WriteConcern,
} from '../core/types.js'
import type { ExecuteResponse } from '../server/protocol.js'
import { RemoteLiveQuery } from './remote-live-query.js'
import type { ServerCapabilityCheck } from './server-capabilities.js'
import { RemoteSubscriptionBuilderImpl } from './subscription.js'
import type { RemoteSubscriptionBuilder, Transport } from './types.js'
import { RemoteError } from './types.js'

const DEFAULT_LOAD_BATCH_SIZE = 1000

export const READ_CONCERN_UNSUPPORTED_MESSAGE =
  'This transport does not carry a per-read concern to the server. Use the HTTP transport for a per-call readConcern, or set the client-wide readConcern that topology routing applies when choosing a node.'

/** Options for {@link RemoteDatabase.loadAll}.
 * @public
 */
export interface LoadAllOptions {
  /**
   * Rows per batch sent to the server. Each batch is one request, so it must
   * fit under the server's `maxBodyBytes`; widen that cap or lower this for
   * wide rows. Default: 1000.
   */
  batchSize?: number
  /** Durability during the load. Default: 'off'. */
  durability?: BulkLoadDurability
}

function isAsyncIterable<T>(value: Iterable<T> | AsyncIterable<T>): value is AsyncIterable<T> {
  return typeof (value as AsyncIterable<T>)[Symbol.asyncIterator] === 'function'
}

/**
 * Proxy for a remote sirannon-db database. Mirrors the core
 * `Database` query interface with async methods that send
 * requests to the server via the configured transport.
 *
 * @public
 */
export class RemoteDatabase {
  constructor(
    /**
     * Identifier of the database on the server.
     */
    readonly id: string,
    private readonly transport: Transport,
    private readonly capabilities: ServerCapabilityCheck,
    private readonly onDispose?: () => void,
  ) {}

  /**
   * Sends a read to the server and returns its rows.
   *
   * @param sql - The statement to run. The server refuses it unless it accepts SQL.
   * @param params - Values bound to the statement, named or positional.
   * @param options - Read concern for this statement.
   * @returns The rows the server returned.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]>
  /**
   * Runs a registered read by name and returns its rows.
   *
   * @param operation - Reference to the registered read, which carries its argument and row types.
   * @param args - Arguments the read takes.
   * @param options - Read concern for this statement.
   * @returns The rows the server returned.
   */
  async query<Args, Row>(operation: OperationRef<Args, Row>, args: Args, options?: QueryOptions): Promise<Row[]>
  async query(
    operation: string | OperationRef<unknown, unknown>,
    params?: Params,
    options?: QueryOptions,
  ): Promise<unknown[]> {
    this.assertReadConcernReaches(options?.readConcern)

    if (typeof operation !== 'string') {
      const named = await this.transport.queryNamed(
        operation.name,
        params as OperationArguments | undefined,
        options?.readConcern,
      )
      return named.rows
    }
    await this.capabilities.assertSqlAccepted()
    const response = await this.transport.query(operation, params, options?.readConcern)
    return response.rows
  }

  private assertReadConcernReaches(readConcern: ReadConcern | undefined): void {
    if (readConcern === undefined || this.transport.carriesReadConcern === true) return
    throw new RemoteError('INVALID_ARGUMENT', READ_CONCERN_UNSUPPORTED_MESSAGE)
  }

  /**
   * Sends one write to the server.
   *
   * @param sql - The statement to run. The server refuses it unless it accepts SQL.
   * @param params - Values bound to the statement, named or positional.
   * @returns How many rows changed, and the last inserted row id.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResponse>
  /**
   * Runs a registered write by name.
   *
   * @param operation - Reference to the registered write, which carries its argument type.
   * @param args - Arguments the write takes.
   * @param writeConcern - Acknowledgements the write waits for.
   * @returns One result per statement the operation ran.
   */
  async execute<Args>(
    operation: OperationRef<Args, unknown>,
    args: Args,
    writeConcern?: WriteConcern,
  ): Promise<ExecuteResponse[]>
  async execute(
    operation: string | OperationRef<unknown, unknown>,
    params?: Params,
    writeConcern?: WriteConcern,
  ): Promise<ExecuteResponse | ExecuteResponse[]> {
    if (typeof operation !== 'string') {
      const named = await this.transport.executeNamed(
        operation.name,
        params as OperationArguments | undefined,
        writeConcern,
      )
      return named.results
    }
    await this.capabilities.assertSqlAccepted()
    return this.transport.execute(operation, params)
  }

  /**
   * Opens a live query on a registered read, which keeps its rows current as the tables behind it change.
   *
   * @param name - Name of the registered read.
   * @param args - Arguments the read takes.
   * @param options - Carries `onError`, which receives a failure of any listener on this query.
   * @returns The live query, already subscribed.
   */
  async live<T = Record<string, unknown>>(
    name: string,
    args?: OperationArguments,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<T>>
  /**
   * Opens a live query on a registered read, which keeps its rows current as the tables behind it change.
   *
   * @param operation - Reference to the registered read, which carries its argument and row types.
   * @param args - Arguments the read takes.
   * @param options - Carries `onError`, which receives a failure of any listener on this query.
   * @returns The live query, already subscribed.
   */
  async live<Args, Row>(
    operation: OperationRef<Args, Row>,
    args: Args,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<Row>>
  async live(
    operation: string | OperationRef<unknown, unknown>,
    args?: OperationArguments,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<Record<string, unknown>>> {
    const name = typeof operation === 'string' ? operation : operation.name
    return RemoteLiveQuery.open(
      handlers =>
        this.transport.liveSubscribe(name, args, handlers, refresh => this.capabilities.registryDigest(refresh)),
      options?.onError,
    )
  }

  /**
   * Execute multiple statements as a single atomic transaction.
   * Returns an array of results, one per statement.
   *
   * The whole list is sent in one request and commits or rolls back as a
   * unit, so the client is never in the loop between statements.
   */
  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<ExecuteResponse[]> {
    await this.capabilities.assertSqlAccepted()
    const response = await this.transport.transaction(statements)
    return response.results
  }

  /**
   * Run the same statement once per parameter set as a single atomic
   * transaction that commits with one fsync. Returns one result per
   * parameter set, in order. Use this for a burst of same-shape writes
   * (an import, a bulk insert) that must all commit or all roll back.
   */
  async batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<ExecuteResponse[]> {
    await this.capabilities.assertSqlAccepted()
    const response = await this.transport.batch(sql, paramsBatch, writeConcern)
    return response.results
  }

  /**
   * Load a whole dataset through the same statement, batching it into requests
   * for you and paying the one fsyncing WAL checkpoint once, after the final
   * batch. The configured durability is restored after every batch, so an
   * import that stops partway never leaves the writer at the relaxed level.
   * Prefer this over {@link RemoteDatabase.load} for anything larger than a single request:
   * it finalises the load itself, so there is no checkpoint flag to forget.
   *
   * Accepts a synchronous or asynchronous iterable of parameter sets so that rows
   * can stream from a file or the network without being held in memory at
   * once. Returns the total rows loaded and changes applied.
   *
   * ```ts
   * const summary = await db.loadAll(
   *   'INSERT INTO events (id, payload) VALUES (?, ?)',
   *   rowStream,
   *   { batchSize: 5000, durability: 'off' },
   * )
   * ```
   */
  async loadAll(
    sql: string,
    rows: Iterable<Params> | AsyncIterable<Params>,
    options?: LoadAllOptions,
  ): Promise<BulkLoadResult> {
    const batchSize = options?.batchSize ?? DEFAULT_LOAD_BATCH_SIZE
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new RemoteError('INVALID_ARGUMENT', 'loadAll batchSize must be a positive integer')
    }
    await this.capabilities.assertSqlAccepted()
    const durability = options?.durability
    const total: BulkLoadResult = { rowsLoaded: 0, changes: 0 }

    const send = async (paramsBatch: Params[], checkpoint: boolean): Promise<void> => {
      const summary = await this.transport.load(sql, paramsBatch, durability, checkpoint)
      total.rowsLoaded += summary.rowsLoaded
      total.changes += summary.changes
    }

    let batch: Params[] = []
    let previous: Params[] | null = null
    const rotate = async (): Promise<void> => {
      if (previous !== null) await send(previous, false)
      previous = batch
      batch = []
    }

    if (isAsyncIterable(rows)) {
      for await (const row of rows) {
        batch.push(row)
        if (batch.length >= batchSize) await rotate()
      }
    } else {
      for (const row of rows) {
        batch.push(row)
        if (batch.length >= batchSize) await rotate()
      }
    }

    if (batch.length > 0) {
      if (previous !== null) await send(previous, false)
      await send(batch, true)
    } else if (previous !== null) {
      await send(previous, true)
    }

    return total
  }

  /**
   * Load one batch of rows through the same statement with writer durability
   * relaxed for the duration, then restored before this resolves. This is the
   * low-level primitive; prefer {@link RemoteDatabase.loadAll} for a dataset that spans more
   * than one request, since it finalises the load itself rather than relying on
   * a `checkpoint` flag.
   *
   * Returns the total rows loaded and changes applied. When splitting a dataset
   * across many `load` calls by hand, pass `checkpoint: false` on every call
   * but the last so the one fsyncing WAL checkpoint runs once at the end; the
   * configured durability is restored after each call regardless.
   */
  async load(
    sql: string,
    paramsBatch: Params[],
    durability?: BulkLoadDurability,
    checkpoint?: boolean,
  ): Promise<BulkLoadResult> {
    await this.capabilities.assertSqlAccepted()
    return this.transport.load(sql, paramsBatch, durability, checkpoint)
  }

  /**
   * Start building a CDC subscription for the given table.
   * Chain `.filter()` to narrow the events, then call `.subscribe()`
   * with a callback to begin receiving real-time change events.
   *
   * ```ts
   * const sub = await db
   *   .on('orders')
   *   .filter({ status: 'pending' })
   *   .subscribe(event => console.log(event))
   *
   * // Later:
   * sub.unsubscribe()
   * ```
   *
   * Begins a change subscription on a watched table.
   *
   * @param table - Name of the watched table.
   * @returns A builder you narrow with a filter and then subscribe to.
   */
  on(table: string): RemoteSubscriptionBuilder {
    return new RemoteSubscriptionBuilderImpl(table, this.transport)
  }

  /**
   * Close the transport for this database. After calling `close()`,
   * all pending requests are rejected and new calls will throw.
   */
  close(): void {
    this.transport.close()
    this.onDispose?.()
  }
}
