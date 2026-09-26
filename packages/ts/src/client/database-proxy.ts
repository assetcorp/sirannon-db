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
   * The number of rows in each batch that the client sends to the server. Each
   * batch is one request that must fit under the server's `maxBodyBytes`, so for
   * wide rows, raise that limit or lower this value. Defaults to 1000.
   */
  batchSize?: number
  /** The writer's durability level during the load. Defaults to 'off'. */
  durability?: BulkLoadDurability
}

function isAsyncIterable<T>(value: Iterable<T> | AsyncIterable<T>): value is AsyncIterable<T> {
  return typeof (value as AsyncIterable<T>)[Symbol.asyncIterator] === 'function'
}

/**
 * A handle to one database on a sirannon-db server. Its async methods match
 * the core `Database` query methods and send each request through the
 * configured transport.
 *
 * @public
 */
export class RemoteDatabase {
  constructor(
    /**
     * The identifier of the database on the server.
     */
    readonly id: string,
    private readonly transport: Transport,
    private readonly capabilities: ServerCapabilityCheck,
    private readonly onDispose?: () => void,
  ) {}

  /**
   * Sends a read to the server and returns its rows.
   *
   * @param sql - The statement to execute, which the client sends only to a server that accepts SQL.
   * @param params - The values to bind to the statement, by name or by position.
   * @param options - The read concern for this statement.
   * @returns The rows that the server returns.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]>
  /**
   * Executes a registered read by name and returns its rows.
   *
   * @param operation - A reference to the registered read, typed with its arguments and rows.
   * @param args - The arguments that the read takes.
   * @param options - The read concern for this statement.
   * @returns The rows that the server returns.
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
   * @param sql - The statement to execute, which the client sends only to a server that accepts SQL.
   * @param params - The values to bind to the statement, by name or by position.
   * @returns How many rows changed, and the last inserted row id.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResponse>
  /**
   * Executes a registered write by name.
   *
   * @param operation - A reference to the registered write, typed with its arguments.
   * @param args - The arguments that the write takes.
   * @param writeConcern - The acknowledgements that the server waits for before it confirms the write.
   * @returns One result for each statement that the operation executes.
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
   * Opens a live query on a registered read, and keeps its rows current as rows change in the tables that the read selects from.
   *
   * @param name - The name of the registered read.
   * @param args - The arguments that the read takes.
   * @param options - Options with `onError`, which receives the error when any listener on this query fails.
   * @returns The live query, which is already subscribed.
   */
  async live<T = Record<string, unknown>>(
    name: string,
    args?: OperationArguments,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<T>>
  /**
   * Opens a live query on a registered read, and keeps its rows current as rows change in the tables that the read selects from.
   *
   * @param operation - A reference to the registered read, typed with its arguments and rows.
   * @param args - The arguments that the read takes.
   * @param options - Options with `onError`, which receives the error when any listener on this query fails.
   * @returns The live query, which is already subscribed.
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
   * Executes several statements as one atomic transaction and returns one result
   * per statement.
   *
   * The client sends the whole list in one request, and the server commits or
   * rolls it back as a unit, with no round trip between statements.
   */
  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<ExecuteResponse[]> {
    await this.capabilities.assertSqlAccepted()
    const response = await this.transport.transaction(statements)
    return response.results
  }

  /**
   * Executes the same statement once for each parameter set, in one atomic
   * transaction that commits with one fsync, and returns one result per
   * parameter set, in order. Use it for a burst of writes with the same shape,
   * such as an import or a bulk insert, that must all commit or all roll back.
   */
  async batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<ExecuteResponse[]> {
    await this.capabilities.assertSqlAccepted()
    const response = await this.transport.batch(sql, paramsBatch, writeConcern)
    return response.results
  }

  /**
   * Loads a whole dataset through one statement in requests of `batchSize` rows,
   * and asks the server to perform the fsyncing WAL checkpoint once, after the final
   * batch. The server restores the configured durability after every batch, so an
   * import that stops partway leaves the writer at its configured level. Use this
   * in place of {@link RemoteDatabase.load} for anything larger than one request,
   * because it sets the `checkpoint` flag itself.
   *
   * It accepts a synchronous or asynchronous iterable of parameter sets, so rows can
   * stream from a file or the network while the client holds at most two batches in
   * memory. It returns the total number of rows loaded and changes applied.
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
   * Loads one batch of rows through one statement, while the server relaxes the
   * writer's durability for this call and restores it before the call resolves.
   * For a dataset that spans more than one request, use {@link RemoteDatabase.loadAll},
   * which sets the `checkpoint` flag itself.
   *
   * It returns the total number of rows loaded and changes applied. When you split
   * a dataset across many `load` calls yourself, pass `checkpoint: false` on every
   * call except the last, so that the server performs the fsyncing WAL checkpoint once
   * at the end. The server restores the configured durability after each call.
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
   * Returns a builder for a change subscription on one table.
   * Chain `.filter()` to narrow the events, then call `.subscribe()`
   * with a callback to start receiving change events.
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
   * @param table - The name of the table.
   * @returns A builder that you narrow with a filter and then subscribe to.
   */
  on(table: string): RemoteSubscriptionBuilder {
    return new RemoteSubscriptionBuilderImpl(table, this.transport)
  }

  /**
   * Closes the transport for this database, so every later call throws.
   * With the WebSocket transport, it also rejects every pending request.
   */
  close(): void {
    this.transport.close()
    this.onDispose?.()
  }
}
