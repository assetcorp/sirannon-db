import type { BulkLoadDurability, BulkLoadResult, Params, QueryOptions, WriteConcern } from '../core/types.js'
import type { ExecuteResponse } from '../server/protocol.js'
import { RemoteSubscriptionBuilderImpl } from './subscription.js'
import type { RemoteSubscriptionBuilder, Transport } from './types.js'
import { RemoteError } from './types.js'

const DEFAULT_LOAD_BATCH_SIZE = 1000

/** Options for {@link RemoteDatabase.loadAll}. */
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
 * {@link Database} query interface with async methods that send
 * requests to the server via the configured transport.
 */
export class RemoteDatabase {
  constructor(
    readonly id: string,
    private readonly transport: Transport,
    private readonly onDispose?: () => void,
  ) {}

  /**
   * Execute a SELECT and return all matching rows.
   *
   * ```ts
   * const users = await db.query<{ id: number; name: string }>(
   *   'SELECT * FROM users WHERE age > ?',
   *   [21],
   * )
   * ```
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    const response = await this.transport.query(sql, params, options?.readConcern)
    return response.rows as T[]
  }

  /**
   * Execute a mutation (INSERT, UPDATE, DELETE) and return
   * the number of affected rows and last insert row ID.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    return this.transport.execute(sql, params)
  }

  /**
   * Execute multiple statements as a single atomic transaction.
   * Returns an array of results, one per statement.
   *
   * Requires HTTP transport. WebSocket transport does not
   * support server-side transactions.
   */
  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<ExecuteResponse[]> {
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
    const response = await this.transport.batch(sql, paramsBatch, writeConcern)
    return response.results
  }

  /**
   * Load a whole dataset through the same statement, batching it into requests
   * for you and paying the one fsyncing WAL checkpoint once, after the final
   * batch. The configured durability is restored after every batch, so an
   * import that stops partway never leaves the writer at the relaxed level.
   * Prefer this over {@link load} for anything larger than a single request:
   * it runs the finalize itself, so there is no checkpoint flag to forget.
   *
   * Accepts a synchronous or asynchronous iterable of parameter sets, so rows
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
    const durability = options?.durability
    const total: BulkLoadResult = { rowsLoaded: 0, changes: 0 }

    const send = async (paramsBatch: Params[], checkpoint: boolean): Promise<void> => {
      const summary = await this.transport.load(sql, paramsBatch, durability, checkpoint)
      total.rowsLoaded += summary.rowsLoaded
      total.changes += summary.changes
    }

    // A one-batch lookahead marks only the final batch as the checkpoint, so
    // the fsyncing WAL checkpoint runs once at the end rather than once per
    // batch, while every batch still restores the configured durability.
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
   * low-level primitive; prefer {@link loadAll} for a dataset that spans more
   * than one request, since it runs the finalize itself rather than relying on
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
