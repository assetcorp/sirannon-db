import type { BulkLoadDurability, BulkLoadResult, Params, QueryOptions, WriteConcern } from '../core/types.js'
import type { ExecuteResponse } from '../server/protocol.js'
import { RemoteSubscriptionBuilderImpl } from './subscription.js'
import type { RemoteSubscriptionBuilder, Transport } from './types.js'

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
   * Load many rows through the same statement with writer durability
   * relaxed for the duration, then restored before this resolves. Built
   * for seeding and large imports where the load itself need not be
   * crash-durable but every write after it must run at the configured
   * durability. Returns the total rows loaded and changes applied.
   *
   * Pass `checkpoint: false` on every load but the last of a multi-batch
   * import so the one fsyncing WAL checkpoint is paid once at the end rather
   * than once per batch; the configured durability is still restored after
   * each batch either way.
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
