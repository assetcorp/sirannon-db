import type { Params } from '../core/types.js'
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
  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    const response = await this.transport.query(sql, params)
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
