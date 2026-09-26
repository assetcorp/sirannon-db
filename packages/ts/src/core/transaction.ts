import type { SQLiteConnection } from './driver/types.js'
import { execute, executeBatch, query } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'

/**
 * Executes statements inside one transaction; Sirannon passes it to the function that you give {@link Database.transaction}.
 *
 * @public
 */
export class Transaction {
  private _lastInsertRowId: number | bigint = 0

  constructor(private readonly conn: SQLiteConnection) {}

  /**
   * Executes a read inside this transaction.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @returns The rows that the statement returns.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    return query<T>(this.conn, sql, params)
  }

  /**
   * Executes one write inside this transaction.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @returns The number of rows that the statement changed, and the row id of the last inserted row.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    const result = await execute(this.conn, sql, params)
    this._lastInsertRowId = result.lastInsertRowId
    return result
  }

  /**
   * Executes one statement once for each parameter set, inside this transaction.
   *
   * @param sql - The statement to execute for each parameter set.
   * @param paramsBatch - One parameter set for each execution.
   * @returns One result per parameter set, in order.
   */
  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    const results = await executeBatch(this.conn, sql, paramsBatch)
    if (results.length > 0) {
      this._lastInsertRowId = results[results.length - 1].lastInsertRowId
    }
    return results
  }

  /**
   * The row id that SQLite assigned to the last row that this transaction inserted.
   */
  get lastInsertRowId(): number | bigint {
    return this._lastInsertRowId
  }

  /** @internal */
  static async run<T>(conn: SQLiteConnection, fn: (tx: Transaction) => Promise<T>): Promise<T> {
    return conn.transaction(async txConn => {
      const tx = new Transaction(txConn)
      return fn(tx)
    })
  }
}
