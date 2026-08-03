import type { SQLiteConnection } from './driver/types.js'
import { execute, executeBatch, query } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'

/**
 * Runs statements inside one transaction. A function passed to {@link Database.transaction} receives it.
 *
 * @public
 */
export class Transaction {
  private _lastInsertRowId: number | bigint = 0

  constructor(private readonly conn: SQLiteConnection) {}

  /**
   * Runs a read inside this transaction.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @returns The rows the statement produced.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    return query<T>(this.conn, sql, params)
  }

  /**
   * Runs one write inside this transaction.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @returns How many rows changed, and the last inserted row id.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    const result = await execute(this.conn, sql, params)
    this._lastInsertRowId = result.lastInsertRowId
    return result
  }

  /**
   * Runs one statement over many parameter sets inside this transaction.
   *
   * @param sql - The statement to run for each parameter set.
   * @param paramsBatch - One parameter set per run.
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
   * Row id SQLite assigned to the last row this transaction inserted.
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
