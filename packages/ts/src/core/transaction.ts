import type { SQLiteConnection } from './driver/types.js'
import { execute, executeBatch, query } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'

export class Transaction {
  private _lastInsertRowId: number | bigint = 0

  constructor(private readonly conn: SQLiteConnection) {}

  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    return query<T>(this.conn, sql, params)
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    const result = await execute(this.conn, sql, params)
    this._lastInsertRowId = result.lastInsertRowId
    return result
  }

  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    const results = await executeBatch(this.conn, sql, paramsBatch)
    if (results.length > 0) {
      this._lastInsertRowId = results[results.length - 1].lastInsertRowId
    }
    return results
  }

  get lastInsertRowId(): number | bigint {
    return this._lastInsertRowId
  }

  static async run<T>(conn: SQLiteConnection, fn: (tx: Transaction) => Promise<T>): Promise<T> {
    return conn.transaction(async txConn => {
      const tx = new Transaction(txConn)
      return fn(tx)
    })
  }
}
