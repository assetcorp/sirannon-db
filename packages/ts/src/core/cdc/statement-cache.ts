import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'

export class StatementCache {
  private readonly cache = new WeakMap<SQLiteConnection, Map<string, Promise<SQLiteStatement>>>()

  async get(conn: SQLiteConnection, key: string, sql: string): Promise<SQLiteStatement> {
    let stmts = this.cache.get(conn)
    if (!stmts) {
      stmts = new Map()
      this.cache.set(conn, stmts)
    }

    const existing = stmts.get(key)
    if (existing) return existing

    const pending = conn.prepare(sql)
    stmts.set(key, pending)

    try {
      return await pending
    } catch (err) {
      stmts.delete(key)
      throw err
    }
  }
}
