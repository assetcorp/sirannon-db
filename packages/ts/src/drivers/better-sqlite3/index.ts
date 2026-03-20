import { defineDriver } from '../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'

export interface BetterSqlite3Options {
  busyTimeout?: number
}

function createConnection(db: import('better-sqlite3').Database): SQLiteConnection {
  const conn: SQLiteConnection = {
    async exec(sql: string): Promise<void> {
      db.exec(sql)
    },

    async prepare(sql: string): Promise<SQLiteStatement> {
      const stmt = db.prepare(sql)
      return {
        async all<T = unknown>(...params: unknown[]): Promise<T[]> {
          return stmt.all(...params) as T[]
        },
        async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
          return stmt.get(...params) as T | undefined
        },
        async run(...params: unknown[]) {
          const result = stmt.run(...params)
          return {
            changes: result.changes,
            lastInsertRowId: result.lastInsertRowid,
          }
        },
      }
    },

    async transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
      await conn.exec('BEGIN')
      try {
        const result = await fn(conn)
        await conn.exec('COMMIT')
        return result
      } catch (err) {
        try {
          await conn.exec('ROLLBACK')
        } catch {
          /* ROLLBACK failure is secondary; preserve the original error */
        }
        throw err
      }
    },

    async close(): Promise<void> {
      db.close()
    },
  }

  return conn
}

export function betterSqlite3(driverOptions?: BetterSqlite3Options): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true },
    async open(path, options) {
      const Database = (await import('better-sqlite3')).default
      const db = new Database(path, { readonly: options?.readonly ?? false })
      if (options?.walMode !== false) db.pragma('journal_mode = WAL')
      db.pragma('synchronous = NORMAL')
      db.pragma('foreign_keys = ON')
      db.pragma(`busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)
      return createConnection(db)
    },
  })
}
