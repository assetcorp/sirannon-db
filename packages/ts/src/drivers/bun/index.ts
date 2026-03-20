import { defineDriver } from '../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'

export interface BunSqliteOptions {
  busyTimeout?: number
}

export function bunSqlite(driverOptions?: BunSqliteOptions): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true },
    async open(path, options) {
      const { Database } = await import('bun:sqlite')
      const db = new Database(path, { readonly: options?.readonly ?? false })
      if (options?.walMode !== false) db.run('PRAGMA journal_mode = WAL')
      db.run('PRAGMA synchronous = NORMAL')
      db.run('PRAGMA foreign_keys = ON')
      db.run(`PRAGMA busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)

      const conn: SQLiteConnection = {
        async exec(sql: string): Promise<void> {
          db.run(sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          const stmt = db.query(sql)
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              return stmt.all(...params) as T[]
            },
            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              return (stmt.get(...params) as T | null) ?? undefined
            },
            async run(...params: unknown[]) {
              stmt.run(...params)
              const changesStmt = db.query('SELECT changes() AS changes, last_insert_rowid() AS lastId')
              const info = changesStmt.get() as { changes: number; lastId: number | bigint }
              return {
                changes: info.changes,
                lastInsertRowId: info.lastId,
              }
            },
          }
        },

        async transaction<T>(fn: (c: SQLiteConnection) => Promise<T>): Promise<T> {
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
    },
  })
}
