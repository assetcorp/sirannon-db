import { defineDriver } from '../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'

export interface NodeSqliteOptions {
  busyTimeout?: number
}

export function nodeSqlite(driverOptions?: NodeSqliteOptions): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true },
    async open(path, options) {
      const { DatabaseSync } = await import('node:sqlite')
      const db = new DatabaseSync(path, { readOnly: options?.readonly ?? false })
      if (options?.walMode !== false) db.exec('PRAGMA journal_mode = WAL')
      db.exec('PRAGMA synchronous = NORMAL')
      db.exec('PRAGMA foreign_keys = ON')
      db.exec(`PRAGMA busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)

      const conn: SQLiteConnection = {
        async exec(sql: string): Promise<void> {
          db.exec(sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          const stmt = db.prepare(sql)
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              return stmt.all(...(params as Parameters<typeof stmt.all>)) as T[]
            },
            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              return stmt.get(...(params as Parameters<typeof stmt.get>)) as T | undefined
            },
            async run(...params: unknown[]) {
              const result = stmt.run(...(params as Parameters<typeof stmt.run>)) as {
                changes: number | bigint
                lastInsertRowid: number | bigint
              }
              return {
                changes: Number(result.changes),
                lastInsertRowId: result.lastInsertRowid,
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
