import { defineDriver } from '../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'

export function expoSqlite(): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: false, extensions: false },
    async open(path, options) {
      const SQLite = await import('expo-sqlite')
      const db = await SQLite.openDatabaseAsync(path, {
        readOnly: options?.readonly,
      } as Record<string, unknown>)
      if (options?.walMode !== false) await db.execAsync('PRAGMA journal_mode = WAL')
      await db.execAsync('PRAGMA synchronous = NORMAL')
      await db.execAsync('PRAGMA foreign_keys = ON')

      const conn: SQLiteConnection = {
        async exec(sql: string): Promise<void> {
          await db.execAsync(sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              return db.getAllAsync(sql, params as (string | number | null)[]) as Promise<T[]>
            },

            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              const row = await db.getFirstAsync(sql, params as (string | number | null)[])
              return (row ?? undefined) as T | undefined
            },

            async run(...params: unknown[]) {
              const result = await db.runAsync(sql, params as (string | number | null)[])
              return {
                changes: result.changes,
                lastInsertRowId: result.lastInsertRowId,
              }
            },
          }
        },

        async transaction<T>(fn: (c: SQLiteConnection) => Promise<T>): Promise<T> {
          let result: T | undefined
          await db.withTransactionAsync(async () => {
            result = await fn(conn)
          })
          return result as T
        },

        async close(): Promise<void> {
          await db.closeAsync()
        },
      }

      return conn
    },
  })
}
