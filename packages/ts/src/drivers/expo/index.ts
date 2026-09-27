import { defineDriver } from '../../core/driver/define.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { ExtensionError } from '../../core/errors.js'

/**
 * Returns a driver that opens SQLite databases on a device through `expo-sqlite`.
 *
 * The driver sets `multipleConnections` to `false`, so Sirannon opens no reader
 * pool and sends every read and write for a database through one connection.
 *
 * @returns The driver, which you can pass to a `Sirannon` registry in a React Native app.
 *
 * @public
 */
export function expoSqlite(): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: false, extensions: false, steppedCopy: false },
    async open(path, options) {
      const SQLite = await import('expo-sqlite')
      const db = await SQLite.openDatabaseAsync(path, {
        readOnly: options?.readonly,
      } as Record<string, unknown>)
      type SQLiteHandle = Awaited<ReturnType<typeof SQLite.openDatabaseAsync>>

      if (options?.walMode !== false) await db.execAsync('PRAGMA journal_mode = WAL')
      await db.execAsync(`PRAGMA synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
      await db.execAsync('PRAGMA foreign_keys = ON')
      if (options?.walAutoCheckpoint !== undefined) {
        await db.execAsync(`PRAGMA wal_autocheckpoint = ${Math.trunc(options.walAutoCheckpoint)}`)
      }

      const buildConnectionFromHandle = (dbHandle: SQLiteHandle): SQLiteConnection => ({
        async exec(sql: string): Promise<void> {
          await dbHandle.execAsync(sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              return dbHandle.getAllAsync(sql, params as (string | number | null)[]) as Promise<T[]>
            },

            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              const row = await dbHandle.getFirstAsync(sql, params as (string | number | null)[])
              return (row ?? undefined) as T | undefined
            },

            async run(...params: unknown[]) {
              const result = await dbHandle.runAsync(sql, params as (string | number | null)[])
              return {
                changes: result.changes,
                lastInsertRowId: result.lastInsertRowId,
              }
            },
          }
        },

        async transaction<T>(fn: (c: SQLiteConnection) => Promise<T>): Promise<T> {
          let result: T | undefined
          await dbHandle.withExclusiveTransactionAsync(async txDb => {
            const txConn = buildConnectionFromHandle(txDb)
            result = await fn(txConn)
          })
          return result as T
        },

        async loadExtension(extensionPath: string): Promise<void> {
          throw new ExtensionError(
            extensionPath,
            'expo-sqlite carries no extension loading call, so a device running Expo loads no compiled extension',
          )
        },

        async close(): Promise<void> {
          await dbHandle.closeAsync()
        },
      })

      const conn = buildConnectionFromHandle(db)
      return conn
    },
  })
}
