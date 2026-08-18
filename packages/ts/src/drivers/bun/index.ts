/// <reference path="./bun-sqlite.d.ts" />
import { resolve } from 'node:path'
import { defineDriver } from '../../core/driver/define.js'
import { loadThroughRuntime } from '../../core/driver/extension.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'

/**
 * @public
 *
 * Settings for the driver built on Bun's built-in SQLite.
 */
export interface BunSqliteOptions {
  /**
   * Milliseconds a statement waits for the write lock before it fails as busy. Default: 5000.
   */
  busyTimeout?: number
}

/**
 * @public
 *
 * Builds a driver that runs SQLite through `bun:sqlite`, which is built into the Bun runtime.
 *
 * It reads every integer as a BigInt and narrows the safe ones back, so a
 * value beyond `Number.MAX_SAFE_INTEGER` survives the round trip.
 *
 * @param driverOptions - How long a statement waits for the write lock.
 * @returns The driver, ready to pass to a `Sirannon` registry running under Bun.
 */
export function bunSqlite(driverOptions?: BunSqliteOptions): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true },
    resolveExtensionPath: extensionPath => resolve(extensionPath),
    async open(path, options) {
      const { Database } = await import('bun:sqlite')
      const db = new Database(path, { readonly: options?.readonly ?? false, safeIntegers: true })
      if (options?.walMode !== false) db.run('PRAGMA journal_mode = WAL')
      db.run(`PRAGMA synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
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
              return narrowRowsIntegers(stmt.all(...params) as T[])
            },
            async allRaw<T = unknown>(...params: unknown[]): Promise<T[]> {
              return stmt.all(...params) as T[]
            },
            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              return narrowRowIntegers((stmt.get(...params) as T | null) ?? undefined)
            },
            async run(...params: unknown[]) {
              stmt.run(...params)
              const changesStmt = db.query('SELECT changes() AS changes, last_insert_rowid() AS lastId')
              const info = changesStmt.get() as { changes: number | bigint; lastId: number | bigint }
              return {
                changes: Number(info.changes),
                lastInsertRowId: narrowSafeBigInt(info.lastId) as number | bigint,
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
            } catch {}
            throw err
          }
        },

        async loadExtension(extensionPath: string): Promise<void> {
          await loadThroughRuntime(extensionPath, () => db.loadExtension(extensionPath))
        },

        async close(): Promise<void> {
          db.close()
        },
      }

      return conn
    },
  })
}
