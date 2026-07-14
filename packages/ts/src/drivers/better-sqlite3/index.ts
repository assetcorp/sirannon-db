import { defineDriver } from '../../core/driver/define.js'
import { createStatementCache } from '../../core/driver/statement-cache.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'

export interface BetterSqlite3Options {
  busyTimeout?: number
}

function createConnection(db: import('better-sqlite3').Database): SQLiteConnection {
  const batchStatementFor = createStatementCache(sql => db.prepare(sql))
  const conn: SQLiteConnection = {
    async exec(sql: string): Promise<void> {
      db.exec(sql)
    },

    async prepare(sql: string): Promise<SQLiteStatement> {
      const stmt = db.prepare(sql)
      return {
        async all<T = unknown>(...params: unknown[]): Promise<T[]> {
          return narrowRowsIntegers(stmt.all(...params) as T[])
        },
        async allRaw<T = unknown>(...params: unknown[]): Promise<T[]> {
          return stmt.all(...params) as T[]
        },
        async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
          return narrowRowIntegers(stmt.get(...params) as T | undefined)
        },
        async run(...params: unknown[]) {
          const result = stmt.run(...params)
          return {
            changes: Number(result.changes),
            lastInsertRowId: narrowSafeBigInt(result.lastInsertRowid) as number | bigint,
          }
        },
      }
    },

    async runBatch(sql: string, paramsBatch: readonly unknown[][]) {
      const stmt = batchStatementFor(sql)
      const results = new Array(paramsBatch.length)
      for (let i = 0; i < paramsBatch.length; i++) {
        const result = stmt.run(...paramsBatch[i])
        results[i] = {
          changes: Number(result.changes),
          lastInsertRowId: narrowSafeBigInt(result.lastInsertRowid) as number | bigint,
        }
      }
      return results
    },

    async runBatchSummary(sql: string, paramsBatch: readonly unknown[][]) {
      const stmt = batchStatementFor(sql)
      let changes = 0
      for (let i = 0; i < paramsBatch.length; i++) {
        changes += Number(stmt.run(...paramsBatch[i]).changes)
      }
      return { rowsLoaded: paramsBatch.length, changes }
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
    worker: { specifier: import.meta.url, exportName: 'betterSqlite3', config: driverOptions },
    async open(path, options) {
      const Database = (await import('better-sqlite3')).default
      const db = new Database(path, { readonly: options?.readonly ?? false })
      db.defaultSafeIntegers(true)
      if (options?.walMode !== false) db.pragma('journal_mode = WAL')
      db.pragma(`synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
      db.pragma('foreign_keys = ON')
      db.pragma(`busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)
      return createConnection(db)
    },
  })
}
