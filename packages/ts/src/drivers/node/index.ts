import { defineDriver } from '../../core/driver/define.js'
import { createStatementCache } from '../../core/driver/statement-cache.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'

export interface NodeSqliteOptions {
  busyTimeout?: number
}

export function nodeSqlite(driverOptions?: NodeSqliteOptions): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true },
    worker: { specifier: import.meta.url, exportName: 'nodeSqlite', config: driverOptions },
    async open(path, options) {
      const { DatabaseSync } = await import('node:sqlite')
      const db = new DatabaseSync(path, { readOnly: options?.readonly ?? false })
      if (options?.walMode !== false) db.exec('PRAGMA journal_mode = WAL')
      db.exec(`PRAGMA synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
      db.exec('PRAGMA foreign_keys = ON')
      db.exec(`PRAGMA busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)

      const batchStatementFor = createStatementCache(sql => db.prepare(sql))

      const conn: SQLiteConnection = {
        async exec(sql: string): Promise<void> {
          db.exec(sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          const stmt = db.prepare(sql)
          stmt.setReadBigInts(true)
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              return narrowRowsIntegers(stmt.all(...(params as Parameters<typeof stmt.all>)) as T[])
            },
            async allRaw<T = unknown>(...params: unknown[]): Promise<T[]> {
              return stmt.all(...(params as Parameters<typeof stmt.all>)) as T[]
            },
            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              return narrowRowIntegers(stmt.get(...(params as Parameters<typeof stmt.get>)) as T | undefined)
            },
            async run(...params: unknown[]) {
              const result = stmt.run(...(params as Parameters<typeof stmt.run>)) as {
                changes: number | bigint
                lastInsertRowid: number | bigint
              }
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
            const result = stmt.run(...(paramsBatch[i] as Parameters<typeof stmt.run>)) as {
              changes: number | bigint
              lastInsertRowid: number | bigint
            }
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
            const result = stmt.run(...(paramsBatch[i] as Parameters<typeof stmt.run>)) as { changes: number | bigint }
            changes += Number(result.changes)
          }
          return { rowsLoaded: paramsBatch.length, changes }
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
