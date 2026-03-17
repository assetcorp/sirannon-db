import { defineDriver } from '../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'

export interface WaSqliteOptions {
  vfs?: 'IDBBatchAtomicVFS' | 'AccessHandlePoolVFS'
}

export function waSqlite(driverOptions?: WaSqliteOptions): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: false, extensions: false },
    async open(path, options) {
      const { default: SQLiteESMFactory } = await import('wa-sqlite/dist/wa-sqlite-async.mjs')
      const { Factory } = await import('wa-sqlite')

      const module = await SQLiteESMFactory()
      const sqlite3 = Factory(module)

      const vfsName = driverOptions?.vfs ?? 'IDBBatchAtomicVFS'
      if (vfsName === 'AccessHandlePoolVFS') {
        const { AccessHandlePoolVFS } = await import('wa-sqlite/src/examples/AccessHandlePoolVFS.js')
        const vfs = new AccessHandlePoolVFS(path)
        await vfs.isReady
        sqlite3.vfs_register(vfs, true)
      } else {
        const { IDBBatchAtomicVFS } = await import('wa-sqlite/src/examples/IDBBatchAtomicVFS.js')
        const vfs = new IDBBatchAtomicVFS(path)
        await vfs.isReady
        sqlite3.vfs_register(vfs, true)
      }

      const db = await sqlite3.open_v2(path)
      if (options?.walMode !== false) {
        try {
          await sqlite3.exec(db, 'PRAGMA journal_mode = WAL')
        } catch {
          // WAL may not be supported by all VFS implementations
        }
      }
      await sqlite3.exec(db, 'PRAGMA synchronous = NORMAL')
      await sqlite3.exec(db, 'PRAGMA foreign_keys = ON')

      async function execStatements(
        sql: string,
        params?: unknown[],
      ): Promise<{ columns: string[]; rows: unknown[][]; changes: number; lastRowId: number | bigint }> {
        const columns: string[] = []
        const rows: unknown[][] = []
        let changes = 0
        let lastRowId: number | bigint = 0

        for await (const stmt of sqlite3.statements(db, sql)) {
          if (params && params.length > 0) {
            for (let i = 0; i < params.length; i++) {
              sqlite3.bind(stmt, i + 1, params[i] as number | string | null)
            }
          }

          const cols = sqlite3.column_names(stmt)
          if (cols.length > 0 && columns.length === 0) {
            columns.push(...cols)
          }

          while ((await sqlite3.step(stmt)) === 100) {
            const row: unknown[] = []
            for (let i = 0; i < cols.length; i++) {
              row.push(sqlite3.column(stmt, i))
            }
            rows.push(row)
          }
        }

        changes = sqlite3.changes(db)
        let rowIdCaptured = false
        await sqlite3.exec(db, 'SELECT last_insert_rowid()', (row: unknown[]) => {
          if (!rowIdCaptured && row[0] !== undefined) {
            lastRowId = row[0] as number | bigint
            rowIdCaptured = true
          }
        })
        return { columns, rows, changes, lastRowId }
      }

      const conn: SQLiteConnection = {
        async exec(sql: string): Promise<void> {
          await sqlite3.exec(db, sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              const result = await execStatements(sql, params)
              return result.rows.map(row => {
                const obj: Record<string, unknown> = {}
                for (let i = 0; i < result.columns.length; i++) {
                  obj[result.columns[i]] = row[i]
                }
                return obj as T
              })
            },

            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              const result = await execStatements(sql, params)
              if (result.rows.length === 0) return undefined
              const obj: Record<string, unknown> = {}
              for (let i = 0; i < result.columns.length; i++) {
                obj[result.columns[i]] = result.rows[0][i]
              }
              return obj as T
            },

            async run(...params: unknown[]) {
              const result = await execStatements(sql, params)
              return {
                changes: result.changes,
                lastInsertRowId: result.lastRowId,
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
          sqlite3.close(db)
        },
      }

      return conn
    },
  })
}
