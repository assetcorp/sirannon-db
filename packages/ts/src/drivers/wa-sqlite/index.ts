import { defineDriver } from '../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'

export interface WaSqliteOptions {
  vfs?: 'IDBBatchAtomicVFS' | 'AccessHandlePoolVFS'
}

export function waSqlite(driverOptions?: WaSqliteOptions): SQLiteDriver {
  return defineDriver({
    capabilities: { multipleConnections: false, extensions: false },
    async open(path, options) {
      const { default: SQLiteESMFactory } = await import('wa-sqlite')
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
      if (options?.walMode !== false) await sqlite3.exec(db, 'PRAGMA journal_mode = WAL')
      await sqlite3.exec(db, 'PRAGMA synchronous = NORMAL')
      await sqlite3.exec(db, 'PRAGMA foreign_keys = ON')

      const conn: SQLiteConnection = {
        async exec(sql: string): Promise<void> {
          await sqlite3.exec(db, sql)
        },

        async prepare(sql: string): Promise<SQLiteStatement> {
          return {
            async all<T = unknown>(...params: unknown[]): Promise<T[]> {
              const rows: T[] = []
              const stmtPtr = await sqlite3.prepare_v2(db, sql)
              try {
                if (params.length > 0) {
                  for (let i = 0; i < params.length; i++) {
                    sqlite3.bind(stmtPtr, i + 1, params[i] as number | string | null)
                  }
                }
                const columns = sqlite3.column_names(stmtPtr)
                while ((await sqlite3.step(stmtPtr)) === 100) {
                  const row: Record<string, unknown> = {}
                  for (let i = 0; i < columns.length; i++) {
                    row[columns[i]] = sqlite3.column(stmtPtr, i)
                  }
                  rows.push(row as T)
                }
              } finally {
                sqlite3.finalize(stmtPtr)
              }
              return rows
            },

            async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
              const stmtPtr = await sqlite3.prepare_v2(db, sql)
              try {
                if (params.length > 0) {
                  for (let i = 0; i < params.length; i++) {
                    sqlite3.bind(stmtPtr, i + 1, params[i] as number | string | null)
                  }
                }
                const columns = sqlite3.column_names(stmtPtr)
                if ((await sqlite3.step(stmtPtr)) === 100) {
                  const row: Record<string, unknown> = {}
                  for (let i = 0; i < columns.length; i++) {
                    row[columns[i]] = sqlite3.column(stmtPtr, i)
                  }
                  return row as T
                }
                return undefined
              } finally {
                sqlite3.finalize(stmtPtr)
              }
            },

            async run(...params: unknown[]) {
              const stmtPtr = await sqlite3.prepare_v2(db, sql)
              try {
                if (params.length > 0) {
                  for (let i = 0; i < params.length; i++) {
                    sqlite3.bind(stmtPtr, i + 1, params[i] as number | string | null)
                  }
                }
                while ((await sqlite3.step(stmtPtr)) === 100) {
                  // step until done
                }
              } finally {
                sqlite3.finalize(stmtPtr)
              }
              return {
                changes: sqlite3.changes(db),
                lastInsertRowId: sqlite3.lastInsertRowid(db),
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
