import { createRequire } from 'node:module'
import { defineDriver } from '../../core/driver/define.js'
import { loadThroughRuntime } from '../../core/driver/extension.js'
import { createStatementCache } from '../../core/driver/statement-cache.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'
import { ExtensionError } from '../../core/errors.js'
import { WriterWorker } from '../../core/worker/host.js'
import { nodeBackupEngine, nodeResolveExtensionPath, nodeStreamingSupport, nodeWriterContext } from '../node-runtime.js'
import { copyDatabaseWithNodeSqlite } from './copy.js'

/**
 * Settings for the driver built on Node's own SQLite module.
 *
 * @public
 */
export interface NodeSqliteOptions {
  /**
   * Milliseconds a statement waits for the write lock before it fails.
   */
  busyTimeout?: number
  /**
   * Path to the compiled extension that streams a backup to a caller-supplied
   * destination. It defaults to the binary the install fetched for this
   * platform, and naming one here is how a host with no published binary
   * streams a copy from an extension it built itself.
   */
  vfsExtensionPath?: string
}

const FIRST_NODE_MAJOR_THAT_OPENS_A_COPY_BY_URI = 23

function parsesBackupUris(): boolean {
  return Number.parseInt(process.versions.node, 10) >= FIRST_NODE_MAJOR_THAT_OPENS_A_COPY_BY_URI
}

function carriesSteppedBackupCall(): boolean {
  try {
    return typeof createRequire(import.meta.url)('node:sqlite').backup === 'function'
  } catch {
    return false
  }
}

/**
 * Builds a driver on Node's own SQLite module, which needs no native dependency.
 *
 * @param driverOptions - How long a statement waits for the write lock.
 * @returns The driver, ready to pass to a `Sirannon` registry.
 *
 * @public
 */
export function nodeSqlite(driverOptions?: NodeSqliteOptions): SQLiteDriver {
  const workerEntry = { specifier: import.meta.url, exportName: 'nodeSqlite', config: driverOptions }
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true, steppedCopy: carriesSteppedBackupCall() },
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    createWriterContext: nodeWriterContext,
    createBackupEngine: driver =>
      nodeBackupEngine(
        nodeStreamingSupport({
          driver,
          uriFilenames: parsesBackupUris(),
          ...(driverOptions?.vfsExtensionPath === undefined ? {} : { extensionPath: driverOptions.vfsExtensionPath }),
        }),
      ),
    resolveExtensionPath: nodeResolveExtensionPath,
    async open(path, options) {
      const { DatabaseSync } = await import('node:sqlite')
      const db = new DatabaseSync(path, { readOnly: options?.readonly ?? false, allowExtension: true })
      db.enableLoadExtension?.(false)
      if (options?.walMode !== false) db.exec('PRAGMA journal_mode = WAL')
      db.exec(`PRAGMA synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
      db.exec('PRAGMA foreign_keys = ON')
      db.exec(`PRAGMA busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)
      if (options?.walAutoCheckpoint !== undefined) {
        db.exec(`PRAGMA wal_autocheckpoint = ${Math.trunc(options.walAutoCheckpoint)}`)
      }

      const batchStatementFor = createStatementCache(sql => {
        const stmt = db.prepare(sql)
        stmt.setReadBigInts(true)
        return stmt
      })

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
            } catch {}
            throw err
          }
        },

        async loadExtension(extensionPath: string): Promise<void> {
          if (typeof db.loadExtension !== 'function' || typeof db.enableLoadExtension !== 'function') {
            throw new ExtensionError(
              extensionPath,
              `Node's own SQLite module on ${process.version} carries no extension loading call`,
            )
          }
          await loadThroughRuntime(extensionPath, () => {
            db.enableLoadExtension(true)
            try {
              db.loadExtension(extensionPath)
            } finally {
              db.enableLoadExtension(false)
            }
          })
        },

        copyRunsOffCallerThread: true,

        copyDatabase(request) {
          return copyDatabaseWithNodeSqlite(db, request)
        },

        async close(): Promise<void> {
          db.close()
        },
      }

      return conn
    },
  })
}
