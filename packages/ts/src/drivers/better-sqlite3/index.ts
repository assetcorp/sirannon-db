import { defineDriver } from '../../core/driver/define.js'
import { loadThroughRuntime } from '../../core/driver/extension.js'
import { createStatementCache } from '../../core/driver/statement-cache.js'
import { synchronousPragmaValue } from '../../core/driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../core/driver/types.js'
import { bindableParams, narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'
import { EncryptionError } from '../../core/encryption/errors.js'
import { reportEncryptedPageFailures } from '../../core/encryption/page-errors.js'
import { WriterWorker } from '../../core/worker/host.js'
import { asyncLocalWriterContext } from '../async-local-writer-context.js'
import { nodeDatabaseFiles } from '../node-files.js'
import { nodeBackupEngine, nodeResolveExtensionPath, nodeStreamingSupport } from '../node-runtime.js'
import { copyDatabaseWithBetterSqlite3 } from './copy.js'
import {
  assertKeyFitsFile,
  classifyDatabaseFile,
  ENCRYPTION_FEATURE,
  type EncryptingDatabase,
  keyConnection,
  probeDatabaseFile,
} from './encryption.js'
import {
  countingStatement,
  ROW_COUNTS_FEATURE,
  RowCountTaker,
  runCountedBatch,
  runCountedBatchSummary,
} from './row-counting.js'
import { loadSirannonSqlite, type SirannonSqliteFeature } from './sirannon-sqlite.js'

/**
 * Settings for the better-sqlite3 driver.
 *
 * @public
 */
export interface BetterSqlite3Options {
  /**
   * The number of milliseconds that SQLite waits for the database lock before it returns a busy error, which defaults to 5000.
   */
  busyTimeout?: number
  /**
   * The path to the compiled extension that Sirannon uses to stream a backup
   * to a destination that the caller supplies. It defaults to the library in
   * the `@delali/sirannon-vfs-*` package for this platform, when that package
   * is installed. Set it on a platform without a published binary, so that
   * Sirannon streams backups through an extension that you build yourself.
   */
  vfsExtensionPath?: string
  /**
   * Set it to `true` to report the rows that SQLite reads and writes for every
   * statement, through the metrics that `onQueryComplete` receives. The driver
   * then opens databases through `@delali/sirannon-sqlite`, which contains
   * better-sqlite3 built against a SQLite that counts rows. `open` throws
   * `ROW_COUNTS_UNSUPPORTED` when that package or this platform's build is
   * missing.
   */
  rowCounts?: boolean
  /**
   * The path to a build of better-sqlite3's native module against Sirannon's
   * SQLite. It defaults to the file in the `@delali/sirannon-sqlite-*`
   * package for this platform. Set it on a platform without a published
   * build, so that the driver uses a build that you compile from Sirannon's
   * `native/sqlite` directory. The driver reads it only when an option that
   * needs Sirannon's SQLite, such as `rowCounts`, is set.
   */
  sqliteBindingPath?: string
  /**
   * Set it to `true` to open every database through `@delali/sirannon-sqlite`,
   * which contains better-sqlite3 built against SQLite3 Multiple Ciphers, so
   * that a database opened with the `encryption` option keeps every page as
   * AEGIS-256 ciphertext and
   * `changeEncryption` can encrypt, decrypt, or rekey a database. With it unset,
   * the driver keeps better-sqlite3's own SQLite and fails an open with a key
   * with `ENCRYPTION_UNSUPPORTED`.
   */
  encryption?: boolean
}

type BetterSqlite3Database = import('better-sqlite3').Database

function plainStatement(stmt: import('better-sqlite3').Statement): SQLiteStatement {
  return {
    async all<T = unknown>(...params: unknown[]): Promise<T[]> {
      return narrowRowsIntegers(stmt.all(...bindableParams(params)) as T[])
    },
    async allRaw<T = unknown>(...params: unknown[]): Promise<T[]> {
      return stmt.all(...bindableParams(params)) as T[]
    },
    async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
      return narrowRowIntegers(stmt.get(...bindableParams(params)) as T | undefined)
    },
    async run(...params: unknown[]) {
      const result = stmt.run(...bindableParams(params))
      return {
        changes: Number(result.changes),
        lastInsertRowId: narrowSafeBigInt(result.lastInsertRowid) as number | bigint,
      }
    },
  }
}

function createConnection(
  db: BetterSqlite3Database,
  taker: RowCountTaker | null,
  encrypting: EncryptingDatabase | null,
): SQLiteConnection {
  const batchStatementFor = createStatementCache(sql => db.prepare(sql))
  const conn: SQLiteConnection = {
    async exec(sql: string): Promise<void> {
      db.exec(sql)
    },

    async prepare(sql: string): Promise<SQLiteStatement> {
      const stmt = db.prepare(sql)
      return taker ? countingStatement(stmt, taker) : plainStatement(stmt)
    },

    async runBatch(sql: string, paramsBatch: readonly unknown[][]) {
      const stmt = batchStatementFor(sql)
      if (taker) return runCountedBatch(stmt, paramsBatch, taker)
      const results = new Array(paramsBatch.length)
      for (let i = 0; i < paramsBatch.length; i++) {
        const result = stmt.run(...bindableParams(paramsBatch[i]))
        results[i] = {
          changes: Number(result.changes),
          lastInsertRowId: narrowSafeBigInt(result.lastInsertRowid) as number | bigint,
        }
      }
      return results
    },

    async runBatchSummary(sql: string, paramsBatch: readonly unknown[][]) {
      const stmt = batchStatementFor(sql)
      if (taker) return runCountedBatchSummary(stmt, paramsBatch, taker)
      let changes = 0
      for (let i = 0; i < paramsBatch.length; i++) {
        changes += Number(stmt.run(...bindableParams(paramsBatch[i])).changes)
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
        } catch {}
        throw err
      }
    },

    async loadExtension(extensionPath: string): Promise<void> {
      await loadThroughRuntime(extensionPath, () => db.loadExtension(extensionPath))
    },

    copyDatabase(request) {
      return copyDatabaseWithBetterSqlite3(db, request)
    },

    async close(): Promise<void> {
      db.close()
    },
  }

  if (encrypting) {
    conn.startChangeCapture = async () => {
      encrypting.startChangeCapture()
    }
    conn.takeCapturedChanges = async () => encrypting.takeCapturedChanges()
    conn.stopChangeCapture = async () => {
      encrypting.stopChangeCapture()
    }
    conn.applyCapturedChanges = async changes => {
      encrypting.applyChanges(changes)
    }
  }

  return conn
}

interface OpenedDatabase {
  db: BetterSqlite3Database
  taker: RowCountTaker | null
  encrypting: EncryptingDatabase | null
}

function sirannonSqliteFeatures(driverOptions: BetterSqlite3Options | undefined): SirannonSqliteFeature[] {
  const features: SirannonSqliteFeature[] = []
  if (driverOptions?.encryption === true) features.push(ENCRYPTION_FEATURE)
  if (driverOptions?.rowCounts === true) features.push(ROW_COUNTS_FEATURE)
  return features
}

async function openDatabase(
  path: string,
  readonly: boolean,
  driverOptions: BetterSqlite3Options | undefined,
  rawKey: Uint8Array | undefined,
): Promise<OpenedDatabase> {
  const [first, ...rest] = sirannonSqliteFeatures(driverOptions)
  if (first === undefined) {
    const Database = (await import('better-sqlite3')).default
    return { db: new Database(path, { readonly }), taker: null, encrypting: null }
  }
  const sqlite = await loadSirannonSqlite([first, ...rest], driverOptions?.sqliteBindingPath)
  const openConnection = () => new sqlite.Database(path, { readonly, nativeBinding: sqlite.bindingPath })
  if (driverOptions?.encryption === true) {
    const kind = await classifyDatabaseFile(path)
    const settled =
      kind === 'unknown' ? probeDatabaseFile(() => openConnection() as unknown as EncryptingDatabase) : kind
    assertKeyFitsFile(path, settled, rawKey !== undefined)
  }
  const db = openConnection()
  const encrypting = driverOptions?.encryption === true ? (db as unknown as EncryptingDatabase) : null
  try {
    if (rawKey !== undefined && encrypting) keyConnection(encrypting, path, rawKey)
    else db.pragma('temp_store = FILE')
  } catch (err) {
    db.close()
    throw err
  }
  return { db, taker: driverOptions?.rowCounts === true ? new RowCountTaker() : null, encrypting }
}

/**
 * Returns a driver that opens SQLite databases on Node through the `better-sqlite3` native module.
 *
 * @param driverOptions - The busy timeout, the path to the backup streaming extension, and whether the driver counts the rows that each statement reads and writes.
 * @returns The driver, which you can pass to a `Sirannon` registry.
 *
 * @public
 */
export function betterSqlite3(driverOptions?: BetterSqlite3Options): SQLiteDriver {
  const workerEntry = { specifier: import.meta.url, exportName: 'betterSqlite3', config: driverOptions }
  const rowCounts = driverOptions?.rowCounts === true
  const encryption = driverOptions?.encryption === true
  return defineDriver({
    capabilities: { multipleConnections: true, extensions: true, steppedCopy: true, rowCounts, encryption },
    worker: workerEntry,
    files: nodeDatabaseFiles,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, { ...hostOptions, countsRows: rowCounts })
      return host.connection
    },
    createWriterContext: asyncLocalWriterContext,
    createBackupEngine: driver =>
      nodeBackupEngine(
        nodeStreamingSupport({
          driver,
          uriFilenames: process.env.SQLITE_USE_URI === '1',
          ...(driverOptions?.vfsExtensionPath === undefined ? {} : { extensionPath: driverOptions.vfsExtensionPath }),
        }),
      ),
    resolveExtensionPath: nodeResolveExtensionPath,
    async open(path, options) {
      const rawKey = options?.encryption?.key
      if (rawKey !== undefined && !encryption) {
        throw new EncryptionError(
          'The better-sqlite3 driver opens encrypted databases only when you create it with betterSqlite3({ encryption: true })',
          'ENCRYPTION_UNSUPPORTED',
        )
      }
      const { db, taker, encrypting } = await openDatabase(path, options?.readonly ?? false, driverOptions, rawKey)
      try {
        db.defaultSafeIntegers(true)
        if (options?.walMode !== false) db.pragma('journal_mode = WAL')
        db.pragma(`synchronous = ${synchronousPragmaValue(options?.synchronous)}`)
        db.pragma('foreign_keys = ON')
        db.pragma(`busy_timeout = ${driverOptions?.busyTimeout ?? 5000}`)
        if (options?.walAutoCheckpoint !== undefined) {
          db.pragma(`wal_autocheckpoint = ${Math.trunc(options.walAutoCheckpoint)}`)
        }
      } catch (err) {
        db.close()
        throw err
      }
      const connection = createConnection(db, taker, encrypting)
      return rawKey === undefined ? connection : reportEncryptedPageFailures(connection)
    },
  })
}
