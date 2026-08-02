import type { BackupScheduleOptions } from '../types.js'
import type { WorkerHostOptions } from '../worker/host.js'

export interface RunResult {
  changes: number
  lastInsertRowId: number | bigint
}

/**
 * Tells a caller running inside the operation that holds the writer from one
 * merely waiting on it. A runtime without async context tracking cannot answer
 * this, and answering it wrongly runs one caller's writes inside another
 * caller's transaction.
 */
export interface WriterContext {
  run<T>(operation: () => T): T
  isActive(): boolean
  exit<T>(operation: () => T): T
}

export interface BackupEngine {
  backup(conn: SQLiteConnection, destPath: string): Promise<void>
  schedule(
    conn: SQLiteConnection,
    options: BackupScheduleOptions,
    runExclusive: (op: () => Promise<void>) => Promise<void>,
  ): () => void
}

export interface BatchSummary {
  rowsLoaded: number
  changes: number
}

export interface SQLiteStatement {
  all<T = unknown>(...params: unknown[]): Promise<T[]>
  get<T = unknown>(...params: unknown[]): Promise<T | undefined>
  run(...params: unknown[]): Promise<RunResult>
  /**
   * Like {@link all} but skips the safe-range BigInt narrowing, leaving every
   * integer as a BigInt. The server wire path narrows and tags in one pass, so
   * feeding it raw rows avoids a second walk. Optional: a driver that omits it
   * falls back to {@link all}, still correct but with the extra narrowing walk.
   */
  allRaw?<T = unknown>(...params: unknown[]): Promise<T[]>
}

export interface GroupRunError {
  message: string
  name?: string
  code?: string
}

export type GroupRunOutcome = { ok: true; results: RunResult[] } | { ok: false; error: GroupRunError }

export interface SQLiteConnection {
  exec(sql: string): Promise<void>
  prepare(sql: string): Promise<SQLiteStatement>
  transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T>
  close(): Promise<void>
  runBatch?(sql: string, paramsBatch: readonly unknown[][]): Promise<RunResult[]>
  runBatchSummary?(sql: string, paramsBatch: readonly unknown[][]): Promise<BatchSummary>
  /**
   * Runs several independent units in one transaction, one outcome per unit in
   * order. A unit is one write or one whole transaction, and a unit that fails
   * must not disturb the others.
   */
  runGroup?(
    units: readonly { statements: readonly { sql: string; params?: readonly unknown[]; trusted?: boolean }[] }[],
  ): Promise<GroupRunOutcome[]>
}

/**
 * SQLite `PRAGMA synchronous` level applied to a connection. `normal` is safe
 * from corruption in WAL mode but can lose the most recent commits on power
 * loss; `full` fsyncs every commit; `extra` adds a directory sync after the
 * rollback journal is unlinked in DELETE journal mode and equals `full` in
 * WAL mode; `off` hands writes to the OS without syncing and is sanctioned
 * only for re-runnable bulk loads.
 */
export type SynchronousLevel = 'off' | 'normal' | 'full' | 'extra'

export interface OpenOptions {
  readonly?: boolean
  walMode?: boolean
  synchronous?: SynchronousLevel
}

export interface DriverCapabilities {
  multipleConnections: boolean
  extensions: boolean
}

/**
 * Lets a worker thread rebuild the driver, since the driver's `open` function
 * cannot cross the thread boundary. `specifier` must be importable from the
 * worker and `config` must survive a structured clone; the worker imports the
 * module and calls its `exportName` factory (default export otherwise) with it.
 */
export interface DriverWorkerEntry {
  specifier: string
  exportName?: string
  config?: unknown
}

export interface SQLiteDriver {
  readonly capabilities: DriverCapabilities
  open(path: string, options?: OpenOptions): Promise<SQLiteConnection>
  readonly worker?: DriverWorkerEntry
  /**
   * Offloads writes to a worker thread. Only a driver whose runtime has
   * threads implements this, which is what keeps the thread machinery out of
   * bundles built for runtimes that do not.
   */
  startWriterHost?(path: string, options: OpenOptions, hostOptions?: WorkerHostOptions): Promise<SQLiteConnection>
  createWriterContext?(): WriterContext
  createBackupEngine?(): BackupEngine
  /**
   * Makes an extension path absolute. Passing a bare name to `load_extension`
   * would let the dynamic linker search its own paths and open a different
   * library than the operator named.
   */
  resolveExtensionPath?(extensionPath: string): string
}
