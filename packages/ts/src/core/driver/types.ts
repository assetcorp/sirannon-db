import type { BackupCycle } from '../backup/cycle.js'
import type { BackupCycleRequest } from '../backup/cycle-options.js'
import type { BackupFileCopy, BackupRunReport, BackupRunRequest } from '../backup/report.js'
import type { BackupScheduleRequest } from '../backup/schedule-request.js'
import type { WorkerHostOptions } from '../worker/host.js'

/** Describes the result of one write through the driver.
 * @public
 */
export interface RunResult {
  /** The number of rows that the statement inserted, updated, or deleted. */
  changes: number
  /** The rowid that SQLite assigned to the last inserted row. */
  lastInsertRowId: number | bigint
}

/**
 * Distinguishes a caller that runs inside the operation holding the writer
 * from a caller that waits for the writer. A runtime needs async context
 * tracking to make that distinction, and a wrong answer would run one
 * caller's writes inside another caller's transaction.
 */
export interface WriterContext {
  /** Runs an operation and marks it as the one that holds the writer. */
  run<T>(operation: () => T): T
  /** Returns `true` when the caller runs inside the operation that holds the writer. */
  isActive(): boolean
  /** Runs an operation outside the held writer's context, so that its writes stay out of that transaction. */
  exit<T>(operation: () => T): T
}

/**
 * Copies a database to a file or a destination, and schedules repeating copies.
 *
 * @internal
 */
export interface BackupEngine {
  /** Copies the connection's database to a destination path. */
  backup(conn: SQLiteConnection, destPath: string, onFirstStep?: () => void): Promise<BackupFileCopy>
  /** Copies the connection's database to a destination that the caller supplies. */
  copyToDestination(conn: SQLiteConnection, request: BackupRunRequest): Promise<BackupRunReport>
  /** Returns `true` when a full copy streams to the destination without a local file. */
  streamsToDestination(): boolean
  /** Returns a backup cycle that captures the write-ahead log and then checkpoints it. */
  createCycle(request: BackupCycleRequest): BackupCycle
  /** Starts a repeating backup and returns a function that stops it. */
  schedule(conn: SQLiteConnection, request: BackupScheduleRequest): () => void
}

/** Describes the progress after one step of a stepped database copy.
 * @public
 */
export interface DatabaseCopyStep {
  /** The total number of pages to copy. */
  totalPages: number
  /** The number of pages left to copy. */
  remainingPages: number
}

/** Describes the copy that Sirannon requests from a driver, and the number of pages in each step.
 * @public
 */
export interface DatabaseCopyRequest {
  /** The path of the copy's destination file. */
  destPath: string
  /** The number of pages that the driver copies in one step. */
  pagesPerStep: number
  /**
   * The driver calls this function after every step with that step's page
   * counts. The function returns the number of pages for the next step, which
   * a driver passes on when its runtime accepts a new count for each step. A
   * return of zero pauses the copy until the caller catches up.
   */
  onStep?: (step: DatabaseCopyStep) => number
  /**
   * The number of milliseconds without a step after which the caller stops
   * waiting for the copy. A driver that steps the copy on another thread waits
   * this long for that thread, so a longer timeout keeps a slow copy running.
   * Zero removes the limit.
   */
  stallTimeoutMs?: number
}

/** Holds the totals for one statement that the driver applies over many parameter sets.
 * @public
 */
export interface BatchSummary {
  /** The number of parameter sets that the driver applied. */
  rowsLoaded: number
  /** The number of rows that those statements inserted, updated, or deleted. */
  changes: number
}

/** A prepared statement that a driver returns, which you can run many times.
 * @public
 */
export interface SQLiteStatement {
  /** Runs the statement and returns every row. */
  all<T = unknown>(...params: unknown[]): Promise<T[]>
  /** Runs the statement and returns the first row, or undefined when the result is empty. */
  get<T = unknown>(...params: unknown[]): Promise<T | undefined>
  /** Runs the statement as a write and returns the number of changed rows and the last inserted rowid. */
  run(...params: unknown[]): Promise<RunResult>
  /**
   * Runs the statement and returns every row with each integer as a `bigint`,
   * skipping the narrowing that {@link SQLiteStatement.all} applies. The
   * server's wire encoder narrows and tags integers in one pass, so reading raw
   * rows saves the server a second pass over the data. When a driver omits this method,
   * Sirannon calls {@link SQLiteStatement.all}, which gives the same result
   * with the extra pass.
   */
  allRaw?<T = unknown>(...params: unknown[]): Promise<T[]>
}

/**
 * Describes the error from one failed unit of a grouped write.
 *
 * @internal
 */
export interface GroupRunError {
  /** The message that SQLite or the driver raised. */
  message: string
  /** The name of the error class. */
  name?: string
  /** The machine-readable code, when the driver supplies one. */
  code?: string
}

/**
 * Describes the outcome of one unit in a grouped write.
 *
 * @internal
 */
export type GroupRunOutcome = { ok: true; results: RunResult[] } | { ok: false; error: GroupRunError }

/** One open connection to a SQLite database.
 * @public
 */
export interface SQLiteConnection {
  /** Runs one or more statements and returns no rows. */
  exec(sql: string): Promise<void>
  /** Compiles a statement, so that the caller can run it many times. */
  prepare(sql: string): Promise<SQLiteStatement>
  /** Runs a function inside one transaction, committing when it returns and rolling back when it throws. */
  transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T>
  /** Closes the connection. */
  close(): Promise<void>
  /**
   * Loads a compiled SQLite extension into this connection through the
   * runtime's own loading function, so that queries on this connection can
   * call the extension's functions. SQLite registers those functions on the
   * loading connection alone, so load the extension on every connection that
   * calls them. When the runtime has no loading function, the returned promise
   * rejects with an error that names the runtime.
   */
  loadExtension?(extensionPath: string): Promise<void>
  /**
   * Copies this connection's database to a file through SQLite's stepped
   * backup interface, so that writes on this connection can run between
   * steps. SQLite restarts the copy whenever another connection writes to the
   * source, so call this method on the writer connection. The driver throws a
   * `BackupError` when a transaction is open on this connection, because
   * SQLite would copy no pages and still report success. When the runtime has
   * no stepped backup function, the method should reject with a
   * `SirannonError` whose code is `BACKUP_UNSUPPORTED` and whose message names
   * the runtime, as the Node driver does.
   */
  copyDatabase?(request: DatabaseCopyRequest): Promise<DatabaseCopyStep>
  /**
   * `true` when the runtime steps this connection's copy on a thread other
   * than the caller's. A copy on its own thread can pause until the caller
   * catches up, while a pause on the caller's thread would block the only
   * thread.
   */
  readonly copyRunsOffCallerThread?: boolean
  /** Runs one statement once for each parameter set and returns one result for each set. */
  runBatch?(sql: string, paramsBatch: readonly unknown[][]): Promise<RunResult[]>
  /** Runs one statement once for each parameter set and returns only the totals. */
  runBatchSummary?(sql: string, paramsBatch: readonly unknown[][]): Promise<BatchSummary>
  /**
   * Runs several independent units in one transaction and returns one outcome
   * for each unit, in order. A unit is one write or one whole transaction. A
   * failed unit should leave the other units' writes intact.
   */
  runGroup?(
    units: readonly { statements: readonly { sql: string; params?: readonly unknown[]; trusted?: boolean }[] }[],
  ): Promise<GroupRunOutcome[]>
}

/**
 * The SQLite `PRAGMA synchronous` level for a connection. In WAL mode,
 * `normal` keeps the database free of corruption, but a power loss can lose
 * the most recent commits. `full` syncs every commit to disk. `extra` also
 * syncs the directory after SQLite deletes the rollback journal in DELETE
 * journal mode, and it behaves like `full` in WAL mode. `off` passes writes to
 * the operating system without syncing, so use it only for a bulk load that
 * you can run again.
 *
 * @public
 */
export type SynchronousLevel = 'off' | 'normal' | 'full' | 'extra'

/** Settings with which a driver opens one database file.
 * @public
 */
export interface OpenOptions {
  /** `true` to open the file for reads only. */
  readonly?: boolean
  /** Puts the database in write-ahead logging mode unless set to `false`. */
  walMode?: boolean
  /** The writer durability level for the connection. */
  synchronous?: SynchronousLevel
  /**
   * The number of frames in the write-ahead log at which SQLite checkpoints it
   * automatically. Set it to zero when Sirannon captures the database's own
   * log, because an automatic checkpoint lets SQLite overwrite frames that the
   * backup has yet to capture.
   *
   * SQLite keeps this setting per connection, so a driver applies it on every
   * open, including after a writer worker restarts.
   */
  walAutoCheckpoint?: number
}

/** Describes what a driver's runtime supports.
 * @public
 */
export interface DriverCapabilities {
  /** `true` when the runtime can open more than one connection to the same file. */
  multipleConnections: boolean
  /** `true` when the runtime can load SQLite extensions. */
  extensions: boolean
  /** `true` when the runtime can copy an open database through SQLite's stepped backup interface. */
  steppedCopy: boolean
}

/**
 * Describes how a worker thread rebuilds the driver, since a function such as
 * the driver's `open` cannot pass to another thread. The worker imports
 * `specifier` and calls the factory named `exportName`, or the default export,
 * with `config`. `specifier` should be importable from the worker, and
 * `config` should be a value that `structuredClone` accepts.
 *
 * @public
 */
export interface DriverWorkerEntry {
  /** The module that the worker imports to rebuild the driver. */
  specifier: string
  /** The name of the factory export that the worker calls, or the default export when unset. */
  exportName?: string
  /** The value that the worker passes to that factory, which has to be a value that `structuredClone` accepts. */
  config?: unknown
}

/** Describes how Sirannon opens SQLite on one runtime.
 * @public
 */
export interface SQLiteDriver {
  /** The features that this driver's runtime supports. */
  readonly capabilities: DriverCapabilities
  /** Opens a database file and returns a connection to it. */
  open(path: string, options?: OpenOptions): Promise<SQLiteConnection>
  /** The entry through which a worker thread rebuilds this driver. */
  readonly worker?: DriverWorkerEntry
  /**
   * Starts a worker thread for writes and returns a connection whose calls run
   * on that thread. Only a driver whose runtime has threads implements this
   * method, so a bundle for a runtime without threads leaves out the worker code.
   */
  startWriterHost?(path: string, options: OpenOptions, hostOptions?: WorkerHostOptions): Promise<SQLiteConnection>
  /** Returns the `WriterContext` that distinguishes the caller holding the writer from a caller waiting for it. */
  createWriterContext?(): WriterContext
  /**
   * Returns the engine that copies a database to a file. Sirannon passes the
   * driver back in, so that the engine can open its own connection for a
   * streamed copy, which loads the extension that moves the bytes.
   */
  createBackupEngine?(driver: SQLiteDriver): BackupEngine
  /**
   * Returns an extension path in absolute form. When `load_extension` receives
   * a bare name, the dynamic linker searches its own paths and can open a
   * different library from the one that the operator named.
   */
  resolveExtensionPath?(extensionPath: string): string
}
