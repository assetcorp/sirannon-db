import type { BackupCycle } from '../backup/cycle.js'
import type { BackupCycleRequest } from '../backup/cycle-options.js'
import type { BackupRunReport, BackupRunRequest } from '../backup/report.js'
import type { BackupScheduleOptions } from '../types.js'
import type { WorkerHostOptions } from '../worker/host.js'

/** What one write reports back through the driver.
 * @public
 */
export interface RunResult {
  /** Number of rows the statement inserted, updated, or deleted. */
  changes: number
  /** Row id SQLite assigned to the last inserted row. */
  lastInsertRowId: number | bigint
}

/**
 * Tells a caller running inside the operation that holds the writer from one
 * merely waiting on it. A runtime without async context tracking cannot answer
 * this, and answering it wrongly runs one caller's writes inside another
 * caller's transaction.
 */
export interface WriterContext {
  /** Runs an operation marked as the one holding the writer. */
  run<T>(operation: () => T): T
  /** Reports whether the caller is the operation holding the writer. */
  isActive(): boolean
  /** Runs an operation outside the held writer, so its writes stay out of that transaction. */
  exit<T>(operation: () => T): T
}

/**
 * Copies a database to a file, and repeats that copy on a schedule.
 *
 * @internal
 */
export interface BackupEngine {
  /** Copies the database behind a connection to a destination path. */
  backup(conn: SQLiteConnection, destPath: string, onFirstStep?: () => void): Promise<void>
  /** Copies the database behind a connection to a caller-supplied destination. */
  copyToDestination(conn: SQLiteConnection, request: BackupRunRequest): Promise<BackupRunReport>
  /** Reports whether a full copy reaches the destination without a local file. */
  streamsToDestination(): boolean
  /** Builds the cycle that captures the write-ahead log and then checkpoints it. */
  createCycle(request: BackupCycleRequest): BackupCycle
  /** Starts a repeating backup and returns a function that stops it. */
  schedule(
    conn: SQLiteConnection,
    options: BackupScheduleOptions,
    runExclusive: (op: () => Promise<void>) => Promise<void>,
  ): () => void
}

/** What one step of a stepped database copy reported.
 * @public
 */
export interface DatabaseCopyStep {
  /** Pages the copy has to move in total. */
  totalPages: number
  /** Pages the copy has yet to move. */
  remainingPages: number
}

/** What Sirannon asks a driver to copy, and how finely.
 * @public
 */
export interface DatabaseCopyRequest {
  /** Path the copy is written to. */
  destPath: string
  /** Pages the driver moves in one step. */
  pagesPerStep: number
  /**
   * Called after every step with that step's counters. It returns the pages for
   * the next step, and a driver whose runtime takes a fresh count on each step
   * passes that number on. Zero holds the copy still while the caller catches
   * up.
   */
  onStep?: (step: DatabaseCopyStep) => number
}

/** Totals for one statement applied over many parameter sets.
 * @public
 */
export interface BatchSummary {
  /** Number of parameter sets the driver applied. */
  rowsLoaded: number
  /** Number of rows those statements inserted, updated, or deleted. */
  changes: number
}

/** One prepared statement a driver hands back, ready to run many times.
 * @public
 */
export interface SQLiteStatement {
  /** Runs the statement and returns every row. */
  all<T = unknown>(...params: unknown[]): Promise<T[]>
  /** Runs the statement and returns the first row, or undefined when there is none. */
  get<T = unknown>(...params: unknown[]): Promise<T | undefined>
  /** Runs the statement as a write and reports what it changed. */
  run(...params: unknown[]): Promise<RunResult>
  /**
   * Like {@link SQLiteStatement.all} but skips the safe-range BigInt narrowing, leaving every
   * integer as a BigInt. The server wire path narrows and tags in one pass, so
   * feeding it raw rows avoids a second walk. Optional: a driver that omits it
   * falls back to {@link SQLiteStatement.all}, still correct but with the extra narrowing walk.
   */
  allRaw?<T = unknown>(...params: unknown[]): Promise<T[]>
}

/**
 * Why one unit of a grouped run failed.
 *
 * @internal
 */
export interface GroupRunError {
  /** Message SQLite or the driver raised. */
  message: string
  /** Name of the error class. */
  name?: string
  /** Machine-readable code, where the driver supplies one. */
  code?: string
}

/**
 * What one unit of a grouped run produced.
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
  /** Compiles a statement so the caller can run it many times. */
  prepare(sql: string): Promise<SQLiteStatement>
  /** Runs a function inside one transaction, committing when it returns and rolling back when it throws. */
  transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T>
  /** Closes the connection. */
  close(): Promise<void>
  /**
   * Loads a compiled SQLite extension into this connection through the
   * runtime's own loading call, so a query on this connection can call the
   * extension's functions. SQLite scopes a loaded extension to the connection
   * that loaded it, so a caller that needs it everywhere loads it on every
   * connection. Where the runtime carries no loading call, this rejects with an
   * error that names that runtime.
   */
  loadExtension?(extensionPath: string): Promise<void>
  /**
   * Copies this connection's database to a file through SQLite's stepped
   * backup interface, one step at a time, so writes on this connection run in
   * the gaps between steps. SQLite restarts a copy whose source is written
   * through any other connection, so the copy must run on the connection that
   * writes. A copy started while a transaction is already open on this
   * connection produces nothing, so the caller starts one only when no
   * transaction is open. Where the runtime carries no stepped backup call,
   * this rejects with an error that names that runtime.
   */
  copyDatabase?(request: DatabaseCopyRequest): Promise<DatabaseCopyStep>
  /**
   * Whether the runtime steps this connection's copy on a thread other than the
   * caller's. A copy on its own thread can wait for the caller to catch up, and
   * a copy on the caller's thread would stop the only thread there is.
   */
  readonly copyRunsOffCallerThread?: boolean
  /** Optional fast path that applies one statement over many parameter sets. */
  runBatch?(sql: string, paramsBatch: readonly unknown[][]): Promise<RunResult[]>
  /** Optional fast path that applies one statement over many parameter sets and returns only the totals. */
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
 *
 * @public
 */
export type SynchronousLevel = 'off' | 'normal' | 'full' | 'extra'

/** How a driver opens one database file.
 * @public
 */
export interface OpenOptions {
  /** Opens the file for reads only. */
  readonly?: boolean
  /** Puts the database in write-ahead logging mode. */
  walMode?: boolean
  /** Writer durability the connection runs at. */
  synchronous?: SynchronousLevel
  /**
   * How many frames the write-ahead log may reach before SQLite checkpoints it
   * on its own. Zero turns that off, which a database capturing its own log
   * needs: a checkpoint lets SQLite overwrite frames nothing has captured yet.
   *
   * SQLite holds this per connection, so a driver applies it every time it
   * opens one, including after a writer worker restarts.
   */
  walAutoCheckpoint?: number
}

/** What a driver's runtime supports.
 * @public
 */
export interface DriverCapabilities {
  /** Whether the runtime opens more than one connection to the same file. */
  multipleConnections: boolean
  /** Whether the runtime loads SQLite extensions. */
  extensions: boolean
  /** Whether the runtime copies an open database through SQLite's stepped backup interface. */
  steppedCopy: boolean
}

/**
 * Lets a worker thread rebuild the driver, since the driver's `open` function
 * cannot cross the thread boundary. `specifier` must be importable from the
 * worker and `config` must survive a structured clone; the worker imports the
 * module and calls its `exportName` factory (default export otherwise) with it.
 *
 * @public
 */
export interface DriverWorkerEntry {
  /** Module the worker imports to rebuild the driver. */
  specifier: string
  /** Named export the worker calls, or the default export when absent. */
  exportName?: string
  /** Value passed to that factory, which must survive a structured clone. */
  config?: unknown
}

/** How Sirannon opens SQLite on one runtime.
 * @public
 */
export interface SQLiteDriver {
  /** What this driver's runtime supports. */
  readonly capabilities: DriverCapabilities
  /** Opens a database file and returns a connection to it. */
  open(path: string, options?: OpenOptions): Promise<SQLiteConnection>
  /** How a worker thread rebuilds this driver. */
  readonly worker?: DriverWorkerEntry
  /**
   * Offloads writes to a worker thread. Only a driver whose runtime has
   * threads implements this, which is what keeps the thread machinery out of
   * bundles built for runtimes that do not.
   */
  startWriterHost?(path: string, options: OpenOptions, hostOptions?: WorkerHostOptions): Promise<SQLiteConnection>
  /** Builds the tracker that tells the caller holding the writer from one waiting on it. */
  createWriterContext?(): WriterContext
  /**
   * Builds the engine that copies a database to a file. The driver is passed
   * back so the engine can open a connection of its own, which a streamed copy
   * needs for the extension that carries the bytes.
   */
  createBackupEngine?(driver: SQLiteDriver): BackupEngine
  /**
   * Makes an extension path absolute. Passing a bare name to `load_extension`
   * would let the dynamic linker search its own paths and open a different
   * library than the operator named.
   */
  resolveExtensionPath?(extensionPath: string): string
}
