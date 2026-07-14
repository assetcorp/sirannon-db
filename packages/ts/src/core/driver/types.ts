export interface RunResult {
  changes: number
  lastInsertRowId: number | bigint
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

export interface SQLiteConnection {
  exec(sql: string): Promise<void>
  prepare(sql: string): Promise<SQLiteStatement>
  transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T>
  close(): Promise<void>
  runBatch?(sql: string, paramsBatch: readonly unknown[][]): Promise<RunResult[]>
  runBatchSummary?(sql: string, paramsBatch: readonly unknown[][]): Promise<BatchSummary>
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
}
