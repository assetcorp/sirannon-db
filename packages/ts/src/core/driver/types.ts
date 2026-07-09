export interface RunResult {
  changes: number
  lastInsertRowId: number | bigint
}

export interface SQLiteStatement {
  all<T = unknown>(...params: unknown[]): Promise<T[]>
  get<T = unknown>(...params: unknown[]): Promise<T | undefined>
  run(...params: unknown[]): Promise<RunResult>
}

export interface SQLiteConnection {
  exec(sql: string): Promise<void>
  prepare(sql: string): Promise<SQLiteStatement>
  transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T>
  close(): Promise<void>
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

export interface SQLiteDriver {
  readonly capabilities: DriverCapabilities
  open(path: string, options?: OpenOptions): Promise<SQLiteConnection>
}
