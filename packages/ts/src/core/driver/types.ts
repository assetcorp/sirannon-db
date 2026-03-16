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

export interface OpenOptions {
  readonly?: boolean
  walMode?: boolean
}

export interface DriverCapabilities {
  multipleConnections: boolean
  extensions: boolean
}

export interface SQLiteDriver {
  readonly capabilities: DriverCapabilities
  open(path: string, options?: OpenOptions): Promise<SQLiteConnection>
}
