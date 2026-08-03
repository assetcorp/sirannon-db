declare module 'bun:sqlite' {
  export interface BunSqliteChanges {
    changes: number
    lastInsertRowid: number | bigint
  }

  export interface BunSqliteOpenOptions {
    readonly?: boolean
    create?: boolean
    readwrite?: boolean
    safeIntegers?: boolean
    strict?: boolean
  }

  export class Statement<Row = unknown> {
    all(...params: unknown[]): Row[]
    get(...params: unknown[]): Row | null
    run(...params: unknown[]): BunSqliteChanges
    finalize(): void
  }

  export class Database {
    constructor(filename: string, options?: BunSqliteOpenOptions)
    query<Row = unknown>(sql: string): Statement<Row>
    run(sql: string, ...params: unknown[]): BunSqliteChanges
    close(throwOnError?: boolean): void
  }
}
