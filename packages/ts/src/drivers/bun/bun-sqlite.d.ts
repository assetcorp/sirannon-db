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
    readonly columnNames: string[]
    readonly declaredTypes?: (string | null)[] | null
    all(...params: unknown[]): Row[]
    get(...params: unknown[]): Row | null
    run(...params: unknown[]): BunSqliteChanges
    values(...params: unknown[]): unknown[][]
    iterate(...params: unknown[]): IterableIterator<Record<string, unknown>>
    finalize(): void
  }

  export class Database {
    constructor(filename: string, options?: BunSqliteOpenOptions)
    query<Row = unknown>(sql: string): Statement<Row>
    run(sql: string, ...params: unknown[]): BunSqliteChanges
    loadExtension(path: string, entryPoint?: string): void
    close(throwOnError?: boolean): void
  }
}
