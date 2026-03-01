import { ConnectionPool } from './connection-pool.js'
import { SirannonError } from './errors.js'
import { MigrationRunner } from './migrations/runner.js'
import { execute, executeBatch, query, queryOne } from './query-executor.js'
import { Transaction } from './transaction.js'
import type {
  AfterQueryHook,
  BackupScheduleOptions,
  BeforeQueryHook,
  DatabaseOptions,
  ExecuteResult,
  MigrationResult,
  Params,
  SubscriptionBuilder,
} from './types.js'

export class Database {
  readonly id: string
  readonly path: string
  readonly readOnly: boolean
  private readonly pool: ConnectionPool
  private readonly closeListeners: (() => void)[] = []
  private _closed = false

  constructor(id: string, path: string, options?: DatabaseOptions) {
    this.id = id
    this.path = path
    this.readOnly = options?.readOnly ?? false

    this.pool = new ConnectionPool({
      path,
      readOnly: this.readOnly,
      readPoolSize: options?.readPoolSize ?? 4,
      walMode: options?.walMode ?? true,
    })
  }

  query<T = Record<string, unknown>>(sql: string, params?: Params): T[] {
    this.ensureOpen()
    const reader = this.pool.acquireReader()
    return query<T>(reader, sql, params)
  }

  queryOne<T = Record<string, unknown>>(sql: string, params?: Params): T | undefined {
    this.ensureOpen()
    const reader = this.pool.acquireReader()
    return queryOne<T>(reader, sql, params)
  }

  execute(sql: string, params?: Params): ExecuteResult {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    return execute(writer, sql, params)
  }

  executeBatch(sql: string, paramsBatch: Params[]): ExecuteResult[] {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    return executeBatch(writer, sql, paramsBatch)
  }

  transaction<T>(fn: (tx: Transaction) => T): T {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    return Transaction.run(writer, fn)
  }

  watch(_table: string): void {
    throw new Error('not implemented')
  }

  unwatch(_table: string): void {
    throw new Error('not implemented')
  }

  on(_table: string): SubscriptionBuilder {
    throw new Error('not implemented')
  }

  migrate(migrationsPath: string): MigrationResult {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    return MigrationRunner.run(writer, migrationsPath)
  }

  backup(_destPath: string): void {
    throw new Error('not implemented')
  }

  scheduleBackup(_options: BackupScheduleOptions): void {
    throw new Error('not implemented')
  }

  loadExtension(_path: string): void {
    throw new Error('not implemented')
  }

  onBeforeQuery(_hook: BeforeQueryHook): void {
    throw new Error('not implemented')
  }

  onAfterQuery(_hook: AfterQueryHook): void {
    throw new Error('not implemented')
  }

  addCloseListener(fn: () => void): void {
    this.ensureOpen()
    this.closeListeners.push(fn)
  }

  close(): void {
    if (this._closed) return
    this._closed = true

    let poolError: unknown
    try {
      this.pool.close()
    } catch (err) {
      poolError = err
    }

    for (const fn of this.closeListeners) {
      try {
        fn()
      } catch {
        // Listener errors are secondary to pool close
      }
    }

    if (poolError) {
      throw poolError
    }
  }

  get closed(): boolean {
    return this._closed
  }

  get readerCount(): number {
    return this.pool.readerCount
  }

  private ensureOpen(): void {
    if (this._closed) {
      throw new SirannonError(`Database '${this.id}' is closed`, 'DATABASE_CLOSED')
    }
  }
}
