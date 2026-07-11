import { runBulkLoad } from './bulk-load.js'
import { CdcAwareTransaction, type CdcTransactionState } from './cdc/cdc-aware-transaction.js'
import { ChangeTracker } from './cdc/change-tracker.js'
import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import { SubscriptionBuilderImpl, SubscriptionManager, startPolling } from './cdc/subscription.js'
import { ConnectionPool } from './connection-pool.js'
import { DatabaseBackupController } from './database-backup.js'
import { DEFAULT_SYNCHRONOUS } from './driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SynchronousLevel } from './driver/types.js'
import { ReadOnlyError, SirannonError } from './errors.js'
import { loadExtension as loadExtensionImpl } from './extension-loader.js'
import { fireAfterQueryHooks, fireBeforeQueryHooks } from './hooks/query-hooks.js'
import { HookRegistry } from './hooks/registry.js'
import type { MetricsCollector } from './metrics/collector.js'
import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { execute, executeBatch, executeBatchSummary, query, queryForWire, queryOne } from './query-executor.js'
import { Transaction } from './transaction.js'
import type {
  AfterQueryHook,
  BackupScheduleOptions,
  BeforeQueryHook,
  BulkLoadOptions,
  BulkLoadResult,
  DatabaseOptions,
  ExecuteResult,
  Params,
  QueryOptions,
  SubscriptionBuilder,
} from './types.js'
import { WriterLock } from './writer-lock.js'

export interface DatabaseInternals {
  parentHooks?: HookRegistry
  metrics?: MetricsCollector
}

export class Database {
  readonly id: string
  readonly path: string
  readonly readOnly: boolean
  private readonly pool: ConnectionPool
  private readonly driver: SQLiteDriver
  private readonly synchronous: SynchronousLevel
  private readonly walMode: boolean
  private readonly writerLock = new WriterLock()
  private readonly closeListeners: (() => void | Promise<void>)[] = []
  private _closed = false

  private changeTracker: ChangeTracker | null = null
  private subscriptionManager: SubscriptionManager | null = null
  private stopCdcPolling: (() => void) | null = null
  private readonly cdcPollInterval: number
  private readonly cdcRetention: number

  private readonly hookRegistry = new HookRegistry()
  private readonly parentHooks: HookRegistry | null
  private readonly metricsCollector: MetricsCollector | null

  private readonly backups = new DatabaseBackupController(
    op => this.writerLock.run(op),
    () => this.pool.acquireWriter(),
  )

  private constructor(
    id: string,
    path: string,
    pool: ConnectionPool,
    driver: SQLiteDriver,
    options?: DatabaseOptions,
    internals?: DatabaseInternals,
  ) {
    this.id = id
    this.path = path
    this.pool = pool
    this.driver = driver
    this.readOnly = options?.readOnly ?? false
    this.synchronous = options?.synchronous ?? DEFAULT_SYNCHRONOUS
    this.walMode = options?.walMode ?? true
    this.cdcPollInterval = options?.cdcPollInterval ?? 50
    this.cdcRetention = options?.cdcRetention ?? 3_600_000
    this.parentHooks = internals?.parentHooks ?? null
    this.metricsCollector = internals?.metrics ?? null
  }

  static async create(
    id: string,
    path: string,
    driver: SQLiteDriver,
    options?: DatabaseOptions,
    internals?: DatabaseInternals,
  ): Promise<Database> {
    const pool = await ConnectionPool.create({
      driver,
      path,
      readOnly: options?.readOnly,
      readPoolSize: options?.readPoolSize ?? 4,
      walMode: options?.walMode ?? true,
      synchronous: options?.synchronous,
    })

    return new Database(id, path, pool, driver, options, internals)
  }

  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql, params, options)

    const start = performance.now()
    try {
      return await this.runRead(sql, conn => query<T>(conn, sql, params))
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  /**
   * Returns query rows already encoded for the wire (safe-range integers as
   * plain numbers, larger integers and BLOBs as tagged envelopes) in a single
   * pass. The server response path uses this so a read walks its values once
   * rather than narrowing on the driver and re-scanning to tag.
   */
  async queryForWire(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql, params, options)

    const start = performance.now()
    try {
      return await this.runRead(sql, conn => queryForWire(conn, sql, params))
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  async queryOne<T = Record<string, unknown>>(
    sql: string,
    params?: Params,
    options?: QueryOptions,
  ): Promise<T | undefined> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql, params, options)

    const start = performance.now()
    try {
      return await this.runRead(sql, conn => queryOne<T>(conn, sql, params))
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  /**
   * On a single-connection driver the reader is the writer, so serialise the
   * read through the writer lock to avoid a bulk load's uncommitted rows.
   */
  private runRead<T>(sql: string, op: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
    if (this.pool.readerCount === 0) {
      return this.writerLock.run(() => this.track(sql, () => op(this.pool.acquireWriter())))
    }
    return this.track(sql, () => op(this.pool.acquireReader()))
  }

  async execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    this.fireBeforeQueryHooks(sql, params, options)

    const start = performance.now()
    try {
      return await this.writerLock.run(async () => {
        const writer = this.pool.acquireWriter()
        const result = await this.track(sql, () => execute(writer, sql, params))
        await applyDdlSideEffectsIfRelevant(this.changeTracker, writer, sql)
        return result
      })
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  async executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    this.fireBeforeQueryHooks(sql, undefined, options)

    const start = performance.now()
    try {
      return await this.writerLock.run(() =>
        this.runInTransaction(this.pool.acquireWriter(), sql, txConn => executeBatch(txConn, sql, paramsBatch)),
      )
    } finally {
      this.fireAfterQueryHooks(sql, undefined, performance.now() - start)
    }
  }

  private track<T>(sql: string, operation: () => Promise<T>): Promise<T> {
    if (!this.metricsCollector) return operation()
    return this.metricsCollector.trackQuery(operation, { databaseId: this.id, sql })
  }

  private async runInTransaction<T>(
    writer: SQLiteConnection,
    sql: string,
    run: (txConn: SQLiteConnection) => Promise<T>,
  ): Promise<T> {
    const result = await this.track(sql, () => writer.transaction(run))
    await applyDdlSideEffectsIfRelevant(this.changeTracker, writer, sql)
    return result
  }

  /**
   * Load rows with relaxed writer durability. The load holds the writer lock
   * for its whole duration, so no other write commits under the relaxed level
   * and no two loads race on the shared `synchronous` setting; the configured
   * level is restored before this resolves, whether the load succeeds or
   * fails. The load runs in one transaction, so one commit and one durability
   * barrier cover the whole batch. Rows are summed rather than returned
   * per-row to bound memory on large loads. Like `execute` and `transaction`,
   * this writes only to the local database; under replication the server routes
   * loads through the engine, not through this method.
   */
  async bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    this.fireBeforeQueryHooks(sql, undefined, undefined)

    const start = performance.now()
    try {
      return await this.writerLock.run(() => {
        const writer = this.pool.acquireWriter()
        return runBulkLoad({
          writer,
          configuredSynchronous: this.synchronous,
          walMode: this.walMode,
          durability: options?.durability,
          loadRows: () => this.runInTransaction(writer, sql, txConn => executeBatchSummary(txConn, sql, paramsBatch)),
        })
      })
    } finally {
      this.fireAfterQueryHooks(sql, undefined, performance.now() - start)
    }
  }

  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)

    return this.writerLock.run(async () => {
      const writer = this.pool.acquireWriter()
      const tracker = this.changeTracker

      if (!tracker) {
        return Transaction.run(writer, fn)
      }

      const state: CdcTransactionState = { sawDdl: false, droppedTables: [] }
      const result = await writer.transaction(async txConn => {
        const tx = new CdcAwareTransaction(txConn, tracker, state)
        return fn(tx)
      })

      if (state.sawDdl && state.droppedTables.length > 0) {
        await tracker.pruneDroppedTables(writer, state.droppedTables)
      }

      return result
    })
  }

  async watch(table: string): Promise<void> {
    this.ensureOpen()
    if (this.readOnly) {
      throw new ReadOnlyError(this.id)
    }

    this.ensureCdc()
    const tracker = this.changeTracker
    await this.writerLock.run(() => tracker?.watch(this.pool.acquireWriter(), table) ?? Promise.resolve())
    this.ensureCdcPolling()
  }

  /**
   * Runs a CDC maintenance write (change-log pruning) on the shared writer
   * under the writer lock. Serialising it with application writes keeps it
   * from becoming a second writer that contends for SQLite's single write
   * lock and stalls the event loop on `busy_timeout`.
   */
  async runCdcMaintenance(op: (writer: SQLiteConnection) => Promise<unknown>): Promise<void> {
    if (this._closed) return
    await this.writerLock.run(() => op(this.pool.acquireWriter()))
  }

  async unwatch(table: string): Promise<void> {
    this.ensureOpen()
    const tracker = this.changeTracker
    if (!tracker) return

    await this.writerLock.run(() => tracker.unwatch(this.pool.acquireWriter(), table))

    if (tracker.watchedTables.size === 0) {
      this.stopCdcPollingLoop()
    }
  }

  on(table: string): SubscriptionBuilder {
    this.ensureOpen()
    this.ensureCdc()
    const manager = this.subscriptionManager
    if (!manager) throw new Error('subscriptionManager not initialized')
    return new SubscriptionBuilderImpl(table, manager)
  }

  async migrate(migrations: Migration[]): Promise<MigrationResult> {
    this.ensureOpen()
    return this.writerLock.run(() => MigrationRunner.run(this.pool.acquireWriter(), migrations))
  }

  async rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    this.ensureOpen()
    return this.writerLock.run(() => MigrationRunner.rollback(this.pool.acquireWriter(), migrations, version))
  }

  async backup(destPath: string): Promise<void> {
    this.ensureOpen()
    await this.backups.backup(destPath)
  }

  scheduleBackup(options: BackupScheduleOptions): void {
    this.ensureOpen()
    this.backups.schedule(options)
  }

  async loadExtension(extensionPath: string): Promise<void> {
    this.ensureOpen()
    await this.writerLock.run(() => loadExtensionImpl(this.driver, this.pool.acquireWriter(), extensionPath))
  }

  onBeforeQuery(hook: BeforeQueryHook): void {
    this.hookRegistry.register('beforeQuery', hook)
  }

  onAfterQuery(hook: AfterQueryHook): void {
    this.hookRegistry.register('afterQuery', hook)
  }

  addCloseListener(fn: () => void | Promise<void>): void {
    this.ensureOpen()
    this.closeListeners.push(fn)
  }

  async close(): Promise<void> {
    if (this._closed) return
    this._closed = true

    this.stopCdcPollingLoop()
    this.backups.cancelAll()

    let poolError: unknown
    try {
      await this.pool.close()
    } catch (err) {
      poolError = err
    }

    for (const fn of this.closeListeners) {
      try {
        await fn()
      } catch {
        /* listener errors are secondary to pool close */
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

  private ensureCdc(): void {
    if (!this.changeTracker) {
      this.changeTracker = new ChangeTracker({ retention: this.cdcRetention })
    }
    if (!this.subscriptionManager) {
      this.subscriptionManager = new SubscriptionManager()
    }
  }

  private ensureCdcPolling(): void {
    if (this.stopCdcPolling) return
    if (!this.changeTracker || !this.subscriptionManager) return

    const writer = this.pool.acquireWriter()
    this.stopCdcPolling = startPolling(
      writer,
      this.changeTracker,
      this.subscriptionManager,
      this.cdcPollInterval,
      undefined,
      operation => this.writerLock.run(operation),
    )
  }

  private stopCdcPollingLoop(): void {
    if (this.stopCdcPolling) {
      this.stopCdcPolling()
      this.stopCdcPolling = null
    }
  }

  private fireBeforeQueryHooks(sql: string, params?: Params, options?: QueryOptions): void {
    fireBeforeQueryHooks(this.parentHooks, this.hookRegistry, this.id, sql, params, options)
  }

  private fireAfterQueryHooks(sql: string, params: Params | undefined, durationMs: number): void {
    fireAfterQueryHooks(this.parentHooks, this.hookRegistry, this.id, sql, params, durationMs)
  }
}
