import { BackupManager } from './backup/backup.js'
import { BackupScheduler } from './backup/scheduler.js'
import { ChangeTracker } from './cdc/change-tracker.js'
import { SubscriptionBuilderImpl, SubscriptionManager, startPolling } from './cdc/subscription.js'
import { ConnectionPool } from './connection-pool.js'
import type { SQLiteDriver } from './driver/types.js'
import { ExtensionError, ReadOnlyError, SirannonError } from './errors.js'
import { HookRegistry } from './hooks/registry.js'
import type { MetricsCollector } from './metrics/collector.js'
import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { execute, executeBatch, query, queryOne } from './query-executor.js'
import { Transaction } from './transaction.js'
import type {
  AfterQueryHook,
  BackupScheduleOptions,
  BeforeQueryHook,
  DatabaseOptions,
  ExecuteResult,
  Params,
  QueryHookContext,
  SubscriptionBuilder,
} from './types.js'

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

  private readonly backupManager = new BackupManager()
  private readonly backupScheduler = new BackupScheduler(this.backupManager)
  private readonly scheduledBackupCancellers: (() => void)[] = []

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
    })

    return new Database(id, path, pool, driver, options, internals)
  }

  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql, params)

    const start = performance.now()
    try {
      const reader = this.pool.acquireReader()
      if (this.metricsCollector) {
        return await this.metricsCollector.trackQuery(() => query<T>(reader, sql, params), {
          databaseId: this.id,
          sql,
        })
      }
      return await query<T>(reader, sql, params)
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  async queryOne<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T | undefined> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql, params)

    const start = performance.now()
    try {
      const reader = this.pool.acquireReader()
      if (this.metricsCollector) {
        return await this.metricsCollector.trackQuery(() => queryOne<T>(reader, sql, params), {
          databaseId: this.id,
          sql,
        })
      }
      return await queryOne<T>(reader, sql, params)
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql, params)

    const start = performance.now()
    try {
      const writer = this.pool.acquireWriter()
      if (this.metricsCollector) {
        return await this.metricsCollector.trackQuery(() => execute(writer, sql, params), {
          databaseId: this.id,
          sql,
        })
      }
      return await execute(writer, sql, params)
    } finally {
      this.fireAfterQueryHooks(sql, params, performance.now() - start)
    }
  }

  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    this.ensureOpen()
    this.fireBeforeQueryHooks(sql)

    const start = performance.now()
    try {
      const writer = this.pool.acquireWriter()
      const batchFn = () => writer.transaction(async txConn => executeBatch(txConn, sql, paramsBatch))
      if (this.metricsCollector) {
        return await this.metricsCollector.trackQuery(batchFn, {
          databaseId: this.id,
          sql,
        })
      }
      return await batchFn()
    } finally {
      this.fireAfterQueryHooks(sql, undefined, performance.now() - start)
    }
  }

  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    return Transaction.run(writer, fn)
  }

  async watch(table: string): Promise<void> {
    this.ensureOpen()
    if (this.readOnly) {
      throw new ReadOnlyError(this.id)
    }

    this.ensureCdc()
    const writer = this.pool.acquireWriter()
    await this.changeTracker?.watch(writer, table)
    this.ensureCdcPolling()
  }

  async unwatch(table: string): Promise<void> {
    this.ensureOpen()
    if (!this.changeTracker) return

    const writer = this.pool.acquireWriter()
    await this.changeTracker.unwatch(writer, table)

    if (this.changeTracker.watchedTables.size === 0) {
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
    const writer = this.pool.acquireWriter()
    return MigrationRunner.run(writer, migrations)
  }

  async rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    return MigrationRunner.rollback(writer, migrations, version)
  }

  async backup(destPath: string): Promise<void> {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    await this.backupManager.backup(writer, destPath)
  }

  scheduleBackup(options: BackupScheduleOptions): void {
    this.ensureOpen()
    const writer = this.pool.acquireWriter()
    const cancel = this.backupScheduler.schedule(writer, options)
    this.scheduledBackupCancellers.push(cancel)
  }

  async loadExtension(extensionPath: string): Promise<void> {
    this.ensureOpen()

    if (!this.driver.capabilities.extensions) {
      throw new ExtensionError(extensionPath, 'Extensions are not supported by the current driver')
    }

    if (!extensionPath || extensionPath.includes('\0')) {
      throw new ExtensionError(extensionPath || '', 'Extension path is empty or contains null bytes')
    }

    const segments = extensionPath.split(/[/\\]/)
    if (segments.includes('..')) {
      throw new ExtensionError(extensionPath, 'Extension path must not contain directory traversal segments')
    }

    try {
      const writer = this.pool.acquireWriter()
      const escaped = extensionPath.replace(/'/g, "''")
      await writer.exec(`SELECT load_extension('${escaped}')`)
    } catch (err) {
      throw new ExtensionError(extensionPath, err instanceof Error ? err.message : String(err))
    }
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

    for (const cancel of this.scheduledBackupCancellers) {
      try {
        cancel()
      } catch {
        /* best-effort */
      }
    }
    this.scheduledBackupCancellers.length = 0

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
    this.stopCdcPolling = startPolling(writer, this.changeTracker, this.subscriptionManager, this.cdcPollInterval)
  }

  private stopCdcPollingLoop(): void {
    if (this.stopCdcPolling) {
      this.stopCdcPolling()
      this.stopCdcPolling = null
    }
  }

  private fireBeforeQueryHooks(sql: string, params?: Params): void {
    const hasParent = this.parentHooks?.has('beforeQuery')
    const hasLocal = this.hookRegistry.has('beforeQuery')
    if (!hasParent && !hasLocal) return

    const ctx: QueryHookContext = { databaseId: this.id, sql, params }
    this.parentHooks?.invokeSync('beforeQuery', ctx)
    this.hookRegistry.invokeSync('beforeQuery', ctx)
  }

  private fireAfterQueryHooks(sql: string, params: Params | undefined, durationMs: number): void {
    const hasParent = this.parentHooks?.has('afterQuery')
    const hasLocal = this.hookRegistry.has('afterQuery')
    if (!hasParent && !hasLocal) return

    const ctx = { databaseId: this.id, sql, params, durationMs }
    try {
      this.parentHooks?.invokeSync('afterQuery', ctx)
    } catch {
      /* non-fatal */
    }
    try {
      this.hookRegistry.invokeSync('afterQuery', ctx)
    } catch {
      /* non-fatal */
    }
  }
}
