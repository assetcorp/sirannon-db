import { Database } from './database.js'
import type { SQLiteDriver } from './driver/types.js'
import { DatabaseAlreadyExistsError, DatabaseNotFoundError, SirannonError } from './errors.js'
import { HookRegistry } from './hooks/registry.js'
import { LifecycleManager } from './lifecycle/manager.js'
import { MetricsCollector } from './metrics/collector.js'
import type {
  AfterQueryHook,
  BeforeConnectHook,
  BeforeQueryHook,
  DatabaseCloseHook,
  DatabaseOpenHook,
  DatabaseOptions,
  SirannonOptions,
} from './types.js'

export class Sirannon {
  private readonly dbs = new Map<string, Database>()
  private readonly opening = new Set<string>()
  private readonly resolving = new Map<string, Promise<Database | undefined>>()
  private _shutdown = false

  private readonly _driver: SQLiteDriver
  private readonly hookRegistry: HookRegistry
  private readonly metricsCollector: MetricsCollector | null
  private readonly lifecycleManager: LifecycleManager | null

  constructor(readonly options: SirannonOptions) {
    this._driver = options.driver
    this.hookRegistry = new HookRegistry(options.hooks)
    this.metricsCollector = options.metrics ? new MetricsCollector(options.metrics) : null
    this.lifecycleManager = options.lifecycle
      ? new LifecycleManager(options.lifecycle, {
          open: (id, path, opts) => this.open(id, path, opts),
          close: id => this.close(id),
          count: () => this.dbs.size,
          has: id => this.dbs.has(id),
        })
      : null
  }

  get driver(): SQLiteDriver {
    return this._driver
  }

  async open(id: string, path: string, options?: DatabaseOptions): Promise<Database> {
    this.ensureRunning()
    if (this.dbs.has(id) || this.opening.has(id)) {
      throw new DatabaseAlreadyExistsError(id)
    }

    this.opening.add(id)

    let db: Database
    try {
      if (this.hookRegistry.has('beforeConnect')) {
        this.hookRegistry.invokeSync('beforeConnect', { databaseId: id, path })
      }

      db = await Database.create(id, path, this._driver, this.withRegistryDefaults(options), {
        parentHooks: this.hookRegistry,
        metrics: this.metricsCollector ?? undefined,
      })
    } catch (err) {
      this.opening.delete(id)
      if (err instanceof SirannonError) throw err
      throw new SirannonError(
        `Failed to open database '${id}' at '${path}': ${err instanceof Error ? err.message : String(err)}`,
        'DATABASE_OPEN_FAILED',
      )
    }

    try {
      await this.applyRegistryMigrations(db)
    } catch (err) {
      await db.close().catch(() => {})
      if (err instanceof SirannonError) throw err
      throw new SirannonError(
        `Failed to migrate database '${id}' at '${path}': ${err instanceof Error ? err.message : String(err)}`,
        'DATABASE_OPEN_FAILED',
      )
    } finally {
      this.opening.delete(id)
    }

    if (this._shutdown) {
      await db.close().catch(() => {})
      throw new SirannonError('Sirannon has been shut down', 'SHUTDOWN')
    }

    db.addCloseListener(() => {
      this.dbs.delete(id)
      this.lifecycleManager?.untrack(id)

      if (this.hookRegistry.has('databaseClose')) {
        try {
          this.hookRegistry.invokeSync('databaseClose', { databaseId: id, path })
        } catch {
          /* non-fatal */
        }
      }

      this.metricsCollector?.trackConnection({
        databaseId: id,
        path,
        readerCount: 0,
        event: 'close',
      })
    })

    this.dbs.set(id, db)
    this.lifecycleManager?.markActive(id)

    if (this.hookRegistry.has('databaseOpen')) {
      try {
        this.hookRegistry.invokeSync('databaseOpen', { databaseId: id, path })
      } catch {
        /* non-fatal */
      }
    }

    this.metricsCollector?.trackConnection({
      databaseId: id,
      path,
      readerCount: db.readerCount,
      event: 'open',
    })

    return db
  }

  private withRegistryDefaults(options?: DatabaseOptions): DatabaseOptions | undefined {
    const fallback = this.options.writerWorker
    if (fallback === undefined || options?.writerWorker !== undefined) return options
    return { ...options, writerWorker: fallback }
  }

  async close(id: string): Promise<void> {
    this.ensureRunning()
    const db = this.dbs.get(id)
    if (!db) {
      throw new DatabaseNotFoundError(id)
    }
    await db.close()
  }

  get(id: string): Database | undefined {
    const db = this.dbs.get(id)
    if (db) {
      this.lifecycleManager?.markActive(id)
      return db
    }
    if (this._shutdown) return undefined
    return undefined
  }

  async resolve(id: string): Promise<Database | undefined> {
    const db = this.get(id)
    if (db) return db
    if (this._shutdown) return undefined
    const manager = this.lifecycleManager
    if (!manager) return undefined

    const pending = this.resolving.get(id)
    if (pending) return pending

    const inFlight = manager.resolve(id).finally(() => {
      this.resolving.delete(id)
    })
    this.resolving.set(id, inFlight)
    return inFlight
  }

  private async applyRegistryMigrations(db: Database): Promise<void> {
    const migrations = this.options.migrations
    if (!migrations || migrations.length === 0 || db.readOnly) return
    await db.migrate(migrations)
  }

  has(id: string): boolean {
    return this.dbs.has(id)
  }

  databases(): Map<string, Database> {
    return new Map(this.dbs)
  }

  async shutdown(): Promise<void> {
    if (this._shutdown) return
    this._shutdown = true

    this.lifecycleManager?.dispose()

    const errors: unknown[] = []
    const snapshot = [...this.dbs.values()]

    for (const db of snapshot) {
      try {
        await db.close()
      } catch (err) {
        errors.push(err)
      }
    }

    this.dbs.clear()

    if (errors.length > 0) {
      throw new SirannonError(`Shutdown completed with ${errors.length} error(s)`, 'SHUTDOWN_ERROR')
    }
  }

  onBeforeQuery(hook: BeforeQueryHook): void {
    this.hookRegistry.register('beforeQuery', hook)
  }

  onAfterQuery(hook: AfterQueryHook): void {
    this.hookRegistry.register('afterQuery', hook)
  }

  onBeforeConnect(hook: BeforeConnectHook): void {
    this.hookRegistry.register('beforeConnect', hook)
  }

  onDatabaseOpen(hook: DatabaseOpenHook): void {
    this.hookRegistry.register('databaseOpen', hook)
  }

  onDatabaseClose(hook: DatabaseCloseHook): void {
    this.hookRegistry.register('databaseClose', hook)
  }

  private ensureRunning(): void {
    if (this._shutdown) {
      throw new SirannonError('Sirannon has been shut down', 'SHUTDOWN')
    }
  }
}
