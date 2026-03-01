import { Database } from './database.js'
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
  private _shutdown = false

  private readonly hookRegistry: HookRegistry
  private readonly metricsCollector: MetricsCollector | null
  private readonly lifecycleManager: LifecycleManager | null

  constructor(readonly options?: SirannonOptions) {
    this.hookRegistry = new HookRegistry(options?.hooks)
    this.metricsCollector = options?.metrics ? new MetricsCollector(options.metrics) : null
    this.lifecycleManager = options?.lifecycle
      ? new LifecycleManager(options.lifecycle, {
          open: (id, path, opts) => this.open(id, path, opts),
          close: id => this.close(id),
          count: () => this.dbs.size,
          has: id => this.dbs.has(id),
        })
      : null
  }

  open(id: string, path: string, options?: DatabaseOptions): Database {
    this.ensureRunning()
    if (this.dbs.has(id)) {
      throw new DatabaseAlreadyExistsError(id)
    }

    if (this.hookRegistry.has('beforeConnect')) {
      this.hookRegistry.invokeSync('beforeConnect', { databaseId: id, path })
    }

    let db: Database
    try {
      db = new Database(id, path, options, {
        parentHooks: this.hookRegistry,
        metrics: this.metricsCollector ?? undefined,
      })
    } catch (err) {
      if (err instanceof SirannonError) throw err
      throw new SirannonError(
        `Failed to open database '${id}' at '${path}': ${err instanceof Error ? err.message : String(err)}`,
        'DATABASE_OPEN_FAILED',
      )
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

  close(id: string): void {
    this.ensureRunning()
    const db = this.dbs.get(id)
    if (!db) {
      throw new DatabaseNotFoundError(id)
    }
    db.close()
  }

  get(id: string): Database | undefined {
    const db = this.dbs.get(id)
    if (db) {
      this.lifecycleManager?.markActive(id)
      return db
    }
    if (this._shutdown) return undefined
    return this.lifecycleManager?.resolve(id)
  }

  has(id: string): boolean {
    return this.dbs.has(id)
  }

  databases(): Map<string, Database> {
    return new Map(this.dbs)
  }

  shutdown(): void {
    if (this._shutdown) return
    this._shutdown = true

    this.lifecycleManager?.dispose()

    const errors: unknown[] = []
    const snapshot = [...this.dbs.values()]

    for (const db of snapshot) {
      try {
        db.close()
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
