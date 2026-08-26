import { Database } from './database.js'
import type { SQLiteDriver } from './driver/types.js'
import { DatabaseAlreadyExistsError, DatabaseNotFoundError, ReadOnlyError, SirannonError } from './errors.js'
import { HookRegistry } from './hooks/registry.js'
import type { HookDispose } from './hooks/types.js'
import { LifecycleManager } from './lifecycle/manager.js'
import { MetricsCollector } from './metrics/collector.js'
import { RegistryMigrationSet } from './migrations/registry-set.js'
import type { Migration } from './migrations/types.js'
import { type OfflineOutcome, takeDatabaseOffline } from './sirannon-offline.js'
import { closeEveryDatabase } from './sirannon-shutdown.js'
import type {
  AfterQueryHook,
  BeforeConnectHook,
  BeforeQueryHook,
  DatabaseCloseHook,
  DatabaseOpenHook,
  DatabaseOptions,
  SirannonOptions,
} from './types.js'

/**
 * A registry of open SQLite databases, keyed by identifier.
 *
 * It opens each database through one driver, applies shared hooks, metrics, and migrations, and closes idle databases when you configure a lifecycle.
 *
 * @public
 */
export class Sirannon {
  private readonly dbs = new Map<string, Database>()
  private readonly openedWith = new Map<string, { path: string; options?: DatabaseOptions }>()
  private readonly offline = new Set<string>()
  private readonly offlineWork = new Set<Promise<unknown>>()
  private readonly opening = new Set<string>()
  private readonly resolving = new Map<string, Promise<Database | undefined>>()
  private _shutdown = false

  private readonly _driver: SQLiteDriver
  private readonly _hookRegistry: HookRegistry
  private readonly metricsCollector: MetricsCollector | null
  private readonly lifecycleManager: LifecycleManager | null
  private readonly migrations: RegistryMigrationSet

  /** The driver, hooks, metrics, lifecycle, migrations, and writer-worker default this registry was built with. */
  readonly options: SirannonOptions

  /**
   * Builds a registry.
   *
   * @param options - Driver, hooks, metrics, lifecycle, migrations, and the writer-worker default.
   */
  constructor(options: SirannonOptions) {
    this.options = options
    this._driver = options.driver
    this._hookRegistry = new HookRegistry(options.hooks)
    this.migrations = new RegistryMigrationSet(options.migrations)
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

  /** @internal */
  get driver(): SQLiteDriver {
    return this._driver
  }

  /** @internal */
  get hookRegistry(): HookRegistry {
    return this._hookRegistry
  }

  /**
   * Opens a database and registers it under an identifier.
   *
   * @param id - Identifier callers reach this database by.
   * @param path - File path of the SQLite database.
   * @param options - Pool size, journal mode, durability, and change-capture settings.
   * @returns The open database.
   * @throws When the identifier is already registered.
   */
  async open(id: string, path: string, options?: DatabaseOptions): Promise<Database> {
    this.ensureOpenAllowed(id)
    if (this.dbs.has(id) || this.opening.has(id)) {
      throw new DatabaseAlreadyExistsError(id)
    }

    this.opening.add(id)

    const resolvedOptions = this.withRegistryDefaults(options)

    let db: Database
    try {
      if (this._hookRegistry.has('beforeConnect')) {
        this._hookRegistry.invokeSync('beforeConnect', { databaseId: id, path })
      }

      db = await Database.create(id, path, this._driver, resolvedOptions, {
        parentHooks: this._hookRegistry,
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
      await this.migrations.applyTo(db)
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

    if (this._shutdown && !this.offline.has(id)) {
      await db.close().catch(() => {})
      throw new SirannonError('Sirannon has been shut down', 'SHUTDOWN')
    }

    db.addCloseListener(() => {
      this.dbs.delete(id)
      this.openedWith.delete(id)
      this.lifecycleManager?.untrack(id)

      if (this._hookRegistry.has('databaseClose')) {
        try {
          this._hookRegistry.invokeSync('databaseClose', { databaseId: id, path })
        } catch {}
      }

      this.metricsCollector?.trackConnection({
        databaseId: id,
        path,
        readerCount: 0,
        event: 'close',
      })
    })

    this.dbs.set(id, db)
    this.openedWith.set(id, resolvedOptions === undefined ? { path } : { path, options: resolvedOptions })
    this.lifecycleManager?.markActive(id)

    if (this._hookRegistry.has('databaseOpen')) {
      try {
        this._hookRegistry.invokeSync('databaseOpen', { databaseId: id, path })
      } catch {}
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

  /**
   * Closes one database and removes it from the registry.
   *
   * @param id - Identifier of the database to close.
   */
  async close(id: string): Promise<void> {
    this.ensureRunning()
    const db = this.dbs.get(id)
    if (!db) {
      throw new DatabaseNotFoundError(id)
    }
    await db.close()
  }

  /**
   * Closes one database, calls an action against the file behind it, and then
   * opens that database again under the same identifier with the settings it
   * had before.
   *
   * A restore rebuilds a database at the path it already occupies, so no
   * connection may be open on that file while Sirannon replaces its bytes. The
   * identifier answers nothing until that database is open again, though an
   * action that failed still leaves a database open at the end.
   *
   * @param id - Identifier of the database to take offline.
   * @param action - Runs with the database closed, and receives its file path.
   * @returns Whether the action returned, what it produced or threw, and what a failed reopen threw.
   * @throws When no database is open under the identifier, when it refuses writes, or when the close fails.
   *
   * @internal
   */
  async withDatabaseOffline<T>(id: string, action: (path: string) => Promise<T>): Promise<OfflineOutcome<T>> {
    this.ensureRunning()
    const db = this.dbs.get(id)
    const opened = this.openedWith.get(id)
    if (!db || !opened) throw new DatabaseNotFoundError(id)
    if (db.readOnly) throw new ReadOnlyError(id)

    this.dbs.delete(id)
    this.opening.add(id)
    this.offline.add(id)

    const work = takeDatabaseOffline({
      database: db,
      path: opened.path,
      action,
      reopen: async () => {
        this.opening.delete(id)
        try {
          return await this.open(id, opened.path, opened.options)
        } finally {
          this.offline.delete(id)
        }
      },
    }).finally(() => {
      this.opening.delete(id)
      this.offline.delete(id)
      this.offlineWork.delete(work)
    })
    this.offlineWork.add(work)
    return work
  }

  /**
   * Returns an already-open database.
   *
   * @param id - Identifier of the database.
   * @returns The database, or undefined when none is open under that identifier.
   */
  get(id: string): Database | undefined {
    const db = this.dbs.get(id)
    if (db) {
      this.lifecycleManager?.markActive(id)
      return db
    }
    if (this._shutdown) return undefined
    return undefined
  }

  /**
   * Returns an open database, opening it through the lifecycle resolver where it is not open yet.
   *
   * This is the call an in-process application makes for a database the registry opens on first
   * use through the `lifecycle.autoOpen` resolver, such as one file per tenant. Concurrent calls
   * for the same unopened identifier share one open, so each receives the same database or the
   * same error.
   *
   * @param id - Identifier of the database.
   * @returns The database, or undefined when none is open and the resolver names no path for it.
   */
  async resolve(id: string): Promise<Database | undefined> {
    const db = this.get(id)
    if (db) return db
    if (this._shutdown || this.offline.has(id)) return undefined
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

  /**
   * Returns the registry migration set every database applies as it opens.
   *
   * Where the set came from a function, the registry calls that function once and
   * caches what it returned, so this reports the same list on every call.
   *
   * @returns The migrations declared in `SirannonOptions.migrations`, or an empty list where none are.
   */
  registryMigrations(): Promise<Migration[]> {
    return this.migrations.load()
  }

  /**
   * Reports whether a database is open under an identifier.
   *
   * @param id - Identifier to check.
   * @returns True when the registry holds an open database under it.
   */
  has(id: string): boolean {
    return this.dbs.has(id)
  }

  /**
   * Returns every database this registry currently holds open.
   *
   * @returns The open databases, keyed by identifier.
   */
  databases(): Map<string, Database> {
    return new Map(this.dbs)
  }

  /**
   * Closes every open database and stops the lifecycle timers.
   *
   * A database whose file Sirannon is replacing delays the shutdown until that
   * work finishes, since a process that exits part-way through would leave the
   * path with no database on it at all.
   */
  async shutdown(): Promise<void> {
    if (this._shutdown) return
    this._shutdown = true

    while (this.offlineWork.size > 0) {
      await Promise.allSettled([...this.offlineWork])
    }

    this.lifecycleManager?.dispose()

    await closeEveryDatabase(this.dbs)
  }

  /**
   * Registers a hook that runs before each statement on every database in this registry. Throw from it to refuse the statement.
   *
   * @param hook - Receives the statement, its parameters, and the concerns it carries.
   * @returns A function that removes the hook.
   */
  onBeforeQuery(hook: BeforeQueryHook): HookDispose {
    return this._hookRegistry.register('beforeQuery', hook)
  }

  /**
   * Registers a hook that runs after each statement on every database in this registry.
   *
   * @param hook - Receives the statement and how long it took.
   * @returns A function that removes the hook.
   */
  onAfterQuery(hook: AfterQueryHook): HookDispose {
    return this._hookRegistry.register('afterQuery', hook)
  }

  /**
   * Registers a hook that runs before a database connection opens.
   *
   * @param hook - Receives the database identifier and its file path.
   * @returns A function that removes the hook.
   */
  onBeforeConnect(hook: BeforeConnectHook): HookDispose {
    return this._hookRegistry.register('beforeConnect', hook)
  }

  /**
   * Registers a hook that runs once a database is open.
   *
   * @param hook - Receives the database identifier and its file path.
   * @returns A function that removes the hook.
   */
  onDatabaseOpen(hook: DatabaseOpenHook): HookDispose {
    return this._hookRegistry.register('databaseOpen', hook)
  }

  /**
   * Registers a hook that runs once a database is closed.
   *
   * @param hook - Receives the database identifier and its file path.
   * @returns A function that removes the hook.
   */
  onDatabaseClose(hook: DatabaseCloseHook): HookDispose {
    return this._hookRegistry.register('databaseClose', hook)
  }

  private ensureRunning(): void {
    if (this._shutdown) {
      throw new SirannonError('Sirannon has been shut down', 'SHUTDOWN')
    }
  }

  private ensureOpenAllowed(id: string): void {
    if (this.offline.has(id)) return
    this.ensureRunning()
  }
}
