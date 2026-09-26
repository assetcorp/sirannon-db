import { Database } from './database.js'
import type { SQLiteDriver } from './driver/types.js'
import { DatabaseAlreadyExistsError, DatabaseNotFoundError, ReadOnlyError, SirannonError } from './errors.js'
import { HookRegistry } from './hooks/registry.js'
import type { HookDispose } from './hooks/types.js'
import { LifecycleManager } from './lifecycle/manager.js'
import { MetricsCollector } from './metrics/collector.js'
import { RegistryMigrationSet } from './migrations/registry-set.js'
import type { Migration } from './migrations/types.js'
import { withRegistryDefaults } from './registry-defaults.js'
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
 * The registry opens every database through one driver and applies its hooks, metrics, and migrations to each one; when you configure a lifecycle, it also closes idle databases.
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

  /** The driver, hooks, metrics, lifecycle, migrations, and writer-worker and retention defaults that this registry was constructed with. */
  readonly options: SirannonOptions

  /**
   * Creates a registry.
   *
   * @param options - The driver, hooks, metrics, lifecycle, migrations, and the defaults for every database that the registry opens.
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
   * @param id - The identifier that callers use to reach this database.
   * @param path - The file path of the SQLite database.
   * @param options - The pool size, journal mode, synchronous level, and change-capture settings.
   * @returns The open database.
   * @throws A `DatabaseAlreadyExistsError` when the identifier is already in use, or `DATABASE_OPEN_FAILED` when the open or a registry migration fails.
   */
  async open(id: string, path: string, options?: DatabaseOptions): Promise<Database> {
    this.ensureOpenAllowed(id)
    if (this.dbs.has(id) || this.opening.has(id)) {
      throw new DatabaseAlreadyExistsError(id)
    }

    this.opening.add(id)

    const resolvedOptions = withRegistryDefaults(this.options, options)

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

  /**
   * Closes one database and removes it from the registry.
   *
   * @param id - The identifier of the database to close.
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
   * Closes one database, calls an action on its file, and then opens that
   * database again under the same identifier with its earlier settings.
   *
   * A restore rebuilds a database at its current path, so no connection may be
   * open on that file while Sirannon replaces its bytes. {@link Sirannon.get}
   * returns undefined for the identifier until the database is open again, and
   * Sirannon reopens the database even after a failed action.
   *
   * @param id - The identifier of the database to take offline.
   * @param action - The function that Sirannon calls with the database file path once the database is closed.
   * @returns Whether the action returned, the value that it returned or the error that it threw, and any error that the reopen threw.
   * @throws A `DatabaseNotFoundError` when no database is open under the identifier, a `ReadOnlyError` when the database is read-only, or the close error when the close fails.
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
   * @param id - The identifier of the database.
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
   * Returns an open database, and opens it through the lifecycle resolver when it is not open yet.
   *
   * An in-process application calls this for a database that the registry opens on first use
   * through the `lifecycle.autoOpen` resolver, such as one file per tenant. Concurrent calls for
   * the same unopened identifier share one open, so each call receives the same database or the
   * same error.
   *
   * @param id - The identifier of the database.
   * @returns The database, or undefined when none is open and the resolver returns no path for it.
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
   * Returns the registry's migration set, which Sirannon applies to each writable database as it opens.
   *
   * When the set comes from a function, the registry calls that function once and caches the
   * result, so this returns the same list on every call; after a failed load, the next call
   * tries again.
   *
   * @returns The migrations declared in `SirannonOptions.migrations`, or an empty list when there are none.
   */
  registryMigrations(): Promise<Migration[]> {
    return this.migrations.load()
  }

  /**
   * Returns whether a database is open under an identifier.
   *
   * @param id - The identifier to check.
   * @returns True when the registry has an open database under that identifier.
   */
  has(id: string): boolean {
    return this.dbs.has(id)
  }

  /**
   * Returns every database that this registry has open.
   *
   * @returns A copy of the open databases, keyed by identifier.
   */
  databases(): Map<string, Database> {
    return new Map(this.dbs)
  }

  /**
   * Closes every open database and stops the lifecycle timers.
   *
   * When Sirannon is replacing a database's file, the shutdown waits for that
   * work to finish, because a process that exits part-way through the
   * replacement can leave no database at the path.
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
   * Registers a hook that Sirannon calls before each statement on every database in this registry; throw from the hook to reject the statement.
   *
   * @param hook - The hook, which Sirannon calls with the statement, its parameters, and its read or write concern.
   * @returns A function that removes the hook.
   */
  onBeforeQuery(hook: BeforeQueryHook): HookDispose {
    return this._hookRegistry.register('beforeQuery', hook)
  }

  /**
   * Registers a hook that Sirannon calls after each statement on every database in this registry.
   *
   * @param hook - The hook, which Sirannon calls with the statement and its duration.
   * @returns A function that removes the hook.
   */
  onAfterQuery(hook: AfterQueryHook): HookDispose {
    return this._hookRegistry.register('afterQuery', hook)
  }

  /**
   * Registers a hook that Sirannon calls before it opens a database.
   *
   * @param hook - The hook, which Sirannon calls with the database identifier and file path.
   * @returns A function that removes the hook.
   */
  onBeforeConnect(hook: BeforeConnectHook): HookDispose {
    return this._hookRegistry.register('beforeConnect', hook)
  }

  /**
   * Registers a hook that Sirannon calls once a database is open.
   *
   * @param hook - The hook, which Sirannon calls with the database identifier and file path.
   * @returns A function that removes the hook.
   */
  onDatabaseOpen(hook: DatabaseOpenHook): HookDispose {
    return this._hookRegistry.register('databaseOpen', hook)
  }

  /**
   * Registers a hook that Sirannon calls once a database is closed.
   *
   * @param hook - The hook, which Sirannon calls with the database identifier and file path.
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
