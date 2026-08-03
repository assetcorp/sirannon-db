import { runBulkLoad } from './bulk-load.js'
import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseBackupController } from './database-backup.js'
import type { DatabaseCdcController } from './database-cdc.js'
import { createDatabaseRuntime, type DatabaseInternals, type DatabaseRuntime } from './database-create.js'
import type { DatabaseObserver } from './database-observability.js'
import type { DatabaseReadDeps } from './database-reads.js'
import { readOneRow, readRows, readWireRows } from './database-reads.js'
import type { DatabaseSyncController, DeviceSyncPort } from './database-sync.js'
import { DEFAULT_SYNCHRONOUS } from './driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SynchronousLevel } from './driver/types.js'
import { ReadOnlyError, SirannonError } from './errors.js'
import { loadExtension as loadExtensionImpl } from './extension-loader.js'
import { canGroupTransaction, type GroupCommitter } from './group-committer.js'
import type { HookRegistry } from './hooks/registry.js'
import { openLiveQuery } from './live/database-live.js'
import type { LiveQuery, LiveQueryOptions } from './live/types.js'

export type { DatabaseInternals } from './database-create.js'

import { migrateWithTriggerRefresh, readAppliedMigrations, rollbackWithTriggerRefresh } from './database-migrations.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { executeBatch, executeBatchSummary } from './query-executor.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
import type { AppliedMigrationRow } from './system-catalog/index.js'
import type { Transaction } from './transaction.js'
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
import type { WriteGate } from './worker/gate.js'
import type { WriterLock } from './writer-lock.js'

/**
 * One open SQLite database, with its reads, writes, transactions, migrations, change subscriptions, and live queries.
 *
 * Open one through {@link Sirannon.open} rather than constructing it.
 *
 * @public
 */
export class Database {
  /**
   * Identifier this database was opened under.
   */
  readonly id: string
  /**
   * File path of the SQLite database.
   */
  readonly path: string
  /**
   * Whether this database refuses writes.
   */
  readonly readOnly: boolean
  private readonly pool: ConnectionPool
  private readonly driver: SQLiteDriver
  private readonly synchronous: SynchronousLevel
  private readonly walMode: boolean
  private readonly writerLock: WriterLock
  private readonly writeGate: WriteGate
  private readonly groupCommitter: GroupCommitter
  private readonly closeListeners: (() => void | Promise<void>)[] = []
  private _closed = false

  private readonly cdc: DatabaseCdcController
  private readonly sync: DatabaseSyncController

  private readonly hookRegistry: HookRegistry
  private readonly observer: DatabaseObserver

  private readonly backups: DatabaseBackupController
  private readonly reads: DatabaseReadDeps

  private constructor(
    id: string,
    path: string,
    driver: SQLiteDriver,
    runtime: DatabaseRuntime,
    options?: DatabaseOptions,
  ) {
    this.id = id
    this.path = path
    this.driver = driver
    this.pool = runtime.pool
    this.writeGate = runtime.writeGate
    this.writerLock = runtime.writerLock
    this.hookRegistry = runtime.hookRegistry
    this.observer = runtime.observer
    this.backups = runtime.backups
    this.cdc = runtime.cdc
    this.sync = runtime.sync
    this.groupCommitter = runtime.groupCommitter
    this.reads = { pool: runtime.pool, writerLock: runtime.writerLock, observer: runtime.observer }
    this.readOnly = options?.readOnly ?? false
    this.synchronous = options?.synchronous ?? DEFAULT_SYNCHRONOUS
    this.walMode = options?.walMode ?? true
  }

  /** @internal */
  static async create(
    id: string,
    path: string,
    driver: SQLiteDriver,
    options?: DatabaseOptions,
    internals?: DatabaseInternals,
  ): Promise<Database> {
    const runtime = await createDatabaseRuntime(id, path, driver, options, internals)
    return new Database(id, path, driver, runtime, options)
  }

  /** @internal */
  async applyChanges(
    batch: ReplicationBatch,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    return this.sync.applyChanges(batch, resolver)
  }

  /**
   * Returns the device-sync port for this database, which reads and advances
   * the cursors a `SyncController` keeps against a server.
   *
   * @returns The port, which `downloadDatabaseSnapshot` also accepts.
   */
  deviceSync(): DeviceSyncPort {
    this.ensureNotClosed()
    return this.sync.devicePort()
  }

  /**
   * Runs a read and returns every row.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @param options - Read concern for this statement.
   * @returns The rows the statement produced.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    this.ensureOpen()
    return readRows<T>(this.reads, sql, params, options)
  }

  /** @internal */
  async queryForWire(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]> {
    this.ensureOpen()
    return readWireRows(this.reads, sql, params, options)
  }

  /**
   * Runs a read and returns its first row.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @param options - Read concern for this statement.
   * @returns The first row, or undefined when the statement produced none.
   */
  async queryOne<T = Record<string, unknown>>(
    sql: string,
    params?: Params,
    options?: QueryOptions,
  ): Promise<T | undefined> {
    this.ensureOpen()
    return readOneRow<T>(this.reads, sql, params, options)
  }

  /**
   * Runs one write.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @param options - Write concern for this statement.
   * @returns How many rows changed, and the last inserted row id.
   */
  async execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    return this.observer.withQueryHooks(sql, params, options, () =>
      this.writeGate.run(() =>
        this.observer.track(sql, () =>
          this.writerLock.isHeld()
            ? this.groupCommitter.runUngrouped(sql, params)
            : this.groupCommitter.submit(sql, params),
        ),
      ),
    )
  }

  /**
   * Runs one statement over many parameter sets inside a single transaction.
   *
   * @param sql - The statement to run for each parameter set.
   * @param paramsBatch - One parameter set per run.
   * @param options - Write concern for the transaction.
   * @returns One result per parameter set, in order.
   */
  async executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    return this.observer.withQueryHooks(sql, undefined, options, () =>
      this.writeGate.run(() =>
        this.writerLock.run(() =>
          this.runInTransaction(this.pool.acquireWriter(), sql, txConn => executeBatch(txConn, sql, paramsBatch)),
        ),
      ),
    )
  }

  private async runInTransaction<T>(
    writer: SQLiteConnection,
    sql: string,
    run: (txConn: SQLiteConnection) => Promise<T>,
  ): Promise<T> {
    const result = await this.observer.track(sql, () =>
      writer.transaction(async txConn => {
        const value = await run(txConn)
        await this.cdc.applyStamps(txConn)
        return value
      }),
    )
    await applyDdlSideEffectsIfRelevant(this.cdc.changeTracker, writer, sql)
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
   *
   * Imports many rows with relaxed writer durability, then restores the configured level.
   *
   * Use this for a load you can re-run from scratch. {@link Database.executeBatch} keeps full durability.
   *
   * @param sql - The statement to run for each parameter set.
   * @param paramsBatch - One parameter set per row.
   * @param options - Durability during the load, and whether it ends with a checkpoint.
   * @returns How many rows the load applied and how many rows changed.
   */
  async bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    return this.observer.withQueryHooks(sql, undefined, undefined, () =>
      this.writeGate.run(() =>
        this.writerLock.run(() => {
          const writer = this.pool.acquireWriter()
          return runBulkLoad({
            writer,
            configuredSynchronous: this.synchronous,
            walMode: this.walMode,
            durability: options?.durability,
            checkpoint: options?.checkpoint ?? true,
            loadRows: () => this.runInTransaction(writer, sql, txConn => executeBatchSummary(txConn, sql, paramsBatch)),
          })
        }),
      ),
    )
  }

  /**
   * Takes the statements up front rather than a callback, since a group cannot wait on an
   * arbitrary caller-supplied callback without delaying every transaction beside it.
   *
   * Runs a fixed list of statements in one transaction, so several callers can share one commit.
   *
   * @param statements - The statements to run, in order, each with its own parameters.
   * @returns One result per statement, in order.
   */
  async executeTransaction(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    if (statements.length === 0) return []

    const owned = statements.map(statement => ({ sql: statement.sql, params: statement.params }))
    const run = canGroupTransaction(owned)
      ? () => this.writeGate.run(() => this.groupCommitter.submitTransaction(owned))
      : () => this.runStatementsAlone(owned)

    if (!this.observer.observesQueries) return run()
    return this.observer.withTransactionHooks(owned, run)
  }

  private runStatementsAlone(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    return this.transaction(async tx => {
      const results: ExecuteResult[] = new Array(statements.length)
      for (let i = 0; i < statements.length; i++) {
        results[i] = await tx.execute(statements[i].sql, statements[i].params)
      }
      return results
    })
  }

  /**
   * Runs a function inside one transaction, committing when it returns and rolling back when it throws.
   *
   * @param fn - Receives the transaction and runs statements on it.
   * @returns Whatever the function returned.
   */
  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)

    return this.writeGate.run(() => this.writerLock.run(() => this.cdc.runTransaction(this.pool.acquireWriter(), fn)))
  }

  /**
   * Starts recording changes to a table, so subscribers and replication see them.
   *
   * @param table - Name of the table to watch.
   */
  async watch(table: string): Promise<void> {
    this.ensureOpen()
    if (this.readOnly) {
      throw new ReadOnlyError(this.id)
    }
    await this.cdc.watch(table)
  }

  /**
   * Runs a CDC maintenance write (change-log pruning) on the shared writer
   * under the writer lock. Serialising it with application writes keeps it
   * from becoming a second writer that contends for SQLite's single write
   * lock and stalls the event loop on `busy_timeout`.
   *
   * @internal
   */
  async runCdcMaintenance(op: (writer: SQLiteConnection) => Promise<unknown>): Promise<void> {
    if (this._closed) return
    await this.writerLock.run(() => op(this.pool.acquireWriter()))
  }

  /** @internal */
  async ensureChangeStamping(): Promise<void> {
    this.ensureOpen()
    if (this.readOnly) {
      throw new ReadOnlyError(this.id)
    }
    await this.cdc.ensureStamping()
  }

  /**
   * Stops recording changes to a table.
   *
   * @param table - Name of the table to stop watching.
   */
  async unwatch(table: string): Promise<void> {
    this.ensureOpen()
    await this.cdc.unwatch(table)
  }

  /**
   * Begins a change subscription on a watched table.
   *
   * @param table - Name of the watched table.
   * @returns A builder you narrow with a filter and then subscribe to.
   */
  on(table: string): SubscriptionBuilder {
    this.ensureOpen()
    return this.cdc.on(table)
  }

  /**
   * Opens a live query that keeps a registered read's rows current as the tables behind it change.
   *
   * @param operation - Name of the registered read, or a reference built by {@link operationRef}.
   * @param args - Arguments the operation takes.
   * @param options - Re-read jitter and the transaction size above which the query re-reads.
   * @returns The live query, already subscribed.
   */
  async live<T = Record<string, unknown>>(
    sql: string,
    params?: Params,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<T>> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    return openLiveQuery<T>({ cdc: this.cdc, watch: table => this.watch(table) }, sql, params, options)
  }

  /**
   * Applies every migration this database has not yet applied, in ascending version order.
   *
   * @param migrations - The full set of migrations for this database.
   * @returns Which migrations this call applied, and how many it skipped.
   */
  async migrate(migrations: Migration[]): Promise<MigrationResult> {
    this.ensureOpen()
    return this.writerLock.run(() =>
      migrateWithTriggerRefresh(this.pool.acquireWriter(), this.cdc.changeTracker, migrations),
    )
  }

  /**
   * Lists the migrations this database has applied.
   *
   * @returns One entry per applied migration, with its version, name, and checksum.
   */
  async appliedMigrations(): Promise<AppliedMigrationRow[]> {
    this.ensureNotClosed()
    return this.writerLock.run(() => readAppliedMigrations(this.pool.acquireWriter()))
  }

  /**
   * Undoes applied migrations, newest first.
   *
   * @param migrations - The full set of migrations, so the runner finds each down statement.
   * @param version - Lowest version to keep. Without it, only the newest migration is undone.
   * @returns Which migrations this call undid.
   */
  async rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    this.ensureOpen()
    return this.writerLock.run(() =>
      rollbackWithTriggerRefresh(this.pool.acquireWriter(), this.cdc.changeTracker, migrations, version),
    )
  }

  /**
   * Copies this database to a file while it stays open for reads and writes.
   *
   * @param destPath - Path the copy is written to.
   */
  async backup(destPath: string): Promise<void> {
    this.ensureOpen()
    await this.backups.backup(destPath)
  }

  /**
   * Starts repeating backups on a cron schedule, keeping a bounded number of files.
   *
   * @param options - Cron expression, destination directory, retention, time zone, and failure callback.
   */
  scheduleBackup(options: BackupScheduleOptions): void {
    this.ensureOpen()
    this.backups.schedule(options)
  }

  /**
   * Loads a SQLite extension into this database.
   *
   * @param extensionPath - Path to the extension, which the driver resolves to an absolute path.
   */
  async loadExtension(extensionPath: string): Promise<void> {
    this.ensureOpen()
    await this.writerLock.run(() => loadExtensionImpl(this.driver, this.pool.acquireWriter(), extensionPath))
  }

  /**
   * Registers a hook that runs before each statement on this database. Throw from it to refuse the statement.
   *
   * @param hook - Receives the statement, its parameters, and the concerns it carries.
   */
  onBeforeQuery(hook: BeforeQueryHook): void {
    this.hookRegistry.register('beforeQuery', hook)
  }

  /**
   * Registers a hook that runs after each statement on this database.
   *
   * @param hook - Receives the statement and how long it took.
   */
  onAfterQuery(hook: AfterQueryHook): void {
    this.hookRegistry.register('afterQuery', hook)
  }

  /** @internal */
  addCloseListener(fn: () => void | Promise<void>): void {
    this.ensureNotClosed()
    this.closeListeners.push(fn)
  }

  /**
   * Closes every connection this database holds and ends its subscriptions.
   */
  async close(): Promise<void> {
    if (this._closed) return
    this._closed = true

    this.cdc.stop()
    this.backups.cancelAll()

    let poolError: unknown
    try {
      await this.cdc.closeLiveConnection()
      await this.groupCommitter.drain()
      await this.writerLock.settle()
      await this.pool.close()
    } catch (err) {
      poolError = err
    }

    for (const fn of this.closeListeners) {
      try {
        await fn()
      } catch {}
    }

    if (poolError) {
      throw poolError
    }
  }

  /**
   * Whether this database has been closed.
   */
  get closed(): boolean {
    return this._closed
  }

  /**
   * Number of read connections the pool holds.
   */
  get readerCount(): number {
    return this.pool.readerCount
  }

  private ensureOpen(): void {
    this.ensureNotClosed()
    if (this.sync.snapshotLoadBlocked) {
      throw new SirannonError(
        `Database '${this.id}' is replacing its data from a sync snapshot; retry once the snapshot load completes`,
        'SNAPSHOT_IN_PROGRESS',
      )
    }
  }

  private ensureNotClosed(): void {
    if (this._closed) {
      throw new SirannonError(`Database '${this.id}' is closed`, 'DATABASE_CLOSED')
    }
  }
}
