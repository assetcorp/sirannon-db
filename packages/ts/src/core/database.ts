import { runBulkLoad } from './bulk-load.js'
import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseBackupController } from './database-backup.js'
import type { DatabaseCdcController } from './database-cdc.js'
import { createDatabaseRuntime, type DatabaseInternals, type DatabaseRuntime } from './database-create.js'
import type { DatabaseObserver } from './database-observability.js'
import type { DatabaseSyncController, DeviceSyncPort } from './database-sync.js'
import { DEFAULT_SYNCHRONOUS } from './driver/synchronous.js'
import type { SQLiteConnection, SQLiteDriver, SynchronousLevel } from './driver/types.js'
import { ReadOnlyError, SirannonError } from './errors.js'
import { loadExtension as loadExtensionImpl } from './extension-loader.js'
import { canGroupTransaction, type GroupCommitter } from './group-committer.js'
import type { HookRegistry } from './hooks/registry.js'

export type { DatabaseInternals } from './database-create.js'

import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { executeBatch, executeBatchSummary, query, queryForWire, queryOne } from './query-executor.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
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

export class Database {
  readonly id: string
  readonly path: string
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
    this.readOnly = options?.readOnly ?? false
    this.synchronous = options?.synchronous ?? DEFAULT_SYNCHRONOUS
    this.walMode = options?.walMode ?? true
  }

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

  async applyChanges(
    batch: ReplicationBatch,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)
    return this.sync.applyChanges(batch, resolver)
  }

  deviceSync(): DeviceSyncPort {
    this.ensureNotClosed()
    return this.sync.devicePort()
  }

  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    this.ensureOpen()
    return this.observer.withQueryHooks(sql, params, options, () =>
      this.runRead(sql, conn => query<T>(conn, sql, params)),
    )
  }

  /**
   * Returns query rows already encoded for the wire (safe-range integers as
   * plain numbers, larger integers and BLOBs as tagged envelopes) in a single
   * pass. The server response path uses this so a read walks its values once
   * rather than narrowing on the driver and re-scanning to tag.
   */
  async queryForWire(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]> {
    this.ensureOpen()
    return this.observer.withQueryHooks(sql, params, options, () =>
      this.runRead(sql, conn => queryForWire(conn, sql, params)),
    )
  }

  async queryOne<T = Record<string, unknown>>(
    sql: string,
    params?: Params,
    options?: QueryOptions,
  ): Promise<T | undefined> {
    this.ensureOpen()
    return this.observer.withQueryHooks(sql, params, options, () =>
      this.runRead(sql, conn => queryOne<T>(conn, sql, params)),
    )
  }

  /**
   * On a single-connection driver the reader is the writer, so serialise the
   * read through the writer lock to avoid a bulk load's uncommitted rows.
   */
  private runRead<T>(sql: string, op: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
    if (this.pool.readerCount === 0) {
      return this.writerLock.run(() => this.observer.track(sql, () => op(this.pool.acquireWriter())))
    }
    return this.observer.track(sql, () => op(this.pool.acquireReader()))
  }

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

  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.ensureOpen()
    if (this.readOnly) throw new ReadOnlyError(this.id)

    return this.writeGate.run(() => this.writerLock.run(() => this.cdc.runTransaction(this.pool.acquireWriter(), fn)))
  }

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
   */
  async runCdcMaintenance(op: (writer: SQLiteConnection) => Promise<unknown>): Promise<void> {
    if (this._closed) return
    await this.writerLock.run(() => op(this.pool.acquireWriter()))
  }

  async unwatch(table: string): Promise<void> {
    this.ensureOpen()
    await this.cdc.unwatch(table)
  }

  on(table: string): SubscriptionBuilder {
    this.ensureOpen()
    return this.cdc.on(table)
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
    this.ensureNotClosed()
    this.closeListeners.push(fn)
  }

  async close(): Promise<void> {
    if (this._closed) return
    this._closed = true

    this.cdc.stop()
    this.backups.cancelAll()

    let poolError: unknown
    try {
      await this.groupCommitter.drain()
      await this.writerLock.settle()
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
