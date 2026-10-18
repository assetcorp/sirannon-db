import type { PruneBoundarySource } from './cdc/prune-boundaries.js'
import { DatabaseBackups } from './database-backups.js'
import { createDatabaseRuntime, type DatabaseInternals } from './database-create.js'
import type { DatabaseObserver } from './database-observability.js'
import { readOneRow, readRows, readWireRows } from './database-reads.js'
import type { DeviceSyncPort } from './database-sync.js'
import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
import type { HookDispose } from './hooks/types.js'
import { openLiveQuery } from './live/database-live.js'
import type { LiveQuery, LiveQueryOptions } from './live/types.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
import type { AppliedMigrationRow } from './system-catalog/index.js'
import type { Transaction } from './transaction.js'
import type {
  AfterQueryHook,
  BeforeQueryHook,
  BulkLoadOptions,
  BulkLoadResult,
  DatabaseOptions,
  ExecuteResult,
  Params,
  QueryOptions,
  SubscriptionBuilder,
} from './types.js'

export type { DatabaseInternals } from './database-create.js'

/**
 * An open SQLite database, with methods for reads, writes, transactions, migrations, change subscriptions, and live queries.
 *
 * Open one through {@link Sirannon.open}.
 *
 * @public
 */
export class Database extends DatabaseBackups {
  /** @internal */
  static async create(
    id: string,
    path: string,
    driver: SQLiteDriver,
    options?: DatabaseOptions,
    internals?: DatabaseInternals,
  ): Promise<Database> {
    const runtime = await createDatabaseRuntime(id, path, driver, options, internals)
    return new Database(id, path, runtime, options)
  }

  /** @internal */
  async applyChanges(
    batch: ReplicationBatch,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    this.ensureWritable()
    this.ensureWriterCanWait()
    return this.runtime.sync.applyChanges(batch, resolver)
  }

  /**
   * Returns the device-sync port for this database, which a `SyncController`
   * calls to apply pulled changes, read unpushed local changes, and store its
   * push and pull cursors.
   *
   * @returns The port, which you can also pass to `downloadDatabaseSnapshot`.
   */
  deviceSync(): DeviceSyncPort {
    this.ensureNotClosed()
    return this.runtime.sync.devicePort()
  }

  /**
   * Executes a read and returns every row.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The read concern for this statement.
   * @returns The rows that the statement returns.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    this.ensureOpen()
    return readRows<T>(this.runtime.reads, sql, params, options)
  }

  /** @internal */
  async queryForWire(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]> {
    this.ensureOpen()
    return readWireRows(this.runtime.reads, sql, params, options)
  }

  /**
   * Executes a read and returns its first row.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The read concern for this statement.
   * @returns The first row, or undefined when the statement returns none.
   */
  async queryOne<T = Record<string, unknown>>(
    sql: string,
    params?: Params,
    options?: QueryOptions,
  ): Promise<T | undefined> {
    this.ensureOpen()
    return readOneRow<T>(this.runtime.reads, sql, params, options)
  }

  /**
   * Executes one write statement.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The write concern for this statement.
   * @returns The number of rows that the statement changed, and the row id of the last inserted row.
   */
  async execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    this.ensureWritable()
    return this.runtime.writes.execute(sql, params, options)
  }

  /**
   * Executes one statement once for each parameter set, inside a single transaction.
   *
   * @param sql - The statement to execute for each parameter set.
   * @param paramsBatch - One parameter set for each execution.
   * @param options - The write concern for the transaction.
   * @returns One result per parameter set, in order.
   */
  async executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    this.ensureWritable()
    return this.runtime.writes.executeBatch(sql, paramsBatch, options)
  }

  /**
   * Imports many rows at a relaxed synchronous level, then restores the level that you configured.
   *
   * Use this for a load that you can re-run from scratch, and use
   * {@link Database.executeBatch} where the rows need full durability. Sirannon
   * holds the writer lock for the whole load and commits every row in one
   * transaction. Whether the load succeeds or fails, Sirannon restores the
   * configured level before this method resolves. The result contains totals
   * for the whole load, so that a large load keeps no per-row results in memory.
   * A load commits in a transaction of its own, so inside a
   * {@link Database.transaction} callback this method fails with
   * `BULK_LOAD_IN_TRANSACTION`, and {@link Transaction.executeBatch} writes the
   * rows inside that transaction instead.
   *
   * Like {@link Database.execute}, this writes to the local database only. A
   * server whose execution target has no `bulkLoad` method, such as the
   * replication engine, rejects bulk loads with `BULK_LOAD_UNSUPPORTED`.
   *
   * @param sql - The statement to execute for each parameter set.
   * @param paramsBatch - One parameter set per row.
   * @param options - The synchronous level during the load, and whether the load ends with a checkpoint.
   * @returns The number of parameter sets that the load applied, and the number of rows that changed.
   */
  async bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    this.ensureWritable()
    return this.runtime.writes.bulkLoad(sql, paramsBatch, options)
  }

  /**
   * Executes a fixed list of statements in one transaction, which Sirannon can commit together with other callers' writes.
   *
   * When every statement is an INSERT, UPDATE, DELETE, or REPLACE, Sirannon
   * groups the transaction with other writes into one commit, and otherwise it
   * executes the transaction on its own. This method takes the statements up
   * front, because a group commit that waited on a caller's callback would delay
   * every other transaction in the group.
   *
   * @param statements - The statements to execute, in order, each with its own parameters.
   * @returns One result per statement, in order.
   */
  async executeTransaction(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    this.ensureWritable()
    if (statements.length === 0) return []
    return this.runtime.writes.executeTransaction(statements)
  }

  /**
   * Calls a function inside one transaction, and commits when the function returns or rolls back when it throws.
   *
   * On the Node, better-sqlite3, and Bun drivers, a write that the function
   * makes through this database, with {@link Database.execute},
   * {@link Database.executeBatch}, {@link Database.executeTransaction}, or this
   * method, becomes part of the same transaction. Sirannon executes each batch,
   * statement list, and nested function inside a savepoint, so that one that
   * fails rolls back only its own writes. The wa-sqlite and Expo drivers have
   * no async context to tell the function's own calls from another caller's,
   * and they read through the writer connection. On those drivers, while the
   * function holds the writer, every call through this database that needs
   * the writer fails at once with `WRITER_HELD_BY_CALLBACK`, whoever makes it.
   * The refusal applies to reads, writes, migrations, backups,
   * {@link Database.watch}, and {@link Database.live}, so the function has to
   * make its reads and writes through `tx`.
   * Sirannon fires the query hooks for each statement that the
   * function executes through the transaction, and once the function returns
   * or throws, the transaction refuses every further call with
   * `TRANSACTION_CLOSED`.
   *
   * @param fn - The function to call with the transaction, which executes its statements through it.
   * @returns The value that the function returns.
   */
  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.ensureWritable()
    return this.runtime.writes.transaction(fn)
  }

  /**
   * Starts recording changes to a table, so that subscribers and replication receive them.
   *
   * @param table - The name of the table to watch.
   */
  async watch(table: string): Promise<void> {
    this.ensureWritable()
    this.ensureWriterCanWait()
    await this.runtime.cdc.watch(table)
  }

  /**
   * Stops recording changes to a table.
   *
   * @param table - The name of the table to stop watching.
   */
  async unwatch(table: string): Promise<void> {
    this.ensureOpen()
    this.ensureWriterCanWait()
    await this.runtime.cdc.unwatch(table)
  }

  /**
   * Executes a change-log maintenance write, such as pruning, on the shared
   * writer connection under the writer lock, so that it queues behind
   * application writes and never contends with them for SQLite's write lock,
   * which would block the event loop for the `busy_timeout`.
   *
   * @internal
   */
  async runCdcMaintenance(op: (writer: SQLiteConnection) => Promise<unknown>): Promise<void> {
    if (this.closed) return
    await this.runtime.writerLock.run(() => op(this.runtime.pool.acquireWriter()))
  }

  /**
   * Keeps the change-log entries that a component outside this database still needs, such as the changes that the replication engine's peers have yet to acknowledge, out of reach of this database's own pruning.
   *
   * @param source - The component that holds the entries back.
   * @param seq - The highest change-log sequence that pruning can remove for this component, or `null` to release the hold.
   * @internal
   */
  holdChangeLogPruning(source: PruneBoundarySource, seq: bigint | null): void {
    const tracker = this.runtime.cdc.changeTracker
    if (tracker === null) return
    if (seq === null) tracker.clearPruneBoundary(source)
    else tracker.setPruneBoundary(source, seq)
  }

  /**
   * Returns the observer that fires this database's query hooks and reports its query metrics, for a component that executes statements on the database's behalf, such as the replication engine.
   *
   * @internal
   */
  get observer(): DatabaseObserver {
    return this.runtime.reads.observer
  }

  /**
   * Starts building a change subscription on a watched table.
   *
   * @param table - The name of the watched table.
   * @returns A builder that you narrow with a filter and then subscribe to.
   */
  on(table: string): SubscriptionBuilder {
    this.ensureOpen()
    return this.runtime.cdc.on(table)
  }

  /**
   * Opens a live query that keeps the rows of a single-table SELECT current as that table changes, and watches the table first.
   *
   * @param sql - The SELECT statement to keep current.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The re-read jitter, the transaction size above which the query re-reads, and the reporter for listener failures.
   * @returns The live query, holding its first rows and subscribed to changes.
   */
  async live<T = Record<string, unknown>>(
    sql: string,
    params?: Params,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<T>> {
    this.ensureWritable()
    this.ensureWriterCanWait()
    const cdc = this.runtime.cdc
    return openLiveQuery<T>(
      { cdc, observer: this.runtime.reads.observer, watch: table => this.watch(table) },
      sql,
      params,
      options,
    )
  }

  /**
   * Applies every migration that this database has yet to apply, in ascending version order.
   *
   * @param migrations - The full set of migrations for this database.
   * @returns The migrations that this call applied, and the number that the database had already applied.
   * @throws A `ReadOnlyError` when the database is open in read-only mode.
   */
  async migrate(migrations: Migration[]): Promise<MigrationResult> {
    this.ensureWritable()
    this.ensureWriterCanWait()
    return this.runtime.migrations.migrate(migrations)
  }

  /**
   * Lists the migrations that this database has applied.
   *
   * @returns One entry per applied migration, with its version, name, and checksum.
   */
  async appliedMigrations(): Promise<AppliedMigrationRow[]> {
    this.ensureNotClosed()
    this.ensureWriterCanWait()
    return this.runtime.migrations.applied()
  }

  /**
   * Reverts applied migrations, newest first.
   *
   * @param migrations - The full set of migrations, from which the runner takes each down statement.
   * @param version - The version to roll back to, which stays applied with every version below it; when you omit it, the runner reverts only the newest migration.
   * @returns The migrations that this call reverted.
   * @throws A `ReadOnlyError` when the database is open in read-only mode.
   */
  async rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    this.ensureWritable()
    this.ensureWriterCanWait()
    return this.runtime.migrations.rollback(migrations, version)
  }

  /**
   * Loads a compiled SQLite extension into every connection of this database.
   *
   * @param extensionPath - The path to the extension, which the driver resolves to an absolute path.
   */
  async loadExtension(extensionPath: string): Promise<void> {
    this.ensureOpen()
    this.ensureWriterCanWait()
    await this.runtime.loadExtension(extensionPath)
  }

  /**
   * Registers a hook that Sirannon calls before each statement on this database; throw from the hook to reject the statement.
   *
   * @param hook - The hook, which Sirannon calls with the statement, its parameters, and its read or write concern.
   * @returns A function that removes the hook.
   */
  onBeforeQuery(hook: BeforeQueryHook): HookDispose {
    return this.runtime.hookRegistry.register('beforeQuery', hook)
  }

  /**
   * Registers a hook that Sirannon calls after each statement on this database.
   *
   * @param hook - The hook, which Sirannon calls with the statement and its duration.
   * @returns A function that removes the hook.
   */
  onAfterQuery(hook: AfterQueryHook): HookDispose {
    return this.runtime.hookRegistry.register('afterQuery', hook)
  }
}
