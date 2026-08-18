import { DatabaseBackups } from './database-backups.js'
import { createDatabaseRuntime, type DatabaseInternals } from './database-create.js'
import { readOneRow, readRows, readWireRows } from './database-reads.js'
import type { DeviceSyncPort } from './database-sync.js'
import type { SQLiteConnection, SQLiteDriver } from './driver/types.js'
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
 * One open SQLite database, with its reads, writes, transactions, migrations, change subscriptions, and live queries.
 *
 * Open one through {@link Sirannon.open} rather than constructing it.
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
    return this.runtime.sync.applyChanges(batch, resolver)
  }

  /**
   * Returns the device-sync port for this database, which reads and advances
   * the cursors a `SyncController` keeps against a server.
   *
   * @returns The port, which `downloadDatabaseSnapshot` also accepts.
   */
  deviceSync(): DeviceSyncPort {
    this.ensureNotClosed()
    return this.runtime.sync.devicePort()
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
    return readRows<T>(this.runtime.reads, sql, params, options)
  }

  /** @internal */
  async queryForWire(sql: string, params?: Params, options?: QueryOptions): Promise<unknown[]> {
    this.ensureOpen()
    return readWireRows(this.runtime.reads, sql, params, options)
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
    return readOneRow<T>(this.runtime.reads, sql, params, options)
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
    this.ensureWritable()
    return this.runtime.writes.execute(sql, params, options)
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
    this.ensureWritable()
    return this.runtime.writes.executeBatch(sql, paramsBatch, options)
  }

  /**
   * Imports many rows with relaxed writer durability, then restores the configured level.
   *
   * Use this for a load you can re-run from scratch, because
   * {@link Database.executeBatch} keeps full durability. The load holds the
   * writer lock throughout and restores the configured level before this
   * resolves, whether it succeeds or fails. One transaction covers the whole
   * batch, and the rows are summed rather than returned one by one to bound
   * memory on a large load. Like {@link Database.execute}, this writes only to
   * the local database; under replication the server routes loads through the
   * engine instead.
   *
   * @param sql - The statement to run for each parameter set.
   * @param paramsBatch - One parameter set per row.
   * @param options - Durability during the load, and whether it ends with a checkpoint.
   * @returns How many rows the load applied and how many rows changed.
   */
  async bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    this.ensureWritable()
    return this.runtime.writes.bulkLoad(sql, paramsBatch, options)
  }

  /**
   * Runs a fixed list of statements in one transaction so that several callers can share one commit.
   *
   * This takes the statements up front rather than a callback, because a group
   * cannot wait on an arbitrary caller-supplied callback without delaying every
   * transaction beside it.
   *
   * @param statements - The statements to run, in order, each with its own parameters.
   * @returns One result per statement, in order.
   */
  async executeTransaction(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    this.ensureWritable()
    if (statements.length === 0) return []
    return this.runtime.writes.executeTransaction(statements)
  }

  /**
   * Runs a function inside one transaction, committing when it returns and rolling back when it throws.
   *
   * @param fn - Receives the transaction and runs statements on it.
   * @returns Whatever the function returned.
   */
  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.ensureWritable()
    return this.runtime.writes.transaction(fn)
  }

  /**
   * Starts recording changes to a table so that subscribers and replication see them.
   *
   * @param table - Name of the table to watch.
   */
  async watch(table: string): Promise<void> {
    this.ensureWritable()
    await this.runtime.cdc.watch(table)
  }

  /**
   * Stops recording changes to a table.
   *
   * @param table - Name of the table to stop watching.
   */
  async unwatch(table: string): Promise<void> {
    this.ensureOpen()
    await this.runtime.cdc.unwatch(table)
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
    if (this.closed) return
    await this.runtime.writerLock.run(() => op(this.runtime.pool.acquireWriter()))
  }

  /** @internal */
  async ensureChangeStamping(): Promise<void> {
    this.ensureWritable()
    await this.runtime.cdc.ensureStamping()
  }

  /**
   * Begins a change subscription on a watched table.
   *
   * @param table - Name of the watched table.
   * @returns A builder you narrow with a filter and then subscribe to.
   */
  on(table: string): SubscriptionBuilder {
    this.ensureOpen()
    return this.runtime.cdc.on(table)
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
    this.ensureWritable()
    const cdc = this.runtime.cdc
    return openLiveQuery<T>({ cdc, watch: table => this.watch(table) }, sql, params, options)
  }

  /**
   * Applies every migration this database has not yet applied, in ascending version order.
   *
   * @param migrations - The full set of migrations for this database.
   * @returns Which migrations this call applied, and how many it skipped.
   */
  async migrate(migrations: Migration[]): Promise<MigrationResult> {
    this.ensureOpen()
    return this.runtime.migrations.migrate(migrations)
  }

  /**
   * Lists the migrations this database has applied.
   *
   * @returns One entry per applied migration, with its version, name, and checksum.
   */
  async appliedMigrations(): Promise<AppliedMigrationRow[]> {
    this.ensureNotClosed()
    return this.runtime.migrations.applied()
  }

  /**
   * Undoes applied migrations, newest first.
   *
   * @param migrations - The full set of migrations so that the runner finds each down statement.
   * @param version - Lowest version to keep. Without it, only the newest migration is undone.
   * @returns Which migrations this call undid.
   */
  async rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    this.ensureOpen()
    return this.runtime.migrations.rollback(migrations, version)
  }

  /**
   * Loads a SQLite extension into this database.
   *
   * @param extensionPath - Path to the extension, which the driver resolves to an absolute path.
   */
  async loadExtension(extensionPath: string): Promise<void> {
    this.ensureOpen()
    await this.runtime.loadExtension(extensionPath)
  }

  /**
   * Registers a hook that runs before each statement on this database. Throw from it to refuse the statement.
   *
   * @param hook - Receives the statement, its parameters, and the concerns it carries.
   */
  onBeforeQuery(hook: BeforeQueryHook): void {
    this.runtime.hookRegistry.register('beforeQuery', hook)
  }

  /**
   * Registers a hook that runs after each statement on this database.
   *
   * @param hook - Receives the statement and how long it took.
   */
  onAfterQuery(hook: AfterQueryHook): void {
    this.runtime.hookRegistry.register('afterQuery', hook)
  }
}
