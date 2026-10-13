import type { SQLiteConnection } from '../driver/types.js'
import { CDCError, ForbiddenSqlError } from '../errors.js'
import { CHANGES_TABLE, isReservedIdentifier } from '../internal-tables.js'
import {
  deleteAllReplacedRows,
  deleteChangesBeforeUpToSeqSql,
  deleteReplacedRowsOfTable,
  ensureChangesTable,
  ensureReplacedRowsTable,
  selectMaxChangeSeq,
  selectMinChangeSeqSql,
  selectOrdinaryTableName,
  selectTableExists,
} from '../system-catalog/index.js'
import type { ChangeEvent } from '../types.js'
import { type CaptureShape, readCaptureShape, sameCaptureShape } from './capture-shape.js'
import { pollChanges, readSinceTables } from './change-log-reader.js'
import { PruneBoundaries, type PruneBoundarySource, seqBoundFor } from './prune-boundaries.js'
import { StatementCache } from './statement-cache.js'
import { cdcTriggersMatch, dropCdcTriggers, installCdcTriggers } from './trigger-sql.js'
import type { ChangeTrackerOptions, WatchedTableInfo } from './types.js'

const DEFAULT_RETENTION_MS = 3_600_000
const DEFAULT_POLL_BATCH_SIZE = 1000
const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

/**
 * Records every insert, update, and delete on each table that you watch as a row in the change log, so that subscribers and replication can consume the changes in order.
 *
 * Once you watch each table that you want to capture, you can pass the tracker to a replication engine.
 *
 * @public
 */
export class ChangeTracker {
  private readonly watched = new Map<string, WatchedTableInfo>()
  private readonly watchedByFoldedName = new Map<string, string>()
  private lastSeq = 0n
  private readonly retentionMs: number
  private readonly changesTable: string
  private readonly pollBatchSize: number
  private changesTableReady = false
  private changesTableEnsured = false
  private watchedTablesCache: ReadonlySet<string> | null = null
  private readonly stmtCache = new StatementCache()
  private readonly pruneBoundaries = new PruneBoundaries()
  private lastPollAtTxBoundary = true
  private watchQueue: Promise<void> = Promise.resolve()

  constructor(options?: ChangeTrackerOptions) {
    this.retentionMs = options?.retention ?? DEFAULT_RETENTION_MS
    this.changesTable = options?.changesTable ?? CHANGES_TABLE
    this.pollBatchSize = options?.pollBatchSize ?? DEFAULT_POLL_BATCH_SIZE

    this.assertIdentifier(this.changesTable, 'changes table name')
  }

  /**
   * Installs change-capture triggers on a table, so that every later write to it adds a row to the change log.
   *
   * The tracker matches the name to an ordinary table as SQLite does, ignoring the case of ASCII letters, and records every change under the name that the schema stores. A name that matches no ordinary table, such as the name of a view or a virtual table, fails with `CDC_ERROR`. The tracker runs one watch at a time, so concurrent calls on one connection never open overlapping transactions.
   *
   * @param conn - The writer connection on which the tracker creates the triggers.
   * @param table - The name of the table to watch.
   */
  watch(conn: SQLiteConnection, table: string): Promise<void> {
    const watching = this.watchQueue.then(() => this.watchNow(conn, table))
    this.watchQueue = watching.catch(() => {})
    return watching
  }

  private async watchNow(conn: SQLiteConnection, requested: string): Promise<void> {
    this.assertIdentifier(requested, 'table name')
    if (isReservedIdentifier(requested)) {
      throw new ForbiddenSqlError(`Table '${requested}' is reserved for Sirannon and cannot be watched`)
    }
    await this.ensureChangesTable(conn)

    const table = await selectOrdinaryTableName(conn, requested)
    if (table === null) {
      throw new CDCError(`Table '${requested}' does not exist or is not an ordinary table`)
    }
    this.assertIdentifier(table, 'table name')

    const shape = await this.readShape(conn, table)
    if (shape.columns.length === 0) {
      throw new CDCError(`Table '${table}' does not exist or has no columns`)
    }

    const existing = this.watched.get(table)
    if (existing && sameCaptureShape(existing.shape, shape)) {
      return
    }
    if (existing || !(await cdcTriggersMatch(conn, this.changesTable, table, shape))) {
      await conn.transaction(async txConn => {
        await dropCdcTriggers(txConn, table)
        await installCdcTriggers(txConn, this.changesTable, table, shape)
      })
    }
    this.markWatched(table, shape)
  }

  /**
   * Drops a table's change-capture triggers and removes the table from the watched set.
   *
   * @param conn - The writer connection on which the tracker drops the triggers.
   * @param table - The name of the table to stop watching, in any letter case.
   */
  async unwatch(conn: SQLiteConnection, table: string): Promise<void> {
    const watched = this.watchedTableNamed(table)
    if (watched === undefined) {
      return
    }

    await dropCdcTriggers(conn, watched)
    await deleteReplacedRowsOfTable(conn, watched)
    this.markUnwatched(watched)
  }

  /**
   * Returns the stored name of the watched table that a name refers to, ignoring the case of ASCII letters as SQLite does.
   *
   * @param table - The name to look up.
   * @returns The stored name, or `undefined` when the tracker watches no such table.
   * @internal
   */
  watchedTableNamed(table: string): string | undefined {
    return this.watched.has(table) ? table : this.watchedByFoldedName.get(table.toLowerCase())
  }

  /**
   * Rebuilds the change-capture triggers on `conn` for every watched table
   * whose columns or unique keys have changed, without opening a nested transaction.
   *
   * Call it after a DDL statement inside an open transaction, so that the next
   * write in that transaction records the new columns in `new_data`. You can
   * also call it outside a transaction, in which case SQLite commits each
   * `CREATE TRIGGER` and `DROP TRIGGER` statement on its own. The method skips
   * a watched table that no longer exists, and it leaves that table's entry for
   * `pruneDroppedTables` to remove after the commit. A
   * driver error or an invalid column name propagates to the caller, so that
   * the caller can roll back its transaction.
   *
   * @param conn - The writer connection, which can hold an open transaction.
   * @param rebuilt - The watched tables whose triggers the caller dropped, which the method installs again whether or not their shape changed.
   * @internal
   */
  async refreshAllTriggersUsingConnection(conn: SQLiteConnection, rebuilt?: ReadonlySet<string>): Promise<void> {
    for (const table of Array.from(this.watched.keys())) {
      const existing = this.watched.get(table)
      if (!existing) continue

      const shape = await this.readShape(conn, table)
      if (shape.columns.length === 0) continue
      if (rebuilt?.has(table) !== true && sameCaptureShape(existing.shape, shape)) continue

      await dropCdcTriggers(conn, table)
      await installCdcTriggers(conn, this.changesTable, table, shape)
      this.watched.set(table, { table, shape })
    }
  }

  /**
   * Drops the change-capture triggers of each named table that the tracker
   * watches, and removes that table from the watched set.
   *
   * Call it once a transaction that drops watched tables commits, and skip the
   * call when that transaction rolls back. The method skips each table outside
   * the watched set and issues `DROP TRIGGER IF EXISTS`, so a second call leaves
   * the same state. When one transaction drops a table and creates another with
   * the same name, `refreshAllTriggersUsingConnection` can
   * install triggers on the new table. This method drops those triggers too, so
   * you have to call `watch` again for the new table.
   *
   * @internal
   */
  async pruneDroppedTables(conn: SQLiteConnection, tables: readonly string[]): Promise<void> {
    for (const named of tables) {
      const table = this.watchedTableNamed(named)
      if (table === undefined) {
        continue
      }
      await dropCdcTriggers(conn, table)
      await deleteReplacedRowsOfTable(conn, table)
      this.markUnwatched(table)
    }
  }

  /** @internal */
  async poll(conn: SQLiteConnection): Promise<ChangeEvent[]> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return []
      }
    }

    const result = await pollChanges(conn, this.stmtCache, this.changesTable, this.lastSeq, this.pollBatchSize)

    if (result === null) {
      this.lastPollAtTxBoundary = true
      return []
    }

    this.lastPollAtTxBoundary = result.atTxBoundary
    this.lastSeq = result.lastSeq

    return result.events
  }

  /** @internal */
  get pollEndedAtTxBoundary(): boolean {
    return this.lastPollAtTxBoundary
  }

  /**
   * Returns the sequence number of the last change that the tracker polled or skipped with `advanceToLatest`, so live subscribers receive only later changes.
   *
   * @internal
   */
  get cursor(): bigint {
    return this.lastSeq
  }

  /** @internal */
  async readSinceTables(
    conn: SQLiteConnection,
    tables: readonly string[],
    afterSeq: bigint,
    upToSeq: bigint,
    limit: number,
  ): Promise<ChangeEvent[]> {
    if (tables.length === 0) return []
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return []
      }
    }

    const stored = tables.map(table => this.watchedTableNamed(table) ?? table)
    return readSinceTables(conn, this.stmtCache, this.changesTable, stored, afterSeq, upToSeq, limit)
  }

  /**
   * Returns the lowest sequence number in the change log, or `null` when the log is empty or missing.
   *
   * @internal
   */
  async getMinSeq(conn: SQLiteConnection): Promise<bigint | null> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return null
      }
    }

    const stmt = await this.stmtCache.get(conn, 'min_seq', selectMinChangeSeqSql(this.changesTable))
    const row = (await stmt.get()) as { seq?: unknown } | undefined
    const seq = row?.seq
    if (seq === undefined || seq === null) {
      return null
    }
    return typeof seq === 'bigint' ? seq : BigInt(String(seq))
  }

  /** @internal */
  async advanceToLatest(conn: SQLiteConnection): Promise<void> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return
      }
    }

    const latestSeq = await selectMaxChangeSeq(conn, this.changesTable)
    if (latestSeq > this.lastSeq) {
      this.lastSeq = latestSeq
    }
  }

  /** @internal */
  async cleanup(conn: SQLiteConnection): Promise<number> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return 0
      }
    }

    const cutoff = Date.now() / 1000 - this.retentionMs / 1000
    const seqBound = this.computeSeqBound()
    const stmt = await this.stmtCache.get(conn, 'cleanup_coordinated', deleteChangesBeforeUpToSeqSql(this.changesTable))
    const result = await stmt.run(cutoff, seqBound.toString())
    if (this.watched.size > 0) await deleteAllReplacedRows(conn)
    return result.changes
  }

  /** @internal */
  setPruneBoundary(source: PruneBoundarySource, seq: bigint): void {
    this.pruneBoundaries.set(source, seq)
  }

  /** @internal */
  clearPruneBoundary(source: PruneBoundarySource): void {
    this.pruneBoundaries.clear(source)
  }

  /** @internal */
  get changeLogTable(): string {
    return this.changesTable
  }

  /** @internal */
  get watchedTables(): ReadonlySet<string> {
    if (!this.watchedTablesCache) {
      this.watchedTablesCache = new Set(this.watched.keys())
    }
    return this.watchedTablesCache
  }

  private computeSeqBound(): bigint {
    return seqBoundFor(this.pruneBoundaries.lowest(), this.lastSeq)
  }

  private assertIdentifier(name: string, label: string): void {
    if (!IDENTIFIER_RE.test(name)) {
      throw new CDCError(
        `Invalid ${label} '${name}': must contain only letters, digits, and underscores, and start with a letter or underscore`,
      )
    }
  }

  private async detectChangesTable(conn: SQLiteConnection): Promise<void> {
    if (await selectTableExists(conn, this.changesTable)) {
      this.changesTableReady = true
    }
  }

  private async ensureChangesTable(conn: SQLiteConnection): Promise<void> {
    if (this.changesTableEnsured) {
      return
    }

    await ensureChangesTable(conn, this.changesTable)
    await ensureReplacedRowsTable(conn)
    this.changesTableEnsured = true
    this.changesTableReady = true
  }

  private async readShape(conn: SQLiteConnection, table: string): Promise<CaptureShape> {
    const shape = await readCaptureShape(conn, table)
    for (const col of shape.columns) {
      this.assertIdentifier(col, `column name in table '${table}'`)
    }
    return shape
  }

  private markWatched(table: string, shape: CaptureShape): void {
    this.watched.set(table, { table, shape })
    this.watchedByFoldedName.set(table.toLowerCase(), table)
    this.watchedTablesCache = null
  }

  private markUnwatched(table: string): void {
    this.watched.delete(table)
    this.watchedByFoldedName.delete(table.toLowerCase())
    this.watchedTablesCache = null
  }
}
