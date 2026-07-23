import type { SQLiteConnection } from '../driver/types.js'
import { CDCError, ForbiddenSqlError } from '../errors.js'
import { CHANGES_TABLE, isReservedIdentifier } from '../internal-tables.js'
import { ensureChangesTable } from '../system-catalog/index.js'
import type { ChangeEvent } from '../types.js'
import { decodeTaggedValues } from './encoding.js'
import { StatementCache } from './statement-cache.js'
import { tableColumnNames, tablePkColumns } from './table-info.js'
import { dropCdcTriggers, installCdcTriggers } from './trigger-sql.js'
import type { ChangeRow, ChangeTrackerOptions, WatchedTableInfo } from './types.js'

const DEFAULT_RETENTION_MS = 3_600_000
const DEFAULT_POLL_BATCH_SIZE = 1000
const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export class ChangeTracker {
  private readonly watched = new Map<string, WatchedTableInfo>()
  private lastSeq = 0n
  private readonly retentionMs: number
  private readonly changesTable: string
  private readonly pollBatchSize: number
  private changesTableReady = false
  private changesTableEnsured = false
  private watchedTablesCache: ReadonlySet<string> | null = null
  private readonly stmtCache = new StatementCache()
  private pruneBoundary: bigint | null = null

  constructor(options?: ChangeTrackerOptions) {
    this.retentionMs = options?.retention ?? DEFAULT_RETENTION_MS
    this.changesTable = options?.changesTable ?? CHANGES_TABLE
    this.pollBatchSize = options?.pollBatchSize ?? DEFAULT_POLL_BATCH_SIZE

    this.assertIdentifier(this.changesTable, 'changes table name')
  }

  async watch(conn: SQLiteConnection, table: string): Promise<void> {
    this.assertIdentifier(table, 'table name')
    if (isReservedIdentifier(table)) {
      throw new ForbiddenSqlError(`Table '${table}' is reserved for Sirannon and cannot be watched`)
    }
    await this.ensureChangesTable(conn)

    const columns = await tableColumnNames(conn, table)
    if (columns.length === 0) {
      throw new CDCError(`Table '${table}' does not exist or has no columns`)
    }

    for (const col of columns) {
      this.assertIdentifier(col, `column name in table '${table}'`)
    }

    const pkColumns = await tablePkColumns(conn, table)
    const existing = this.watched.get(table)

    if (existing) {
      const same = existing.columns.length === columns.length && existing.columns.every((col, i) => col === columns[i])
      if (same) {
        return
      }
      await conn.transaction(async txConn => {
        await dropCdcTriggers(txConn, table)
        await this.installTriggers(txConn, table, columns, pkColumns)
      })
    } else {
      await this.installTriggers(conn, table, columns, pkColumns)
    }
    this.watched.set(table, { table, columns, pkColumns })
    this.watchedTablesCache = null
  }

  async unwatch(conn: SQLiteConnection, table: string): Promise<void> {
    if (!this.watched.has(table)) {
      return
    }

    await dropCdcTriggers(conn, table)
    this.watched.delete(table)
    this.watchedTablesCache = null
  }

  /**
   * Rebuilds CDC triggers for every watched table directly on the supplied
   * connection, without opening a nested transaction.
   *
   * Required when a caller has already issued `BEGIN` on `conn` and runs a
   * DDL statement that changes a watched table's column list: the next DML
   * inside the same transaction must see triggers compiled against the new
   * columns, otherwise CDC `new_data` silently omits them. Callers that are
   * not inside an active transaction may also use this method; CREATE
   * TRIGGER and DROP TRIGGER statements are committed individually by the
   * driver in that case.
   *
   * Failure semantics:
   * - If a watched table no longer exists (e.g. it was just dropped by the
   *   DDL), it is skipped. The watched-map entry is left alone so a separate
   *   cleanup path can handle it; throwing here would roll back the user's
   *   transaction over a benign condition.
   * - If reading column metadata succeeds but the column list is unchanged,
   *   no triggers are touched.
   * - If reading column metadata succeeds and the column list differs from
   *   the cached one, the existing triggers are dropped and reinstalled on
   *   `conn`. The cached column list is updated on success.
   * - Any other error (driver failure, identifier validation failure) is
   *   rethrown so the caller's transaction can roll back deterministically.
   */
  async refreshAllTriggersUsingConnection(conn: SQLiteConnection): Promise<void> {
    const tables = Array.from(this.watched.keys())
    let anyMutated = false
    for (const table of tables) {
      const existing = this.watched.get(table)
      if (!existing) continue

      const columns = await tableColumnNames(conn, table)
      if (columns.length === 0) {
        continue
      }

      for (const col of columns) {
        this.assertIdentifier(col, `column name in table '${table}'`)
      }

      const same = existing.columns.length === columns.length && existing.columns.every((col, i) => col === columns[i])
      if (same) {
        continue
      }

      const pkColumns = await tablePkColumns(conn, table)
      await dropCdcTriggers(conn, table)
      await this.installTriggers(conn, table, columns, pkColumns)
      this.watched.set(table, { table, columns, pkColumns })
      anyMutated = true
    }
    if (anyMutated) {
      this.watchedTablesCache = null
    }
  }

  /**
   * Removes the supplied tables from the watched map and drops any leftover
   * CDC triggers carrying their identifier on the supplied connection.
   *
   * Intended to be called after a DDL transaction that dropped one or more
   * watched tables has committed. On rollback the caller must not invoke
   * this method; the rollback semantics rely on the caller discarding its
   * captured drop list before reaching this call.
   *
   * Idempotent: tables not currently in the watched map are silently
   * skipped. `dropCdcTriggers` issues `DROP TRIGGER IF EXISTS` so calling
   * twice in succession produces the same state.
   *
   * Defence in depth: even after a `DROP TABLE`, the in-transaction trigger
   * refresh path may have observed a freshly-created table of the same name
   * (the DROP and CREATE happened inside the same transaction) and
   * re-installed triggers compiled against the new schema. Those triggers
   * are dropped here so the recreated table starts with a clean slate and
   * the caller must explicitly `watch` it again.
   */
  async pruneDroppedTables(conn: SQLiteConnection, tables: readonly string[]): Promise<void> {
    let mutated = false
    for (const table of tables) {
      if (!this.watched.has(table)) {
        continue
      }
      await dropCdcTriggers(conn, table)
      this.watched.delete(table)
      mutated = true
    }
    if (mutated) {
      this.watchedTablesCache = null
    }
  }

  async poll(conn: SQLiteConnection): Promise<ChangeEvent[]> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return []
      }
    }

    const stmt = await this.stmtCache.get(
      conn,
      'poll',
      `SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, hlc
			 FROM "${this.changesTable}"
			 WHERE seq > ?
			 ORDER BY seq ASC
			 LIMIT ?`,
    )

    const rows = (await stmt.all(this.lastSeq.toString(), this.pollBatchSize)) as ChangeRow[]

    if (rows.length === 0) {
      return []
    }

    const events = rows.map(row => this.rowToEvent(row))

    this.lastSeq = BigInt(rows[rows.length - 1].seq)

    return events
  }

  /** The highest seq already polled; live subscribers receive events beyond it. */
  get cursor(): bigint {
    return this.lastSeq
  }

  /**
   * Reads retained changes for one table with seq in `(afterSeq, upToSeq]`,
   * ordered ascending and capped at `limit`. Used to replay history to a
   * resuming subscriber without disturbing the shared poll cursor.
   */
  async readSince(
    conn: SQLiteConnection,
    table: string,
    afterSeq: bigint,
    upToSeq: bigint,
    limit: number,
  ): Promise<ChangeEvent[]> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return []
      }
    }

    const stmt = await this.stmtCache.get(
      conn,
      'read_since',
      `SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, hlc
			 FROM "${this.changesTable}"
			 WHERE table_name = ? AND seq > ? AND seq <= ?
			 ORDER BY seq ASC
			 LIMIT ?`,
    )

    const rows = (await stmt.all(table, afterSeq.toString(), upToSeq.toString(), limit)) as ChangeRow[]
    return rows.map(row => this.rowToEvent(row))
  }

  /** The lowest retained seq, or `null` when the change log is empty. */
  async getMinSeq(conn: SQLiteConnection): Promise<bigint | null> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return null
      }
    }

    const stmt = await this.stmtCache.get(conn, 'min_seq', `SELECT MIN(seq) AS seq FROM "${this.changesTable}"`)
    const row = (await stmt.get()) as { seq?: unknown } | undefined
    const seq = row?.seq
    if (seq === undefined || seq === null) {
      return null
    }
    return typeof seq === 'bigint' ? seq : BigInt(String(seq))
  }

  private rowToEvent(row: ChangeRow): ChangeEvent {
    return {
      type: row.operation.toLowerCase() as 'insert' | 'update' | 'delete',
      table: row.table_name,
      row: row.new_data ? (decodeTaggedValues(JSON.parse(row.new_data)) as Record<string, unknown>) : {},
      oldRow: row.old_data ? (decodeTaggedValues(JSON.parse(row.old_data)) as Record<string, unknown>) : undefined,
      seq: BigInt(row.seq),
      timestamp: row.changed_at,
      ...(row.node_id ? { origin: row.node_id } : {}),
      ...(row.hlc ? { hlc: row.hlc } : {}),
    }
  }

  async advanceToLatest(conn: SQLiteConnection): Promise<void> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return
      }
    }

    const stmt = await this.stmtCache.get(conn, 'latest_seq', `SELECT MAX(seq) AS seq FROM "${this.changesTable}"`)
    const row = (await stmt.get()) as { seq?: unknown } | undefined
    const seq = row?.seq
    if (seq === undefined || seq === null) {
      return
    }

    const latestSeq = typeof seq === 'bigint' ? seq : BigInt(String(seq))
    if (latestSeq > this.lastSeq) {
      this.lastSeq = latestSeq
    }
  }

  async cleanup(conn: SQLiteConnection): Promise<number> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return 0
      }
    }

    const cutoff = Date.now() / 1000 - this.retentionMs / 1000
    const seqBound = this.computeSeqBound()

    if (seqBound !== null) {
      const stmt = await this.stmtCache.get(
        conn,
        'cleanup_coordinated',
        `DELETE FROM "${this.changesTable}" WHERE changed_at < ? AND seq <= ?`,
      )
      const result = await stmt.run(cutoff, seqBound.toString())
      return result.changes
    }

    const stmt = await this.stmtCache.get(conn, 'cleanup', `DELETE FROM "${this.changesTable}" WHERE changed_at < ?`)
    const result = await stmt.run(cutoff)
    return result.changes
  }

  setPruneBoundary(seq: bigint): void {
    this.pruneBoundary = seq
  }

  clearPruneBoundary(): void {
    this.pruneBoundary = null
  }

  get watchedTables(): ReadonlySet<string> {
    if (!this.watchedTablesCache) {
      this.watchedTablesCache = new Set(this.watched.keys())
    }
    return this.watchedTablesCache
  }

  private computeSeqBound(): bigint | null {
    const boundary = this.pruneBoundary

    if (this.lastSeq > 0n && boundary !== null) {
      return this.lastSeq < boundary ? this.lastSeq : boundary
    }

    if (boundary !== null) {
      return boundary
    }

    if (this.lastSeq > 0n) {
      return this.lastSeq
    }

    return null
  }

  private assertIdentifier(name: string, label: string): void {
    if (!IDENTIFIER_RE.test(name)) {
      throw new CDCError(
        `Invalid ${label} '${name}': must contain only letters, digits, and underscores, and start with a letter or underscore`,
      )
    }
  }

  private async detectChangesTable(conn: SQLiteConnection): Promise<void> {
    const stmt = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")
    const row = await stmt.get(this.changesTable)
    if (row) {
      this.changesTableReady = true
    }
  }

  private async ensureChangesTable(conn: SQLiteConnection): Promise<void> {
    if (this.changesTableEnsured) {
      return
    }

    await ensureChangesTable(conn, this.changesTable)
    this.changesTableEnsured = true
    this.changesTableReady = true
  }

  private async installTriggers(
    conn: SQLiteConnection,
    table: string,
    columns: string[],
    pkColumns: string[],
  ): Promise<void> {
    await installCdcTriggers(conn, this.changesTable, table, columns, pkColumns)
  }
}
