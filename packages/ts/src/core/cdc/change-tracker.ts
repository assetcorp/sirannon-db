import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { CDCError } from '../errors.js'
import type { ChangeEvent } from '../types.js'
import type { ChangeRow, ChangeTrackerOptions, ColumnInfo, WatchedTableInfo } from './types.js'

const DEFAULT_RETENTION_MS = 3_600_000
const DEFAULT_CHANGES_TABLE = '_sirannon_changes'
const DEFAULT_POLL_BATCH_SIZE = 1000
const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export class ChangeTracker {
  private readonly watched = new Map<string, WatchedTableInfo>()
  private lastSeq = 0
  private readonly retentionMs: number
  private readonly changesTable: string
  private readonly pollBatchSize: number
  private readonly replication: boolean
  private changesTableReady = false
  private watchedTablesCache: ReadonlySet<string> | null = null
  private readonly stmtCache = new WeakMap<SQLiteConnection, Map<string, Promise<SQLiteStatement>>>()

  constructor(options?: ChangeTrackerOptions) {
    this.retentionMs = options?.retention ?? DEFAULT_RETENTION_MS
    this.changesTable = options?.changesTable ?? DEFAULT_CHANGES_TABLE
    this.pollBatchSize = options?.pollBatchSize ?? DEFAULT_POLL_BATCH_SIZE
    this.replication = options?.replication ?? false

    this.assertIdentifier(this.changesTable, 'changes table name')
  }

  async watch(conn: SQLiteConnection, table: string): Promise<void> {
    this.assertIdentifier(table, 'table name')
    await this.ensureChangesTable(conn)

    const columns = await this.getColumns(conn, table)
    if (columns.length === 0) {
      throw new CDCError(`Table '${table}' does not exist or has no columns`)
    }

    for (const col of columns) {
      this.assertIdentifier(col, `column name in table '${table}'`)
    }

    const pkColumns = await this.getPkColumns(conn, table)
    const existing = this.watched.get(table)

    if (existing) {
      const same = existing.columns.length === columns.length && existing.columns.every((col, i) => col === columns[i])
      if (same) {
        return
      }
      await conn.transaction(async txConn => {
        await this.dropTriggers(txConn, table)
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

    await this.dropTriggers(conn, table)
    this.watched.delete(table)
    this.watchedTablesCache = null
  }

  async poll(conn: SQLiteConnection): Promise<ChangeEvent[]> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return []
      }
    }

    const stmt = await this.getStmt(
      conn,
      'poll',
      `SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data
			 FROM "${this.changesTable}"
			 WHERE seq > ?
			 ORDER BY seq ASC
			 LIMIT ?`,
    )

    const rows = (await stmt.all(this.lastSeq, this.pollBatchSize)) as ChangeRow[]

    if (rows.length === 0) {
      return []
    }

    const events: ChangeEvent[] = []

    for (const row of rows) {
      events.push({
        type: row.operation.toLowerCase() as 'insert' | 'update' | 'delete',
        table: row.table_name,
        row: row.new_data ? JSON.parse(row.new_data) : {},
        oldRow: row.old_data ? JSON.parse(row.old_data) : undefined,
        seq: BigInt(row.seq),
        timestamp: row.changed_at,
      })
    }

    this.lastSeq = rows[rows.length - 1].seq

    return events
  }

  async cleanup(conn: SQLiteConnection): Promise<number> {
    if (!this.changesTableReady) {
      await this.detectChangesTable(conn)
      if (!this.changesTableReady) {
        return 0
      }
    }

    const cutoff = Date.now() / 1000 - this.retentionMs / 1000

    if (this.lastSeq > 0) {
      const stmt = await this.getStmt(
        conn,
        'cleanup_coordinated',
        `DELETE FROM "${this.changesTable}" WHERE changed_at < ? AND seq <= ?`,
      )
      const result = await stmt.run(cutoff, this.lastSeq)
      return result.changes
    }

    const stmt = await this.getStmt(conn, 'cleanup', `DELETE FROM "${this.changesTable}" WHERE changed_at < ?`)
    const result = await stmt.run(cutoff)
    return result.changes
  }

  get watchedTables(): ReadonlySet<string> {
    if (!this.watchedTablesCache) {
      this.watchedTablesCache = new Set(this.watched.keys())
    }
    return this.watchedTablesCache
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
    if (this.changesTableReady) {
      return
    }

    if (this.replication) {
      await conn.exec(`
CREATE TABLE IF NOT EXISTS "${this.changesTable}" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT,
  node_id TEXT NOT NULL DEFAULT '',
  tx_id TEXT NOT NULL DEFAULT '',
  hlc TEXT NOT NULL DEFAULT ''
)`)
      await conn.exec(
        `CREATE INDEX IF NOT EXISTS "idx_${this.changesTable}_changed_at" ON "${this.changesTable}" (changed_at)`,
      )
      await conn.exec(
        `CREATE INDEX IF NOT EXISTS "idx_${this.changesTable}_node_id" ON "${this.changesTable}" (node_id)`,
      )
      await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${this.changesTable}_hlc" ON "${this.changesTable}" (hlc)`)
    } else {
      await conn.exec(`
CREATE TABLE IF NOT EXISTS "${this.changesTable}" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT
)`)
      await conn.exec(
        `CREATE INDEX IF NOT EXISTS "idx_${this.changesTable}_changed_at" ON "${this.changesTable}" (changed_at)`,
      )
    }
    this.changesTableReady = true
  }

  private async getColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
    const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
    const info = (await stmt.all()) as ColumnInfo[]
    return info.map(col => col.name)
  }

  private async getPkColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
    const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
    const info = (await stmt.all()) as ColumnInfo[]
    return info
      .filter(col => col.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map(col => col.name)
  }

  private async dropTriggers(conn: SQLiteConnection, table: string): Promise<void> {
    await conn.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_insert"`)
    await conn.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_update"`)
    await conn.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_delete"`)
  }

  private async getStmt(conn: SQLiteConnection, key: string, sql: string): Promise<SQLiteStatement> {
    let stmts = this.stmtCache.get(conn)
    if (!stmts) {
      stmts = new Map()
      this.stmtCache.set(conn, stmts)
    }

    const existing = stmts.get(key)
    if (existing) return existing

    const pending = conn.prepare(sql)
    stmts.set(key, pending)

    try {
      return await pending
    } catch (err) {
      stmts.delete(key)
      throw err
    }
  }

  private buildPkRef(pkColumns: string[], ref: 'NEW' | 'OLD'): string {
    if (pkColumns.length === 0) {
      return `${ref}.rowid`
    }
    if (pkColumns.length === 1) {
      return `${ref}."${this.escId(pkColumns[0])}"`
    }
    return pkColumns.map(col => `${ref}."${this.escId(col)}"`).join(" || '-' || ")
  }

  private async installTriggers(
    conn: SQLiteConnection,
    table: string,
    columns: string[],
    pkColumns: string[],
  ): Promise<void> {
    const newJson = this.buildJsonObject(columns, 'NEW')
    const oldJson = this.buildJsonObject(columns, 'OLD')
    const newPk = this.buildPkRef(pkColumns, 'NEW')
    const oldPk = this.buildPkRef(pkColumns, 'OLD')

    const replCols = this.replication ? ', node_id, tx_id, hlc' : ''
    const replVals = this.replication ? ", '', '', ''" : ''

    await conn.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_insert"
			AFTER INSERT ON "${table}"
			BEGIN
				INSERT INTO "${this.changesTable}" (table_name, operation, row_id, new_data${replCols})
				VALUES ('${table}', 'INSERT', ${newPk}, ${newJson}${replVals});
			END
		`)

    await conn.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_update"
			AFTER UPDATE ON "${table}"
			BEGIN
				INSERT INTO "${this.changesTable}" (table_name, operation, row_id, old_data, new_data${replCols})
				VALUES ('${table}', 'UPDATE', ${newPk}, ${oldJson}, ${newJson}${replVals});
			END
		`)

    await conn.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_delete"
			AFTER DELETE ON "${table}"
			BEGIN
				INSERT INTO "${this.changesTable}" (table_name, operation, row_id, old_data${replCols})
				VALUES ('${table}', 'DELETE', ${oldPk}, ${oldJson}${replVals});
			END
		`)
  }

  private buildJsonObject(columns: string[], ref: 'NEW' | 'OLD'): string {
    const pairs = columns.map(col => `'${this.escStr(col)}', ${ref}."${this.escId(col)}"`).join(', ')
    return `json_object(${pairs})`
  }

  private escId(name: string): string {
    return name.replace(/"/g, '""')
  }

  private escStr(name: string): string {
    return name.replace(/'/g, "''")
  }
}
