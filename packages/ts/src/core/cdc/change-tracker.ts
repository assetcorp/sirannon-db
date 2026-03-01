import type Database from 'better-sqlite3'
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
  private changesTableReady = false
  private watchedTablesCache: ReadonlySet<string> | null = null
  private readonly stmtCache = new WeakMap<Database.Database, Map<string, Database.Statement>>()

  constructor(options?: ChangeTrackerOptions) {
    this.retentionMs = options?.retention ?? DEFAULT_RETENTION_MS
    this.changesTable = options?.changesTable ?? DEFAULT_CHANGES_TABLE
    this.pollBatchSize = options?.pollBatchSize ?? DEFAULT_POLL_BATCH_SIZE

    this.assertIdentifier(this.changesTable, 'changes table name')
  }

  watch(db: Database.Database, table: string): void {
    this.assertIdentifier(table, 'table name')
    this.ensureChangesTable(db)

    const columns = this.getColumns(db, table)
    if (columns.length === 0) {
      throw new CDCError(`Table '${table}' does not exist or has no columns`)
    }

    for (const col of columns) {
      this.assertIdentifier(col, `column name in table '${table}'`)
    }

    const pkColumns = this.getPkColumns(db, table)
    const existing = this.watched.get(table)

    if (existing) {
      const same = existing.columns.length === columns.length && existing.columns.every((col, i) => col === columns[i])
      if (same) {
        return
      }
      this.dropTriggers(db, table)
    }

    this.installTriggers(db, table, columns, pkColumns)
    this.watched.set(table, { table, columns, pkColumns })
    this.watchedTablesCache = null
  }

  unwatch(db: Database.Database, table: string): void {
    if (!this.watched.has(table)) {
      return
    }

    this.dropTriggers(db, table)
    this.watched.delete(table)
    this.watchedTablesCache = null
  }

  poll(db: Database.Database): ChangeEvent[] {
    if (!this.changesTableReady) {
      this.detectChangesTable(db)
      if (!this.changesTableReady) {
        return []
      }
    }

    const stmt = this.getStmt(
      db,
      'poll',
      `SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data
			 FROM "${this.changesTable}"
			 WHERE seq > ?
			 ORDER BY seq ASC
			 LIMIT ?`,
    )

    const rows = stmt.all(this.lastSeq, this.pollBatchSize) as ChangeRow[]

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

  cleanup(db: Database.Database): number {
    if (!this.changesTableReady) {
      this.detectChangesTable(db)
      if (!this.changesTableReady) {
        return 0
      }
    }

    const cutoff = Date.now() / 1000 - this.retentionMs / 1000

    if (this.lastSeq > 0) {
      const stmt = this.getStmt(
        db,
        'cleanup_coordinated',
        `DELETE FROM "${this.changesTable}" WHERE changed_at < ? AND seq <= ?`,
      )
      return stmt.run(cutoff, this.lastSeq).changes
    }

    const stmt = this.getStmt(db, 'cleanup', `DELETE FROM "${this.changesTable}" WHERE changed_at < ?`)
    return stmt.run(cutoff).changes
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

  private detectChangesTable(db: Database.Database): void {
    const row = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(this.changesTable)
    if (row) {
      this.changesTableReady = true
    }
  }

  private ensureChangesTable(db: Database.Database): void {
    if (this.changesTableReady) {
      return
    }

    db.exec(`
CREATE TABLE IF NOT EXISTS "${this.changesTable}" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT
)`)
    db.exec(`CREATE INDEX IF NOT EXISTS "idx_${this.changesTable}_changed_at" ON "${this.changesTable}" (changed_at)`)
    this.changesTableReady = true
  }

  private getColumns(db: Database.Database, table: string): string[] {
    const info = db.prepare(`PRAGMA table_info("${table}")`).all() as ColumnInfo[]
    return info.map(col => col.name)
  }

  private getPkColumns(db: Database.Database, table: string): string[] {
    const info = db.prepare(`PRAGMA table_info("${table}")`).all() as ColumnInfo[]
    return info
      .filter(col => col.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map(col => col.name)
  }

  private dropTriggers(db: Database.Database, table: string): void {
    db.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_insert"`)
    db.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_update"`)
    db.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_delete"`)
  }

  private getStmt(db: Database.Database, key: string, sql: string): Database.Statement {
    let stmts = this.stmtCache.get(db)
    if (!stmts) {
      stmts = new Map()
      this.stmtCache.set(db, stmts)
    }

    let stmt = stmts.get(key)
    if (!stmt) {
      stmt = db.prepare(sql)
      stmts.set(key, stmt)
    }
    return stmt
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

  private installTriggers(db: Database.Database, table: string, columns: string[], pkColumns: string[]): void {
    const newJson = this.buildJsonObject(columns, 'NEW')
    const oldJson = this.buildJsonObject(columns, 'OLD')
    const newPk = this.buildPkRef(pkColumns, 'NEW')
    const oldPk = this.buildPkRef(pkColumns, 'OLD')

    db.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_insert"
			AFTER INSERT ON "${table}"
			BEGIN
				INSERT INTO "${this.changesTable}" (table_name, operation, row_id, new_data)
				VALUES ('${table}', 'INSERT', ${newPk}, ${newJson});
			END
		`)

    db.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_update"
			AFTER UPDATE ON "${table}"
			BEGIN
				INSERT INTO "${this.changesTable}" (table_name, operation, row_id, old_data, new_data)
				VALUES ('${table}', 'UPDATE', ${newPk}, ${oldJson}, ${newJson});
			END
		`)

    db.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_delete"
			AFTER DELETE ON "${table}"
			BEGIN
				INSERT INTO "${this.changesTable}" (table_name, operation, row_id, old_data)
				VALUES ('${table}', 'DELETE', ${oldPk}, ${oldJson});
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
