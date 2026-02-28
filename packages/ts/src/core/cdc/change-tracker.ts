import type Database from 'better-sqlite3'
import { CDCError } from '../errors.js'
import type { ChangeEvent } from '../types.js'
import type {
	ChangeRow,
	ChangeTrackerOptions,
	ColumnInfo,
	WatchedTableInfo,
} from './types.js'

const DEFAULT_RETENTION_MS = 3_600_000
const DEFAULT_CHANGES_TABLE = '_sirannon_changes'

export class ChangeTracker {
	private readonly watched = new Map<string, WatchedTableInfo>()
	private lastSeq = 0
	private readonly retentionMs: number
	private readonly changesTable: string
	private changesTableReady = false

	constructor(options?: ChangeTrackerOptions) {
		this.retentionMs = options?.retention ?? DEFAULT_RETENTION_MS
		this.changesTable = options?.changesTable ?? DEFAULT_CHANGES_TABLE
	}

	watch(db: Database.Database, table: string): void {
		if (this.watched.has(table)) {
			return
		}

		this.ensureChangesTable(db)
		const columns = this.getColumns(db, table)

		if (columns.length === 0) {
			throw new CDCError(`Table '${table}' does not exist or has no columns`)
		}

		this.installTriggers(db, table, columns)
		this.watched.set(table, { table, columns })
	}

	unwatch(db: Database.Database, table: string): void {
		if (!this.watched.has(table)) {
			return
		}

		db.exec(`DROP TRIGGER IF EXISTS _sirannon_trg_${table}_insert`)
		db.exec(`DROP TRIGGER IF EXISTS _sirannon_trg_${table}_update`)
		db.exec(`DROP TRIGGER IF EXISTS _sirannon_trg_${table}_delete`)

		this.watched.delete(table)
	}

	poll(db: Database.Database): ChangeEvent[] {
		if (!this.changesTableReady) {
			return []
		}

		const rows = db
			.prepare(
				`SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data
				 FROM ${this.changesTable}
				 WHERE seq > ?
				 ORDER BY seq ASC`,
			)
			.all(this.lastSeq) as ChangeRow[]

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
			return 0
		}

		const cutoff = Date.now() / 1000 - this.retentionMs / 1000
		const result = db
			.prepare(`DELETE FROM ${this.changesTable} WHERE changed_at < ?`)
			.run(cutoff)

		return result.changes
	}

	get watchedTables(): ReadonlySet<string> {
		return new Set(this.watched.keys())
	}

	private ensureChangesTable(db: Database.Database): void {
		if (this.changesTableReady) {
			return
		}
		db.exec(`
CREATE TABLE IF NOT EXISTS ${this.changesTable} (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id INTEGER NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT
)`)
		this.changesTableReady = true
	}

	private getColumns(db: Database.Database, table: string): string[] {
		const info = db
			.prepare(`PRAGMA table_info('${table}')`)
			.all() as ColumnInfo[]
		return info.map(col => col.name)
	}

	private installTriggers(
		db: Database.Database,
		table: string,
		columns: string[],
	): void {
		const newJson = this.buildJsonObject(columns, 'NEW')
		const oldJson = this.buildJsonObject(columns, 'OLD')

		db.exec(`
			CREATE TRIGGER IF NOT EXISTS _sirannon_trg_${table}_insert
			AFTER INSERT ON "${table}"
			BEGIN
				INSERT INTO ${this.changesTable} (table_name, operation, row_id, new_data)
				VALUES ('${table}', 'INSERT', NEW.rowid, ${newJson});
			END
		`)

		db.exec(`
			CREATE TRIGGER IF NOT EXISTS _sirannon_trg_${table}_update
			AFTER UPDATE ON "${table}"
			BEGIN
				INSERT INTO ${this.changesTable} (table_name, operation, row_id, old_data, new_data)
				VALUES ('${table}', 'UPDATE', NEW.rowid, ${oldJson}, ${newJson});
			END
		`)

		db.exec(`
			CREATE TRIGGER IF NOT EXISTS _sirannon_trg_${table}_delete
			AFTER DELETE ON "${table}"
			BEGIN
				INSERT INTO ${this.changesTable} (table_name, operation, row_id, old_data)
				VALUES ('${table}', 'DELETE', OLD.rowid, ${oldJson});
			END
		`)
	}

	private buildJsonObject(columns: string[], ref: 'NEW' | 'OLD'): string {
		const pairs = columns.map(col => `'${col}', ${ref}."${col}"`).join(', ')
		return `json_object(${pairs})`
	}
}
