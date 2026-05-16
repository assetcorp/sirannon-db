import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { ReplicationError } from '../errors.js'
import { IDENTIFIER_RE, validateDdlSafety } from './validators.js'

export class SchemaOps {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly changesTable: string,
  ) {}

  async ensureReplicationTables(): Promise<void> {
    await this.conn.exec(`
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

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_peer_state (
	peer_node_id TEXT PRIMARY KEY,
	last_acked_seq INTEGER NOT NULL DEFAULT 0,
	last_received_hlc TEXT NOT NULL DEFAULT '',
	updated_at REAL NOT NULL
)`)

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_applied_changes (
	source_node_id TEXT NOT NULL,
	source_seq INTEGER NOT NULL,
	applied_at REAL NOT NULL,
	PRIMARY KEY (source_node_id, source_seq)
)`)

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_column_versions (
	table_name TEXT NOT NULL,
	row_id TEXT NOT NULL,
	column_name TEXT NOT NULL,
	hlc TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (table_name, row_id, column_name)
)`)

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_sync_state (
	table_name TEXT PRIMARY KEY,
	status TEXT NOT NULL DEFAULT 'pending',
	row_count INTEGER NOT NULL DEFAULT 0,
	pk_hash TEXT NOT NULL DEFAULT '',
	snapshot_seq INTEGER,
	source_peer_id TEXT,
	started_at REAL,
	completed_at REAL,
	request_id TEXT
)`)
  }

  async dumpSchema(conn: SQLiteConnection, excludePrefix: string = '_sirannon_'): Promise<string[]> {
    const stmt = await conn.prepare(
      `SELECT type, name, sql FROM sqlite_master
       WHERE type IN ('table', 'index') AND name NOT LIKE ? AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL`,
    )
    const rows = (await stmt.all(`${excludePrefix}%`)) as Array<{ type: string; name: string; sql: string }>

    const filtered = rows.filter(row => {
      if (row.name.startsWith(excludePrefix)) return false
      if (row.name.startsWith('sqlite_')) return false
      return validateDdlSafety(row.sql)
    })

    const tables: Array<{ name: string; sql: string }> = []
    const indexes: Array<{ sql: string }> = []

    for (const row of filtered) {
      if (row.type === 'table') {
        tables.push({ name: row.name, sql: row.sql })
      } else {
        indexes.push({ sql: row.sql })
      }
    }

    const tableOrder = await this.getTablesInFkOrder(conn)
    const orderMap = new Map<string, number>()
    for (let i = 0; i < tableOrder.length; i++) {
      orderMap.set(tableOrder[i], i)
    }

    tables.sort((a, b) => (orderMap.get(a.name) ?? 999) - (orderMap.get(b.name) ?? 999))

    const result: string[] = []
    for (const t of tables) {
      result.push(t.sql)
    }
    for (const idx of indexes) {
      result.push(idx.sql)
    }
    return result
  }

  async getTablesInFkOrder(conn: SQLiteConnection): Promise<string[]> {
    const stmt = await conn.prepare(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_sirannon_%' AND name NOT LIKE 'sqlite_%'`,
    )
    const tableRows = (await stmt.all()) as Array<{ name: string }>
    const tableNames = tableRows.map(r => r.name)

    const adjacency = new Map<string, Set<string>>()
    const inDegree = new Map<string, number>()

    for (const name of tableNames) {
      adjacency.set(name, new Set())
      inDegree.set(name, 0)
    }

    for (const name of tableNames) {
      const fkStmt = await conn.prepare(`PRAGMA foreign_key_list("${name}")`)
      const fks = (await fkStmt.all()) as Array<{ table: string }>
      for (const fk of fks) {
        if (tableNames.includes(fk.table) && fk.table !== name) {
          const deps = adjacency.get(fk.table)
          if (deps && !deps.has(name)) {
            deps.add(name)
            inDegree.set(name, (inDegree.get(name) ?? 0) + 1)
          }
        }
      }
    }

    const queue: string[] = []
    for (const name of tableNames) {
      if ((inDegree.get(name) ?? 0) === 0) {
        queue.push(name)
      }
    }

    const sorted: string[] = []
    while (queue.length > 0) {
      const current = queue.shift()
      if (current === undefined) break
      sorted.push(current)
      const deps = adjacency.get(current)
      if (deps) {
        for (const dep of deps) {
          const newDeg = (inDegree.get(dep) ?? 1) - 1
          inDegree.set(dep, newDeg)
          if (newDeg === 0) {
            queue.push(dep)
          }
        }
      }
    }

    if (sorted.length < tableNames.length) {
      const remaining = tableNames.filter(n => !sorted.includes(n)).sort()
      sorted.push(...remaining)
    }

    return sorted
  }

  async wipeTables(conn: SQLiteConnection, tables: string[], tracker: ChangeTracker): Promise<void> {
    for (const table of tables) {
      if (!IDENTIFIER_RE.test(table)) {
        throw new ReplicationError(`Invalid table name: ${table}`)
      }
    }

    await conn.exec('PRAGMA foreign_keys = OFF')
    try {
      await conn.transaction(async tx => {
        for (const table of tables) {
          await tracker.unwatch(tx, table)
        }
        const reversed = [...tables].reverse()
        for (const table of reversed) {
          await tx.exec(`DELETE FROM "${table}"`)
        }
        await tx.exec(`DELETE FROM _sirannon_sync_state WHERE table_name != '__sync_meta__'`)
      })
    } finally {
      await conn.exec('PRAGMA foreign_keys = ON')
    }
  }
}
