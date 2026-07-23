import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { INTERNAL_TABLE_PREFIX, SYNC_STATE_TABLE } from '../../core/internal-tables.js'
import { IDENTIFIER_RE, validateDdlSafety } from '../../core/sync/validators.js'
import { ensureChangesTable, ensureMetaTable, ensureReplicationStateTables } from '../../core/system-catalog/index.js'
import { ReplicationError } from '../errors.js'

export class SchemaOps {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly changesTable: string,
  ) {}

  async ensureReplicationTables(): Promise<void> {
    await ensureChangesTable(this.conn, this.changesTable, { replication: true })
    await ensureReplicationStateTables(this.conn)
    await ensureMetaTable(this.conn)
  }

  async dumpSchema(conn: SQLiteConnection, excludePrefix: string = INTERNAL_TABLE_PREFIX): Promise<string[]> {
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
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '${INTERNAL_TABLE_PREFIX}%' AND name NOT LIKE 'sqlite_%'`,
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
        await tx.exec(`DELETE FROM ${SYNC_STATE_TABLE} WHERE table_name != '__sync_meta__'`)
      })
    } finally {
      await conn.exec('PRAGMA foreign_keys = ON')
    }
  }
}
