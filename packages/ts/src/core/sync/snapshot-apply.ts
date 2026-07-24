import type { ChangeTracker } from '../cdc/change-tracker.js'
import type { SQLiteConnection } from '../driver/types.js'
import { META_TABLE } from '../internal-tables.js'
import {
  deleteMetaValue,
  ensureMetaTable,
  getMetaValue,
  setForeignKeysEnabled,
  setMetaValue,
  tableExists,
} from '../system-catalog/index.js'
import { ReplicationError } from './errors.js'
import { IDENTIFIER_RE, validateDdlSafety } from './validators.js'

export const SNAPSHOT_STATE_META_KEY = 'device_sync_snapshot_state'
const SNAPSHOT_STATE_LOADING = 'loading'

function assertTableNames(tables: readonly string[]): void {
  for (const table of tables) {
    if (!IDENTIFIER_RE.test(table)) {
      throw new ReplicationError(`Invalid table name: ${table}`)
    }
  }
}

export async function snapshotLoadPending(conn: SQLiteConnection): Promise<boolean> {
  if (!(await tableExists(conn, META_TABLE))) return false
  return (await getMetaValue(conn, SNAPSHOT_STATE_META_KEY)) !== null
}

export async function beginSnapshotLoad(
  conn: SQLiteConnection,
  tables: readonly string[],
  tracker: ChangeTracker | null,
): Promise<void> {
  assertTableNames(tables)
  await ensureMetaTable(conn)
  await setMetaValue(conn, SNAPSHOT_STATE_META_KEY, SNAPSHOT_STATE_LOADING)
  await setForeignKeysEnabled(conn, false)
  await conn.transaction(async tx => {
    for (const table of tables) {
      await tracker?.unwatch(tx, table)
    }
    const reversed = [...tables].reverse()
    for (const table of reversed) {
      if (await tableExists(tx, table)) {
        await tx.exec(`DELETE FROM "${table}"`)
      }
    }
  })
}

export async function applySnapshotSchema(conn: SQLiteConnection, schema: readonly string[]): Promise<void> {
  for (const ddl of schema) {
    if (!validateDdlSafety(ddl)) {
      throw new ReplicationError(`Snapshot schema statement failed safety validation: ${ddl.slice(0, 120)}`)
    }
  }
  for (const ddl of schema) {
    const rewritten = ddl
      .replace(/^\s*CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)/i, 'CREATE TABLE IF NOT EXISTS ')
      .replace(/^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(?!IF\s+NOT\s+EXISTS)/i, 'CREATE $1INDEX IF NOT EXISTS ')
    await conn.exec(rewritten)
  }
}

export async function loadSnapshotPage(
  conn: SQLiteConnection,
  table: string,
  rows: readonly Record<string, unknown>[],
): Promise<void> {
  if (!IDENTIFIER_RE.test(table)) {
    throw new ReplicationError(`Invalid table name: ${table}`)
  }
  if (rows.length === 0) return

  const columns = Object.keys(rows[0])
  if (columns.length === 0) {
    throw new ReplicationError(`Snapshot page for table '${table}' carries rows without columns`)
  }
  for (const column of columns) {
    if (!IDENTIFIER_RE.test(column) && column !== 'rowid') {
      throw new ReplicationError(`Invalid column name in snapshot page for table '${table}': ${column}`)
    }
  }
  const columnSet = new Set(columns)
  for (const row of rows) {
    const keys = Object.keys(row)
    if (keys.length !== columns.length || keys.some(key => !columnSet.has(key))) {
      throw new ReplicationError(`Snapshot page rows for table '${table}' do not share one column set`)
    }
  }

  const columnList = columns.map(column => `"${column}"`).join(', ')
  const placeholders = columns.map(() => '?').join(', ')
  await conn.transaction(async tx => {
    const insert = await tx.prepare(`INSERT INTO "${table}" (${columnList}) VALUES (${placeholders})`)
    for (const row of rows) {
      await insert.run(...columns.map(column => row[column]))
    }
  })
}

export async function endSnapshotLoad(
  conn: SQLiteConnection,
  tables: readonly string[],
  tracker: ChangeTracker | null,
): Promise<void> {
  assertTableNames(tables)
  for (const table of tables) {
    await tracker?.watch(conn, table)
  }
  await setForeignKeysEnabled(conn, true)
  await deleteMetaValue(conn, SNAPSHOT_STATE_META_KEY)
}

export async function abortSnapshotLoad(conn: SQLiteConnection): Promise<void> {
  await setForeignKeysEnabled(conn, true)
}
