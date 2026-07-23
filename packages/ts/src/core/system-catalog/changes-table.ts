import type { SQLiteConnection } from '../driver/types.js'
import { assertSafeIdentifier, ensureColumn } from './columns.js'
import { ensureMetaTable } from './meta-table.js'

export async function ensureChangesTable(conn: SQLiteConnection, tableName: string): Promise<void> {
  assertSafeIdentifier(tableName)

  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${tableName}" (
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

  await ensureColumn(conn, tableName, 'node_id', 'TEXT', '')
  await ensureColumn(conn, tableName, 'tx_id', 'TEXT', '')
  await ensureColumn(conn, tableName, 'hlc', 'TEXT', '')

  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_changed_at" ON "${tableName}" (changed_at)`)
  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_node_id" ON "${tableName}" (node_id)`)
  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_hlc" ON "${tableName}" (hlc)`)

  await ensureMetaTable(conn)
}
