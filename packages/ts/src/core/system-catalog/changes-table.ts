import type { SQLiteConnection } from '../driver/types.js'
import { assertSafeIdentifier, ensureColumn } from './columns.js'

export interface ChangesTableOptions {
  replication: boolean
}

export async function ensureChangesTable(
  conn: SQLiteConnection,
  tableName: string,
  options: ChangesTableOptions,
): Promise<void> {
  assertSafeIdentifier(tableName)

  const replicationColumns = options.replication
    ? `,
  node_id TEXT NOT NULL DEFAULT '',
  tx_id TEXT NOT NULL DEFAULT '',
  hlc TEXT NOT NULL DEFAULT ''`
    : ''

  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${tableName}" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT${replicationColumns}
)`)

  if (options.replication) {
    await ensureColumn(conn, tableName, 'node_id', 'TEXT', '')
    await ensureColumn(conn, tableName, 'tx_id', 'TEXT', '')
    await ensureColumn(conn, tableName, 'hlc', 'TEXT', '')
  }

  await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_changed_at" ON "${tableName}" (changed_at)`)

  if (options.replication) {
    await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_node_id" ON "${tableName}" (node_id)`)
    await conn.exec(`CREATE INDEX IF NOT EXISTS "idx_${tableName}_hlc" ON "${tableName}" (hlc)`)
  }
}
