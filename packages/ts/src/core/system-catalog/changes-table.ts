import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE } from '../internal-tables.js'
import { assertSafeIdentifier, ensureColumn } from './columns.js'
import { ensureMetaTable } from './meta-table.js'

export async function maxChangeSeq(conn: SQLiteConnection, tableName: string = CHANGES_TABLE): Promise<bigint> {
  const stmt = await conn.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${tableName}"`)
  const row = (await stmt.get()) as { seq?: unknown } | undefined
  const seq = row?.seq
  if (seq === undefined || seq === null) return 0n
  return typeof seq === 'bigint' ? seq : BigInt(String(seq))
}

export async function maxChangeHlc(conn: SQLiteConnection, tableName: string = CHANGES_TABLE): Promise<string | null> {
  const stmt = await conn.prepare(`SELECT MAX(hlc) AS hlc FROM "${tableName}" WHERE hlc != ''`)
  const row = (await stmt.get()) as { hlc?: unknown } | undefined
  return typeof row?.hlc === 'string' && row.hlc.length > 0 ? row.hlc : null
}

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
