import type { SQLiteConnection } from '../driver/types.js'
import { META_TABLE } from '../internal-tables.js'

export async function ensureMetaTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`CREATE TABLE IF NOT EXISTS "${META_TABLE}" (key TEXT PRIMARY KEY, value TEXT NOT NULL)`)
}
