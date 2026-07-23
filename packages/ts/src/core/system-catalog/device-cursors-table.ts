import type { SQLiteConnection } from '../driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../internal-tables.js'

export async function ensureDeviceCursorsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${DEVICE_CURSORS_TABLE}" (
  device_id TEXT PRIMARY KEY,
  acked_seq INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL
)`)
}
