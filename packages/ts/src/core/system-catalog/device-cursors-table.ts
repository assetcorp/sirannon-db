import type { SQLiteConnection } from '../driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../internal-tables.js'

export interface DeviceCursorRow {
  deviceId: string
  ackedSeq: bigint
}

export async function ensureDeviceCursorsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${DEVICE_CURSORS_TABLE}" (
  device_id TEXT PRIMARY KEY,
  acked_seq INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL
)`)
}

export async function upsertDeviceCursor(
  conn: SQLiteConnection,
  deviceId: string,
  ackedSeq: bigint,
  updatedAt: number,
): Promise<void> {
  const stmt = await conn.prepare(
    `INSERT INTO "${DEVICE_CURSORS_TABLE}" (device_id, acked_seq, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(device_id) DO UPDATE SET
       acked_seq = max("${DEVICE_CURSORS_TABLE}".acked_seq, excluded.acked_seq),
       updated_at = excluded.updated_at`,
  )
  await stmt.run(deviceId, ackedSeq.toString(), updatedAt)
}

export async function deleteDeviceCursorsUpdatedBefore(conn: SQLiteConnection, cutoff: number): Promise<number> {
  const stmt = await conn.prepare(`DELETE FROM "${DEVICE_CURSORS_TABLE}" WHERE updated_at < ?`)
  const result = await stmt.run(cutoff)
  return result.changes
}

export async function selectDeviceCursors(conn: SQLiteConnection): Promise<DeviceCursorRow[]> {
  const stmt = await conn.prepare(`SELECT device_id, acked_seq FROM "${DEVICE_CURSORS_TABLE}"`)
  const rows = (await stmt.all()) as Array<{ device_id: string; acked_seq: number | bigint | string }>
  return rows.map(row => ({ deviceId: row.device_id, ackedSeq: BigInt(row.acked_seq) }))
}
