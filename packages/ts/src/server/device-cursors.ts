import type { SQLiteConnection } from '../core/driver/types.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../core/internal-tables.js'
import { ensureDeviceCursorsTable, maxChangeSeq, tableExists } from '../core/system-catalog/index.js'

export const DEFAULT_DEVICE_CURSOR_RETENTION_MS = 30 * 24 * 3_600_000

export async function upsertDeviceAck(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<void> {
  await ensureDeviceCursorsTable(conn)
  const stmt = await conn.prepare(
    `INSERT INTO "${DEVICE_CURSORS_TABLE}" (device_id, acked_seq, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(device_id) DO UPDATE SET
       acked_seq = max("${DEVICE_CURSORS_TABLE}".acked_seq, excluded.acked_seq),
       updated_at = excluded.updated_at`,
  )
  await stmt.run(deviceId, seq.toString(), Date.now() / 1000)
}

export async function evictStaleDeviceCursors(conn: SQLiteConnection, retentionMs: number): Promise<number> {
  const cutoff = Date.now() / 1000 - retentionMs / 1000
  const stmt = await conn.prepare(`DELETE FROM "${DEVICE_CURSORS_TABLE}" WHERE updated_at < ?`)
  const result = await stmt.run(cutoff)
  return result.changes
}

export async function effectiveMinDeviceCursor(
  conn: SQLiteConnection,
  retentionMs: number,
  changesTable: string = CHANGES_TABLE,
): Promise<bigint | null> {
  if (!(await tableExists(conn, DEVICE_CURSORS_TABLE))) return null

  await evictStaleDeviceCursors(conn, retentionMs)

  const cursorsStmt = await conn.prepare(`SELECT device_id, acked_seq FROM "${DEVICE_CURSORS_TABLE}"`)
  const cursors = (await cursorsStmt.all()) as { device_id: string; acked_seq: number | bigint }[]
  if (cursors.length === 0) return null

  const nextForeignStmt = await conn.prepare(
    `SELECT MIN(seq) AS seq FROM "${changesTable}" WHERE seq > ? AND node_id != ?`,
  )
  const maxSeq = await maxChangeSeq(conn, changesTable)

  let min: bigint | null = null
  for (const cursor of cursors) {
    const acked = BigInt(cursor.acked_seq)
    const foreignRow = (await nextForeignStmt.get(acked.toString(), cursor.device_id)) as
      | { seq: number | bigint | null }
      | undefined
    const nextForeign = foreignRow?.seq === null || foreignRow?.seq === undefined ? null : BigInt(foreignRow.seq)
    const effective = nextForeign === null ? maxSeq : nextForeign - 1n
    if (min === null || effective < min) {
      min = effective
    }
  }
  return min
}
