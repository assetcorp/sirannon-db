import type { SQLiteConnection } from '../driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../internal-tables.js'
import { assertSafeIdentifier } from './columns.js'

export interface DeviceCursorRow {
  deviceId: string
  ackedSeq: bigint
}

export interface ExpiredDeviceCursorBounds {
  idleCutoff: number
  changeAgeCutoff: number
  maxChangesHeldForDevice: number
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

export async function deleteExpiredDeviceCursors(
  conn: SQLiteConnection,
  changesTable: string,
  bounds: ExpiredDeviceCursorBounds,
): Promise<number> {
  assertSafeIdentifier(changesTable)

  const stmt = await conn.prepare(
    `DELETE FROM "${DEVICE_CURSORS_TABLE}"
     WHERE updated_at < ?
        OR (? > 0 AND (
              SELECT COUNT(*) FROM "${changesTable}" AS held
              WHERE held.seq > "${DEVICE_CURSORS_TABLE}".acked_seq
                AND held.node_id != "${DEVICE_CURSORS_TABLE}".device_id
            ) > ?)
        OR EXISTS (
             SELECT 1 FROM "${changesTable}" AS oldest
             WHERE oldest.seq = (
                     SELECT MIN(later.seq) FROM "${changesTable}" AS later
                     WHERE later.seq > "${DEVICE_CURSORS_TABLE}".acked_seq
                       AND later.node_id != "${DEVICE_CURSORS_TABLE}".device_id
                   )
               AND oldest.changed_at < ?
           )`,
  )
  const result = await stmt.run(
    bounds.idleCutoff,
    bounds.maxChangesHeldForDevice,
    bounds.maxChangesHeldForDevice,
    bounds.changeAgeCutoff,
  )
  return result.changes
}

export async function selectMinDeviceCursorBoundary(
  conn: SQLiteConnection,
  changesTable: string,
): Promise<bigint | null> {
  assertSafeIdentifier(changesTable)

  const stmt = await conn.prepare(
    `SELECT MIN(
              COALESCE(
                (SELECT MIN(later.seq) FROM "${changesTable}" AS later
                 WHERE later.seq > cursors.acked_seq AND later.node_id != cursors.device_id) - 1,
                (SELECT COALESCE(MAX(seq), 0) FROM "${changesTable}")
              )
            ) AS boundary
     FROM "${DEVICE_CURSORS_TABLE}" AS cursors`,
  )
  const row = (await stmt.get()) as { boundary?: unknown } | undefined
  const boundary = row?.boundary
  if (boundary === undefined || boundary === null) return null
  return typeof boundary === 'bigint' ? boundary : BigInt(String(boundary))
}

export async function selectDeviceCursors(conn: SQLiteConnection): Promise<DeviceCursorRow[]> {
  const stmt = await conn.prepare(`SELECT device_id, acked_seq FROM "${DEVICE_CURSORS_TABLE}"`)
  const rows = (await stmt.all()) as Array<{ device_id: string; acked_seq: number | bigint | string }>
  return rows.map(row => ({ deviceId: row.device_id, ackedSeq: BigInt(row.acked_seq) }))
}
