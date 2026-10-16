import type { SQLiteConnection } from '../driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../internal-tables.js'
import { assertSafeIdentifier } from './columns.js'

/**
 * Holds the highest change-log sequence that one device has acknowledged.
 *
 * @internal
 */
export interface DeviceCursorRow {
  deviceId: string
  ackedSeq: bigint
}

/**
 * Sets the limits past which the server deletes a device's cursor: a last acknowledgement before `idleCutoff`, more
 * than `maxChangesHeldForDevice` changes waiting for the device, or a waiting change recorded before
 * `changeAgeCutoff`. Both cutoffs are in seconds since the Unix epoch, and a `maxChangesHeldForDevice` of zero sets
 * no limit on the number of waiting changes.
 *
 * @internal
 */
export interface ExpiredDeviceCursorBounds {
  idleCutoff: number
  changeAgeCutoff: number
  maxChangesHeldForDevice: number
}

/**
 * Creates the table that holds each device's acknowledged sequence, where the table is missing.
 *
 * @param conn - The writer connection.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureDeviceCursorsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(`
CREATE TABLE IF NOT EXISTS "${DEVICE_CURSORS_TABLE}" (
  device_id TEXT PRIMARY KEY,
  acked_seq INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL
)`)
}

/**
 * Records a device's acknowledgement, creating its cursor where none exists, and keeps the higher of the stored and
 * the acknowledged sequence.
 *
 * @param conn - The writer connection.
 * @param deviceId - The device.
 * @param ackedSeq - The sequence that the device acknowledged.
 * @param updatedAt - The time of the acknowledgement, in seconds since the Unix epoch.
 * @returns A promise that settles once SQLite has written the cursor.
 * @internal
 */
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

/**
 * Records a device's acknowledgement on its existing cursor, and keeps the higher of the stored and the acknowledged
 * sequence.
 *
 * @param conn - The writer connection.
 * @param deviceId - The device.
 * @param ackedSeq - The sequence that the device acknowledged.
 * @param updatedAt - The time of the acknowledgement, in seconds since the Unix epoch.
 * @returns 1 when the device has a cursor, and 0 when the server has deleted it.
 * @internal
 */
export async function updateDeviceCursor(
  conn: SQLiteConnection,
  deviceId: string,
  ackedSeq: bigint,
  updatedAt: number,
): Promise<number> {
  const stmt = await conn.prepare(
    `UPDATE "${DEVICE_CURSORS_TABLE}"
     SET acked_seq = max(acked_seq, CAST(? AS INTEGER)), updated_at = ?
     WHERE device_id = ?`,
  )
  const result = await stmt.run(ackedSeq.toString(), updatedAt, deviceId)
  return result.changes
}

/**
 * Returns whether a device has a cursor.
 *
 * @param conn - The connection to read the table through.
 * @param deviceId - The device.
 * @returns `true` when the table holds a cursor for the device.
 * @internal
 */
export async function selectDeviceCursorExists(conn: SQLiteConnection, deviceId: string): Promise<boolean> {
  const stmt = await conn.prepare(`SELECT 1 AS present FROM "${DEVICE_CURSORS_TABLE}" WHERE device_id = ?`)
  return (await stmt.get(deviceId)) !== undefined
}

/**
 * Deletes the cursor of every device whose last acknowledgement came before a time.
 *
 * @param conn - The writer connection.
 * @param cutoff - The time, in seconds since the Unix epoch.
 * @returns The number of cursors deleted.
 * @internal
 */
export async function deleteDeviceCursorsUpdatedBefore(conn: SQLiteConnection, cutoff: number): Promise<number> {
  const stmt = await conn.prepare(`DELETE FROM "${DEVICE_CURSORS_TABLE}" WHERE updated_at < ?`)
  const result = await stmt.run(cutoff)
  return result.changes
}

/**
 * Deletes the cursor of every device that has passed one of the limits in `bounds`. A change waits for a device when
 * its sequence is above the device's acknowledged sequence and another node or device made it.
 *
 * @param conn - The writer connection.
 * @param changesTable - The change log table, which has to be a safe identifier.
 * @param bounds - The limits.
 * @returns The number of cursors deleted.
 * @internal
 */
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

/**
 * Returns the highest sequence up to which the change log can drop changes without losing one that a device still
 * waits for: for each device, the sequence just below the first change that waits for it, or the log's highest
 * sequence when none waits, and the lowest of those across every device.
 *
 * @param conn - The connection to read the tables through.
 * @param changesTable - The change log table, which has to be a safe identifier.
 * @returns That sequence, or `null` when no device has a cursor.
 * @internal
 */
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

/**
 * Returns every device cursor.
 *
 * @param conn - The connection to read the table through.
 * @returns One row per device, in no set order.
 * @internal
 */
export async function selectDeviceCursors(conn: SQLiteConnection): Promise<DeviceCursorRow[]> {
  const stmt = await conn.prepare(`SELECT device_id, acked_seq FROM "${DEVICE_CURSORS_TABLE}"`)
  const rows = (await stmt.all()) as Array<{ device_id: string; acked_seq: number | bigint | string }>
  return rows.map(row => ({ deviceId: row.device_id, ackedSeq: BigInt(row.acked_seq) }))
}
