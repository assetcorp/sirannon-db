import type { SQLiteConnection } from '../core/driver/types.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../core/internal-tables.js'
import {
  deleteDeviceCursorsUpdatedBefore,
  deviceCursorRows,
  ensureDeviceCursorsTable,
  maxChangeSeq,
  nextForeignChangeSeqSql,
  tableExists,
  upsertDeviceCursor,
} from '../core/system-catalog/index.js'

export const DEFAULT_DEVICE_CURSOR_RETENTION_MS = 30 * 24 * 3_600_000

export async function upsertDeviceAck(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<void> {
  await ensureDeviceCursorsTable(conn)
  await upsertDeviceCursor(conn, deviceId, seq, Date.now() / 1000)
}

export function evictStaleDeviceCursors(conn: SQLiteConnection, retentionMs: number): Promise<number> {
  return deleteDeviceCursorsUpdatedBefore(conn, Date.now() / 1000 - retentionMs / 1000)
}

export async function effectiveMinDeviceCursor(
  conn: SQLiteConnection,
  retentionMs: number,
  changesTable: string = CHANGES_TABLE,
): Promise<bigint | null> {
  if (!(await tableExists(conn, DEVICE_CURSORS_TABLE))) return null

  await evictStaleDeviceCursors(conn, retentionMs)

  const cursors = await deviceCursorRows(conn)
  if (cursors.length === 0) return null

  const nextForeignStmt = await conn.prepare(nextForeignChangeSeqSql(changesTable))
  const maxSeq = await maxChangeSeq(conn, changesTable)

  let min: bigint | null = null
  for (const cursor of cursors) {
    const acked = cursor.ackedSeq
    const foreignRow = (await nextForeignStmt.get(acked.toString(), cursor.deviceId)) as
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
