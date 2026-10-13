import type { SQLiteConnection } from '../core/driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../core/internal-tables.js'
import {
  ensureDeviceCursorsTable,
  selectDeviceCursorExists,
  selectTableExists,
  updateDeviceCursor,
  upsertDeviceCursor,
} from '../core/system-catalog/index.js'

export const WS_CLOSE_DEVICE_CURSOR_DROPPED = 4410

export async function upsertDeviceAck(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<void> {
  await ensureDeviceCursorsTable(conn)
  await upsertDeviceCursor(conn, deviceId, seq, Date.now() / 1000)
}

export async function advanceHeldDeviceCursor(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<boolean> {
  if (!(await selectTableExists(conn, DEVICE_CURSORS_TABLE))) return false
  return (await updateDeviceCursor(conn, deviceId, seq, Date.now() / 1000)) > 0
}

export async function deviceCursorExists(conn: SQLiteConnection, deviceId: string): Promise<boolean> {
  if (!(await selectTableExists(conn, DEVICE_CURSORS_TABLE))) return false
  return selectDeviceCursorExists(conn, deviceId)
}
