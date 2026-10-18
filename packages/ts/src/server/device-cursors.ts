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

/**
 * The close reason that the server sends with close code 4410 once it has deleted the cursor of a subscribed device.
 *
 * @internal
 */
export const DEVICE_CURSOR_DROPPED_REASON = 'The server deleted this device cursor; resubscribe to resync'

/**
 * Stores the acknowledged sequence of a device that holds no cursor yet, and creates the cursor table first when the
 * database has none.
 *
 * @param conn - The writer connection of the database.
 * @param deviceId - The identifier of the device that acknowledged.
 * @param seq - The highest sequence that the device has acknowledged.
 * @internal
 */
export async function upsertDeviceAck(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<void> {
  await ensureDeviceCursorsTable(conn)
  await upsertDeviceCursor(conn, deviceId, seq, Date.now() / 1000)
}

/**
 * Moves the stored cursor of a device to the sequence that the device acknowledged.
 *
 * The function returns `false` when the server has deleted that cursor since the device subscribed, and the caller
 * then closes the connection with close code 4410 so that the device resynchronises.
 *
 * @param conn - The writer connection of the database.
 * @param deviceId - The identifier of the device that acknowledged.
 * @param seq - The highest sequence that the device has acknowledged.
 * @returns `true` when the function moved the stored cursor, and `false` when the database holds no cursor for the device.
 * @internal
 */
export async function advanceHeldDeviceCursor(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<boolean> {
  if (!(await selectTableExists(conn, DEVICE_CURSORS_TABLE))) return false
  return (await updateDeviceCursor(conn, deviceId, seq, Date.now() / 1000)) > 0
}

/**
 * Returns whether the database stores a cursor for a device.
 *
 * @param conn - A connection to the database.
 * @param deviceId - The identifier of the device.
 * @returns `true` when the cursor table exists and holds a row for the device.
 * @internal
 */
export async function deviceCursorExists(conn: SQLiteConnection, deviceId: string): Promise<boolean> {
  if (!(await selectTableExists(conn, DEVICE_CURSORS_TABLE))) return false
  return selectDeviceCursorExists(conn, deviceId)
}
