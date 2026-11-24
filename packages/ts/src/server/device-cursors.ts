import type { SQLiteConnection } from '../core/driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../core/internal-tables.js'
import { selectDeviceCursorExists, selectTableExists } from '../core/system-catalog/index.js'

export const WS_CLOSE_DEVICE_CURSOR_DROPPED = 4410

/**
 * The close reason that the server sends with close code 4410 once it has deleted the cursor of a subscribed device.
 *
 * @internal
 */
export const DEVICE_CURSOR_DROPPED_REASON = 'The server deleted this device cursor; resubscribe to resync'

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
