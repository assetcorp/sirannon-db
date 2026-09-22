import type { SQLiteConnection } from '../core/driver/types.js'
import { ensureDeviceCursorsTable, upsertDeviceCursor } from '../core/system-catalog/index.js'

export async function upsertDeviceAck(conn: SQLiteConnection, deviceId: string, seq: bigint): Promise<void> {
  await ensureDeviceCursorsTable(conn)
  await upsertDeviceCursor(conn, deviceId, seq, Date.now() / 1000)
}
