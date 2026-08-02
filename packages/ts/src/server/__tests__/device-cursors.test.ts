import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../../core/internal-tables.js'
import { ensureChangesTable } from '../../core/system-catalog/index.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { effectiveMinDeviceCursor, evictStaleDeviceCursors, upsertDeviceAck } from '../device-cursors.js'

const DEVICE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const DEVICE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'
const RETENTION_MS = 30 * 24 * 3_600_000

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

async function open(): Promise<SQLiteConnection> {
  conn = await betterSqlite3().open(':memory:')
  await ensureChangesTable(conn, CHANGES_TABLE)
  return conn
}

async function insertChange(c: SQLiteConnection, nodeId: string): Promise<void> {
  const stmt = await c.prepare(
    `INSERT INTO ${CHANGES_TABLE} (table_name, operation, row_id, new_data, node_id, tx_id, hlc) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
  await stmt.run('notes', 'INSERT', '1', '{}', nodeId, 'tx', 'hlc')
}

describe('device cursors', () => {
  it('acks move forward and never backward', async () => {
    const c = await open()
    await upsertDeviceAck(c, DEVICE_A, 5n)
    await upsertDeviceAck(c, DEVICE_A, 3n)

    const stmt = await c.prepare(`SELECT acked_seq FROM ${DEVICE_CURSORS_TABLE} WHERE device_id = ?`)
    const row = (await stmt.get(DEVICE_A)) as { acked_seq: number }
    expect(Number(row.acked_seq)).toBe(5)
  })

  it('evicts cursors idle beyond the retention cap', async () => {
    const c = await open()
    await upsertDeviceAck(c, DEVICE_A, 5n)
    const backdate = await c.prepare(
      `UPDATE ${DEVICE_CURSORS_TABLE} SET updated_at = updated_at - ? WHERE device_id = ?`,
    )
    await backdate.run(RETENTION_MS / 1000 + 60, DEVICE_A)

    expect(await evictStaleDeviceCursors(c, RETENTION_MS)).toBe(1)
    expect(await effectiveMinDeviceCursor(c, RETENTION_MS)).toBeNull()
  })

  it('returns null when no device holds a cursor', async () => {
    const c = await open()
    expect(await effectiveMinDeviceCursor(c, RETENTION_MS)).toBeNull()
  })

  it('takes the minimum across devices', async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await upsertDeviceAck(c, DEVICE_B, 1n)
    await upsertDeviceAck(c, DEVICE_A, 2n)

    expect(await effectiveMinDeviceCursor(c, RETENTION_MS)).toBe(1n)
  })

  it("advances a cursor past the device's own trailing writes", async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await upsertDeviceAck(c, DEVICE_A, 0n)

    expect(await effectiveMinDeviceCursor(c, RETENTION_MS)).toBe(3n)
  })

  it('holds a cursor at the first unseen foreign write', async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_A)
    await upsertDeviceAck(c, DEVICE_A, 0n)

    expect(await effectiveMinDeviceCursor(c, RETENTION_MS)).toBe(1n)
  })
})
