import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../../core/internal-tables.js'
import { ensureChangesTable } from '../../core/system-catalog/index.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { upsertDeviceAck } from '../device-cursors.js'

const DEVICE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'

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

describe('device acknowledgements', () => {
  it('moves a cursor forward and never backward', async () => {
    const c = await open()
    await upsertDeviceAck(c, DEVICE_A, 5n)
    await upsertDeviceAck(c, DEVICE_A, 3n)

    const stmt = await c.prepare(`SELECT acked_seq FROM ${DEVICE_CURSORS_TABLE} WHERE device_id = ?`)
    const row = (await stmt.get(DEVICE_A)) as { acked_seq: number }
    expect(Number(row.acked_seq)).toBe(5)
  })

  it('creates the cursor table on the first acknowledgement', async () => {
    const c = await open()
    await upsertDeviceAck(c, DEVICE_A, 1n)

    const stmt = await c.prepare(`SELECT COUNT(*) AS total FROM ${DEVICE_CURSORS_TABLE}`)
    const row = (await stmt.get()) as { total: number }
    expect(Number(row.total)).toBe(1)
  })
})
