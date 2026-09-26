import { afterEach, describe, expect, it } from 'vitest'
import {
  type DeviceRetentionPolicy,
  dropExpiredDeviceCursors,
  resolveMinDeviceCursorBoundary,
} from '../../cdc/device-retention.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../../internal-tables.js'
import { ensureChangesTable, ensureDeviceCursorsTable, upsertDeviceCursor } from '../../system-catalog/index.js'
import { testDriver } from '../helpers/test-driver.js'

const DEVICE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const DEVICE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'
const THIRTY_DAYS_MS = 30 * 24 * 3_600_000

const unlimited: DeviceRetentionPolicy = {
  cursorRetentionMs: THIRTY_DAYS_MS,
  maxChangesHeldForDevice: 0,
}

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

async function open(): Promise<SQLiteConnection> {
  conn = await testDriver.open(':memory:')
  await ensureChangesTable(conn, CHANGES_TABLE)
  await ensureDeviceCursorsTable(conn)
  return conn
}

async function insertChange(c: SQLiteConnection, nodeId: string): Promise<void> {
  const stmt = await c.prepare(
    `INSERT INTO ${CHANGES_TABLE} (table_name, operation, row_id, new_data, node_id, tx_id, hlc) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
  await stmt.run('notes', 'INSERT', '1', '{}', nodeId, 'tx', 'hlc')
}

async function ackAt(c: SQLiteConnection, deviceId: string, seq: bigint, updatedAt = Date.now() / 1000) {
  await upsertDeviceCursor(c, deviceId, seq, updatedAt)
}

async function backdateChanges(c: SQLiteConnection, seconds: number): Promise<void> {
  const stmt = await c.prepare(`UPDATE ${CHANGES_TABLE} SET changed_at = changed_at - ?`)
  await stmt.run(seconds)
}

async function boundaryAfterPruning(c: SQLiteConnection, policy: DeviceRetentionPolicy): Promise<bigint | null> {
  await dropExpiredDeviceCursors(c, policy)
  return resolveMinDeviceCursorBoundary(c)
}

async function countCursors(c: SQLiteConnection): Promise<number> {
  const stmt = await c.prepare(`SELECT COUNT(*) AS total FROM ${DEVICE_CURSORS_TABLE}`)
  const row = (await stmt.get()) as { total: number }
  return Number(row.total)
}

describe('device cursor boundary', () => {
  it('holds the boundary at the first change a device has not acknowledged', async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_A)
    await ackAt(c, DEVICE_A, 0n)

    expect(await boundaryAfterPruning(c, unlimited)).toBe(1n)
  })

  it("moves the boundary past a device's own writes", async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await ackAt(c, DEVICE_A, 0n)

    expect(await boundaryAfterPruning(c, unlimited)).toBe(3n)
  })

  it('takes the lowest boundary across devices', async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await insertChange(c, DEVICE_A)
    await ackAt(c, DEVICE_B, 1n)
    await ackAt(c, DEVICE_A, 2n)

    expect(await boundaryAfterPruning(c, unlimited)).toBe(1n)
  })

  it('returns no boundary when no device holds a cursor', async () => {
    const c = await open()
    await insertChange(c, DEVICE_A)

    expect(await boundaryAfterPruning(c, unlimited)).toBeNull()
  })

  it('returns no boundary when the cursor table is absent', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE)

    expect(await boundaryAfterPruning(conn, unlimited)).toBeNull()
  })

  it('drops a cursor whose device repeats an acknowledgement after its oldest change ages out', async () => {
    const c = await open()
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_B)
    await backdateChanges(c, THIRTY_DAYS_MS / 1000 + 60)
    await ackAt(c, DEVICE_A, 0n)

    expect(await boundaryAfterPruning(c, unlimited)).toBeNull()
    expect(await countCursors(c)).toBe(0)
  })

  it('keeps a cursor whose oldest unacknowledged change is inside the device window', async () => {
    const c = await open()
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_B)
    await backdateChanges(c, THIRTY_DAYS_MS / 1000 - 3_600)
    await ackAt(c, DEVICE_A, 0n)

    expect(await boundaryAfterPruning(c, unlimited)).toBe(0n)
    expect(await countCursors(c)).toBe(1)
  })

  it('drops a cursor that has fallen further behind than the change limit', async () => {
    const c = await open()
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_B)
    await ackAt(c, DEVICE_A, 0n)

    const capped: DeviceRetentionPolicy = { cursorRetentionMs: THIRTY_DAYS_MS, maxChangesHeldForDevice: 2 }
    expect(await boundaryAfterPruning(c, capped)).toBeNull()
    expect(await countCursors(c)).toBe(0)
  })

  it("counts only the changes a cursor holds back, never the device's own writes", async () => {
    const c = await open()
    for (let i = 0; i < 6; i++) {
      await insertChange(c, DEVICE_A)
    }
    await insertChange(c, DEVICE_B)
    await ackAt(c, DEVICE_A, 0n)

    const capped: DeviceRetentionPolicy = { cursorRetentionMs: THIRTY_DAYS_MS, maxChangesHeldForDevice: 2 }
    expect(await boundaryAfterPruning(c, capped)).toBe(6n)
    expect(await countCursors(c)).toBe(1)
  })

  it('keeps a cursor inside the change limit', async () => {
    const c = await open()
    await insertChange(c, DEVICE_B)
    await insertChange(c, DEVICE_B)
    await ackAt(c, DEVICE_A, 0n)

    const capped: DeviceRetentionPolicy = { cursorRetentionMs: THIRTY_DAYS_MS, maxChangesHeldForDevice: 5 }
    expect(await boundaryAfterPruning(c, capped)).toBe(0n)
    expect(await countCursors(c)).toBe(1)
  })

  it('drops a cursor whose device has been silent beyond the device window', async () => {
    const c = await open()
    await insertChange(c, DEVICE_B)
    await ackAt(c, DEVICE_A, 0n, Date.now() / 1000 - THIRTY_DAYS_MS / 1000 - 60)

    expect(await boundaryAfterPruning(c, unlimited)).toBeNull()
    expect(await countCursors(c)).toBe(0)
  })
})
