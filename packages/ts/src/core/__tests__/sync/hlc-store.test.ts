import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import { META_TABLE } from '../../internal-tables.js'
import { HLC } from '../../sync/hlc.js'
import { HLC_CLOCK_META_KEY, isWellFormedHlc, loadPersistedHlc, persistHlcClock } from '../../sync/hlc-store.js'
import { StampOps } from '../../sync/stamp-ops.js'
import { ensureChangesTable, ensureMetaTable, getMetaValue } from '../../system-catalog/index.js'
import { testDriver } from '../helpers/test-driver.js'

const NODE_ID = 'a1b2c3d4e5f60718293a4b5c6d7e8f90'
const CHANGES_TABLE_NAME = '_sirannon_changes'

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

describe('isWellFormedHlc', () => {
  it('accepts a value produced by the clock', () => {
    const hlc = new HLC(NODE_ID)
    expect(isWellFormedHlc(hlc.now())).toBe(true)
  })

  it('rejects the empty string, malformed values, and negative components', () => {
    expect(isWellFormedHlc('')).toBe(false)
    expect(isWellFormedHlc('not-a-clock')).toBe(false)
    expect(isWellFormedHlc('zzzz-0001')).toBe(false)
  })
})

describe('persistHlcClock and loadPersistedHlc', () => {
  it('round-trips the persisted clock value', async () => {
    conn = await testDriver.open(':memory:')
    const hlc = new HLC(NODE_ID)
    const encoded = hlc.now()

    await persistHlcClock(conn, encoded)
    expect(await loadPersistedHlc(conn)).toBe(encoded)
  })

  it('overwrites an earlier persisted value', async () => {
    conn = await testDriver.open(':memory:')
    const hlc = new HLC(NODE_ID)
    const first = hlc.now()
    const second = hlc.now()

    await persistHlcClock(conn, first)
    await persistHlcClock(conn, second)
    expect(await loadPersistedHlc(conn)).toBe(second)
  })

  it('returns null when nothing is persisted', async () => {
    conn = await testDriver.open(':memory:')
    expect(await loadPersistedHlc(conn)).toBeNull()
  })

  it('returns null when the persisted value is malformed', async () => {
    conn = await testDriver.open(':memory:')
    await ensureMetaTable(conn)
    const stmt = await conn.prepare(`INSERT INTO "${META_TABLE}" (key, value) VALUES (?, ?)`)
    await stmt.run(HLC_CLOCK_META_KEY, 'corrupted')
    expect(await loadPersistedHlc(conn)).toBeNull()
  })
})

describe('StampOps clock persistence', () => {
  it('persists the stamping HLC in the same transaction as the stamp', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE_NAME, { replication: true })
    await ensureMetaTable(conn)
    const insert = await conn.prepare(
      `INSERT INTO "${CHANGES_TABLE_NAME}" (table_name, operation, row_id, old_data, new_data) VALUES (?, ?, ?, ?, ?)`,
    )
    await insert.run('notes', 'INSERT', '1', null, '{"id":1}')

    const hlc = new HLC(NODE_ID)
    const stampOps = new StampOps(NODE_ID, hlc, CHANGES_TABLE_NAME)
    await conn.transaction(async tx => {
      await stampOps.stampChanges(tx, 0n, 'tx-1')
    })

    const stampedStmt = await conn.prepare(`SELECT hlc FROM "${CHANGES_TABLE_NAME}" WHERE row_id = ?`)
    const stamped = (await stampedStmt.get('1')) as { hlc: string }
    expect(isWellFormedHlc(stamped.hlc)).toBe(true)
    expect(await getMetaValue(conn, HLC_CLOCK_META_KEY)).toBe(stamped.hlc)
  })

  it('rolls the persisted clock back together with a failed transaction', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE_NAME, { replication: true })
    await ensureMetaTable(conn)
    const insert = await conn.prepare(
      `INSERT INTO "${CHANGES_TABLE_NAME}" (table_name, operation, row_id, old_data, new_data) VALUES (?, ?, ?, ?, ?)`,
    )
    await insert.run('notes', 'INSERT', '1', null, '{"id":1}')

    const hlc = new HLC(NODE_ID)
    const stampOps = new StampOps(NODE_ID, hlc, CHANGES_TABLE_NAME)
    await expect(
      conn.transaction(async tx => {
        await stampOps.stampChanges(tx, 0n, 'tx-rollback')
        throw new Error('forced rollback')
      }),
    ).rejects.toThrow('forced rollback')

    expect(await getMetaValue(conn, HLC_CLOCK_META_KEY)).toBeNull()
  })
})
