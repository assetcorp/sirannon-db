import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import { SirannonError } from '../../errors.js'
import { CHANGES_TABLE } from '../../internal-tables.js'
import { tableColumns } from '../../system-catalog/columns.js'
import { ensureChangesTable } from '../../system-catalog/index.js'
import { testDriver } from '../helpers/test-driver.js'

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

const NARROW_COLUMNS = new Set(['seq', 'table_name', 'operation', 'row_id', 'changed_at', 'old_data', 'new_data'])
const REPLICATION_COLUMNS = new Set([...NARROW_COLUMNS, 'node_id', 'tx_id', 'hlc'])

async function indexNames(c: SQLiteConnection, table: string): Promise<Set<string>> {
  const stmt = await c.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE 'idx_%'",
  )
  const rows = (await stmt.all(table)) as { name: string }[]
  return new Set(rows.map(r => r.name))
}

describe('ensureChangesTable', () => {
  it('creates the narrow shape with the changed_at index', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: false })

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(NARROW_COLUMNS)
    expect(await indexNames(conn, CHANGES_TABLE)).toEqual(new Set([`idx_${CHANGES_TABLE}_changed_at`]))
  })

  it('creates the replication shape with all three indexes', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: true })

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(REPLICATION_COLUMNS)
    expect(await indexNames(conn, CHANGES_TABLE)).toEqual(
      new Set([`idx_${CHANGES_TABLE}_changed_at`, `idx_${CHANGES_TABLE}_node_id`, `idx_${CHANGES_TABLE}_hlc`]),
    )
  })

  it('upgrades a narrow table to the replication shape, preserving rows', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: false })

    const insert = await conn.prepare(
      `INSERT INTO ${CHANGES_TABLE} (table_name, operation, row_id, new_data) VALUES (?, ?, ?, ?)`,
    )
    await insert.run('users', 'INSERT', '1', '{"id":1}')

    await ensureChangesTable(conn, CHANGES_TABLE, { replication: true })

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(REPLICATION_COLUMNS)
    expect(await indexNames(conn, CHANGES_TABLE)).toEqual(
      new Set([`idx_${CHANGES_TABLE}_changed_at`, `idx_${CHANGES_TABLE}_node_id`, `idx_${CHANGES_TABLE}_hlc`]),
    )

    const select = await conn.prepare(
      `SELECT table_name, operation, row_id, new_data, node_id, tx_id, hlc FROM ${CHANGES_TABLE}`,
    )
    expect(await select.all()).toEqual([
      { table_name: 'users', operation: 'INSERT', row_id: '1', new_data: '{"id":1}', node_id: '', tx_id: '', hlc: '' },
    ])
  })

  it('keeps the replication shape when the narrow ensure runs afterwards', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: true })
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: false })

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(REPLICATION_COLUMNS)
  })

  it('is idempotent in both shapes', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: false })
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: false })
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: true })
    await ensureChangesTable(conn, CHANGES_TABLE, { replication: true })

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(REPLICATION_COLUMNS)
  })

  it('rejects unsafe table names', async () => {
    conn = await testDriver.open(':memory:')
    await expect(ensureChangesTable(conn, 'bad name; --', { replication: false })).rejects.toThrow(SirannonError)
  })
})
