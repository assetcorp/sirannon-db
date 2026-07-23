import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import { SirannonError } from '../../errors.js'
import { CHANGES_TABLE, META_TABLE } from '../../internal-tables.js'
import { tableColumns } from '../../system-catalog/columns.js'
import { ensureChangesTable } from '../../system-catalog/index.js'
import { testDriver } from '../helpers/test-driver.js'

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

const FULL_COLUMNS = new Set([
  'seq',
  'table_name',
  'operation',
  'row_id',
  'changed_at',
  'old_data',
  'new_data',
  'node_id',
  'tx_id',
  'hlc',
])

async function indexNames(c: SQLiteConnection, table: string): Promise<Set<string>> {
  const stmt = await c.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE 'idx_%'",
  )
  const rows = (await stmt.all(table)) as { name: string }[]
  return new Set(rows.map(r => r.name))
}

async function createNarrowChangesTable(c: SQLiteConnection): Promise<void> {
  await c.exec(`
CREATE TABLE "${CHANGES_TABLE}" (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL,
  row_id TEXT NOT NULL,
  changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data TEXT,
  new_data TEXT
)`)
}

describe('ensureChangesTable', () => {
  it('creates the full shape with all three indexes and the meta table', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE)

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(FULL_COLUMNS)
    expect(await indexNames(conn, CHANGES_TABLE)).toEqual(
      new Set([`idx_${CHANGES_TABLE}_changed_at`, `idx_${CHANGES_TABLE}_node_id`, `idx_${CHANGES_TABLE}_hlc`]),
    )
    const metaCheck = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
    expect(await metaCheck.get(META_TABLE)).toBeTruthy()
  })

  it('upgrades a pre-existing narrow table, preserving rows', async () => {
    conn = await testDriver.open(':memory:')
    await createNarrowChangesTable(conn)

    const insert = await conn.prepare(
      `INSERT INTO ${CHANGES_TABLE} (table_name, operation, row_id, new_data) VALUES (?, ?, ?, ?)`,
    )
    await insert.run('users', 'INSERT', '1', '{"id":1}')

    await ensureChangesTable(conn, CHANGES_TABLE)

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(FULL_COLUMNS)
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

  it('is idempotent', async () => {
    conn = await testDriver.open(':memory:')
    await ensureChangesTable(conn, CHANGES_TABLE)
    await ensureChangesTable(conn, CHANGES_TABLE)

    expect(await tableColumns(conn, CHANGES_TABLE)).toEqual(FULL_COLUMNS)
  })

  it('rejects unsafe table names', async () => {
    conn = await testDriver.open(':memory:')
    await expect(ensureChangesTable(conn, 'bad name; --')).rejects.toThrow(SirannonError)
  })
})
