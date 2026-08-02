import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import { MIGRATIONS_TABLE } from '../../internal-tables.js'
import { tableColumns } from '../../system-catalog/columns.js'
import { ensureMigrationsTable } from '../../system-catalog/index.js'
import { testDriver } from '../helpers/test-driver.js'

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

describe('ensureMigrationsTable', () => {
  it('creates the tracking table with a checksum column on a fresh database', async () => {
    conn = await testDriver.open(':memory:')
    await ensureMigrationsTable(conn)
    expect(await tableColumns(conn, MIGRATIONS_TABLE)).toEqual(new Set(['version', 'name', 'applied_at', 'checksum']))
  })

  it('is idempotent', async () => {
    conn = await testDriver.open(':memory:')
    await ensureMigrationsTable(conn)
    await ensureMigrationsTable(conn)
    expect((await tableColumns(conn, MIGRATIONS_TABLE)).has('checksum')).toBe(true)
  })

  it('upgrades a pre-checksum tracking table, adding the column and preserving rows', async () => {
    conn = await testDriver.open(':memory:')
    await conn.exec(
      `CREATE TABLE ${MIGRATIONS_TABLE} (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at REAL NOT NULL DEFAULT (unixepoch('subsec')))`,
    )
    const insert = await conn.prepare(`INSERT INTO ${MIGRATIONS_TABLE} (version, name) VALUES (?, ?)`)
    await insert.run(1, 'create_users')
    await insert.run(2, 'add_posts')

    await ensureMigrationsTable(conn)

    expect((await tableColumns(conn, MIGRATIONS_TABLE)).has('checksum')).toBe(true)

    const select = await conn.prepare(`SELECT version, name, checksum FROM ${MIGRATIONS_TABLE} ORDER BY version`)
    expect(await select.all()).toEqual([
      { version: 1, name: 'create_users', checksum: null },
      { version: 2, name: 'add_posts', checksum: null },
    ])
  })
})
