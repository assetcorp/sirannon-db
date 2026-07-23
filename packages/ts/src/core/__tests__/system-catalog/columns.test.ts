import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import { SirannonError } from '../../errors.js'
import { MIGRATIONS_TABLE } from '../../internal-tables.js'
import { ensureColumn, tableColumns } from '../../system-catalog/columns.js'
import { testDriver } from '../helpers/test-driver.js'

const unusedConn = {} as unknown as SQLiteConnection

function raceConn(columnAppearsAfterAlter: boolean): SQLiteConnection {
  let columnPresent = false
  const stub = {
    async prepare(sql: string) {
      return {
        async all() {
          if (sql.includes('table_info')) {
            return columnPresent ? [{ name: 'id' }, { name: 'checksum' }] : [{ name: 'id' }]
          }
          return []
        },
        async get() {
          return undefined
        },
        async run() {
          return { changes: 0, lastInsertRowId: 0 }
        },
      }
    },
    async exec(sql: string) {
      if (sql.startsWith('ALTER TABLE')) {
        if (columnAppearsAfterAlter) {
          columnPresent = true
          throw new Error('duplicate column name: checksum')
        }
        throw new Error('disk I/O error')
      }
    },
    async transaction() {
      throw new Error('unused')
    },
    async close() {},
  }
  return stub as unknown as SQLiteConnection
}

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

describe('tableColumns and ensureColumn identifier safety', () => {
  it('rejects an unsafe table name before touching the connection', async () => {
    await expect(tableColumns(unusedConn, 'users"; DROP TABLE x; --')).rejects.toBeInstanceOf(SirannonError)
    await expect(tableColumns(unusedConn, 'has space')).rejects.toBeInstanceOf(SirannonError)
  })

  it('rejects an unsafe column name or column type', async () => {
    await expect(ensureColumn(unusedConn, MIGRATIONS_TABLE, 'bad-column', 'TEXT')).rejects.toBeInstanceOf(SirannonError)
    await expect(ensureColumn(unusedConn, MIGRATIONS_TABLE, 'checksum', 'TEXT; DROP TABLE x')).rejects.toBeInstanceOf(
      SirannonError,
    )
    await expect(ensureColumn(unusedConn, 'x"; DROP', 'checksum', 'TEXT')).rejects.toBeInstanceOf(SirannonError)
  })
})

describe('ensureColumn on a real database', () => {
  it('reads existing columns and adds a missing one', async () => {
    conn = await testDriver.open(':memory:')
    await conn.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')

    expect(await tableColumns(conn, 't')).toEqual(new Set(['id', 'name']))

    await ensureColumn(conn, 't', 'extra', 'TEXT')
    expect(await tableColumns(conn, 't')).toEqual(new Set(['id', 'name', 'extra']))
  })

  it('is idempotent when the column already exists', async () => {
    conn = await testDriver.open(':memory:')
    await conn.exec('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    await ensureColumn(conn, 't', 'extra', 'TEXT')
    await ensureColumn(conn, 't', 'extra', 'TEXT')

    expect((await tableColumns(conn, 't')).has('extra')).toBe(true)
  })
})

describe('ensureColumn concurrent-add handling', () => {
  it('treats a duplicate-column ALTER failure as success when the column now exists', async () => {
    await expect(ensureColumn(raceConn(true), 't', 'checksum', 'TEXT')).resolves.toBeUndefined()
  })

  it('rethrows a genuine ALTER failure when the column is still missing', async () => {
    await expect(ensureColumn(raceConn(false), 't', 'checksum', 'TEXT')).rejects.toThrow(/disk I\/O error/)
  })
})
