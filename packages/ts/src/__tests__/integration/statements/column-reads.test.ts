import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { QueryWithColumnsResult } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { expoSqlcipherBuild, STOCK_BETTER_SQLITE3_BUILD } from '../../helpers/expo-modules.js'
import { columnRaw, createWorkspace, type Workspace } from './harness.js'

vi.mock('expo-sqlite', async () => (await import('../../helpers/expo-modules.js')).expoSqliteModule())
vi.mock('expo-file-system/legacy', async () =>
  (await import('../../helpers/expo-modules.js')).expoFileSystemLegacyModule(),
)

const EXPO_DRIVER_MODULE = '../../../drivers/expo/index.js'
const { expoSqlite } = (await import(EXPO_DRIVER_MODULE)) as { expoSqlite: () => SQLiteDriver }

const EVERY_BYTE = Uint8Array.from({ length: 256 }, (_, index) => index)
const MULTILINGUAL = 'héllo 😀 𝄞 ﬁn ܐ'
const SELECT_REPEATED =
  'SELECT a.id, b.id, a.label, a.amount, a.payload, a.price, a.amount + 0 AS total, a.label AS "1" FROM items a JOIN items b ON a.id = b.id ORDER BY a.id'
const ENDLESS_BLOBS = 'WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT i, zeroblob(?) FROM n'
const MANY_COLUMNS = 500

interface DriverCase {
  label: string
  driver: () => SQLiteDriver
  details: 'full' | 'names'
  exactIntegers: boolean
}

const DRIVER_CASES: DriverCase[] = [
  { label: 'better-sqlite3', driver: () => betterSqlite3(), details: 'full', exactIntegers: true },
  { label: 'node:sqlite', driver: () => nodeSqlite(), details: 'full', exactIntegers: true },
  { label: 'Expo', driver: () => expoSqlite(), details: 'names', exactIntegers: false },
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('column-reads')
  expoSqlcipherBuild.binding = STOCK_BETTER_SQLITE3_BUILD
  expoSqlcipherBuild.directory = workspace.path('')
})

afterEach(async () => {
  expoSqlcipherBuild.binding = null
  await workspace.dispose()
})

function utf8Bytes(text: string): number {
  return Buffer.byteLength(text, 'utf8')
}

function oracleBytes(row: readonly unknown[]): number {
  let total = 0
  for (const value of row) {
    total += 8
    if (typeof value === 'string') total += utf8Bytes(value)
    else if (value instanceof Uint8Array) total += value.byteLength
  }
  return total
}

function hexOf(value: unknown): unknown {
  return value instanceof Uint8Array ? `x'${Buffer.from(value).toString('hex')}'` : value
}

function asObjects(read: QueryWithColumnsResult): Record<string, unknown>[] {
  return read.rows.map(row => Object.fromEntries(read.columns.map((column, position) => [column.name, row[position]])))
}

async function openItems(driverCase: DriverCase, name: string): Promise<{ db: Database; path: string }> {
  const path = workspace.path(`${name}.db`)
  const db = await workspace
    .registry({ driver: driverCase.driver() })
    .open(name, driverCase.details === 'full' ? path : `${name}.db`)
  await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, payload BLOB, price REAL)')
  const insert = 'INSERT INTO items (id, label, amount, payload, price) VALUES (?, ?, ?, ?, ?)'
  const wide = driverCase.exactIntegers
    ? [2n ** 53n + 1n, -(2n ** 63n)]
    : [Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER]
  await db.execute(insert, [1, MULTILINGUAL, wide[0], EVERY_BYTE, 1.5])
  await db.execute(insert, [2, '', wide[1], new Uint8Array(0), null])
  await db.execute(insert, [3, null, 7, Uint8Array.from([255]), -0.25])
  return { db, path }
}

describe.each(DRIVER_CASES)('queryWithColumns on $label', driverCase => {
  const supported = (): boolean => driverCase.driver().capabilities.columnDetails === true

  it('returns columns from the schema that produced the rows after a cached read changes shape', async () => {
    const { db, path } = await openItems(driverCase, 'schema')
    if (!supported()) return
    const sql = 'SELECT * FROM items ORDER BY id'
    const empty = 'SELECT * FROM items WHERE id < 0'
    for (const statement of [sql, empty])
      for (let repeat = 0; repeat < 4; repeat++) await db.queryWithColumns(statement)
    const oracle = new BetterSqlite3(path, { readonly: true })
    try {
      const limitsToCheck = [undefined, { maxRows: 1 }, { maxRows: 0 }, { maxBytes: 0 }]
      for (const [index, limits] of limitsToCheck.entries()) {
        await db.execute(`ALTER TABLE items ADD COLUMN extra${index} TEXT`)
        const prepared = oracle.prepare(sql)
        prepared.all()
        const expected = prepared.columns().map(column => column.name)
        for (const statement of [sql, empty]) {
          const result = await db.queryWithColumns(statement, [], limits)
          expect(result.columns.map(column => column.name)).toEqual(expected)
          for (const row of result.rows) expect(row).toHaveLength(expected.length)
        }
      }
    } finally {
      oracle.close()
    }
  })

  it('returns every column in order, keeps repeated and numeric names apart, and returns each value as query does', async () => {
    const { db, path } = await openItems(driverCase, 'order')
    if (!supported()) {
      await expect(db.queryWithColumns(SELECT_REPEATED)).rejects.toMatchObject({ code: 'COLUMN_DETAILS_UNSUPPORTED' })
      return
    }

    const read = await db.queryWithColumns(SELECT_REPEATED)

    expect(read.columns.map(column => column.name)).toEqual([
      'id',
      'id',
      'label',
      'amount',
      'payload',
      'price',
      'total',
      '1',
    ])
    expect(read.truncated).toBeNull()
    if (driverCase.details === 'full') {
      expect(read.columns[1]).toEqual({
        name: 'id',
        declaredType: 'INTEGER',
        table: 'items',
        column: 'id',
        database: 'main',
      })
      expect(read.columns[4]).toEqual({
        name: 'payload',
        declaredType: 'BLOB',
        table: 'items',
        column: 'payload',
        database: 'main',
      })
      expect(read.columns[7]).toEqual({
        name: '1',
        declaredType: 'TEXT',
        table: 'items',
        column: 'label',
        database: 'main',
      })
    }
    expect(read.columns[6]).toEqual({ name: 'total', declaredType: null, table: null, column: null, database: null })
    if (driverCase.exactIntegers) {
      expect(read.rows.map(row => row.map(hexOf))).toEqual([
        [1, 1, MULTILINGUAL, 2n ** 53n + 1n, hexOf(EVERY_BYTE), 1.5, 2n ** 53n + 1n, MULTILINGUAL],
        [2, 2, '', -(2n ** 63n), "x''", null, -(2n ** 63n), ''],
        [3, 3, null, 7, "x'ff'", -0.25, 7, null],
      ])
      expect(columnRaw(path, 'SELECT count(*) FROM items')).toEqual([3n])
    }
    const unique = 'SELECT id, label, amount, payload, price, amount + 0 AS total FROM items ORDER BY id'
    expect(asObjects(await db.queryWithColumns(unique)).map(row => Object.values(row).map(hexOf))).toEqual(
      (await db.query<Record<string, unknown>>(unique)).map(row => Object.values(row).map(hexOf)),
    )
  })

  it('stops at maxRows and at maxBytes against blobs, long text, many columns, and an endless recursive read', async () => {
    const { db } = await openItems(driverCase, 'limits')
    if (!supported()) return
    const select = 'SELECT id, label, amount, payload, price FROM items ORDER BY id'
    const sizes = [
      [1, MULTILINGUAL, 0, EVERY_BYTE, 0],
      [2, '', 0, new Uint8Array(0), null],
      [3, null, 0, Uint8Array.from([255]), 0],
    ].map(oracleBytes)

    const exact = await db.queryWithColumns(select, [], { maxBytes: sizes[0] + sizes[1] })
    const short = await db.queryWithColumns(select, [], { maxBytes: sizes[0] + sizes[1] - 1 })
    const all = await db.queryWithColumns(select, [], { maxBytes: sizes[0] + sizes[1] + sizes[2] })
    expect([exact.rows.length, exact.truncated]).toEqual([2, 'bytes'])
    expect([short.rows.length, short.truncated]).toEqual([1, 'bytes'])
    expect([all.rows.length, all.truncated]).toEqual([3, null])

    expect(await db.queryWithColumns(select, [], { maxRows: 3 })).toMatchObject({ truncated: null })
    expect((await db.queryWithColumns(select, [], { maxRows: 2 })).truncated).toBe('rows')
    const none = await db.queryWithColumns(select, [], { maxRows: 0, maxBytes: 0 })
    expect([none.rows, none.columns.length, none.truncated]).toEqual([[], 5, 'rows'])
    const empty = await db.queryWithColumns('SELECT id FROM items WHERE id < 0', [], { maxRows: 0 })
    expect([empty.rows, empty.truncated]).toEqual([[], null])

    const longText = 'ü'.repeat(100_000)
    const text = await db.queryWithColumns('SELECT ? AS body', [longText], { maxBytes: 8 + utf8Bytes(longText) - 1 })
    expect([text.rows, text.truncated]).toEqual([[], 'bytes'])

    const wide = Array.from({ length: MANY_COLUMNS }, (_, index) => `NULL AS c${index}`).join(', ')
    const wideRow = await db.queryWithColumns(`SELECT ${wide}`, [], { maxBytes: MANY_COLUMNS * 8 - 1 })
    expect([wideRow.rows.length, wideRow.columns.length, wideRow.truncated]).toEqual([0, MANY_COLUMNS, 'bytes'])
    const fits = await db.queryWithColumns(`SELECT ${wide}`, [], { maxBytes: MANY_COLUMNS * 8 })
    expect([fits.rows.length, fits.truncated]).toEqual([1, null])

    const blobBytes = 1 << 20
    const endless = await db.queryWithColumns(ENDLESS_BLOBS, [blobBytes], { maxBytes: 8 * blobBytes })
    const perRow = 8 + 8 + blobBytes
    expect([endless.rows.length, endless.truncated]).toEqual([Math.floor((8 * blobBytes) / perRow), 'bytes'])
    expect(endless.rows.map(row => row[0])).toEqual(
      Array.from({ length: endless.rows.length }, (_, index) => index + 1),
    )
    const counted = await db.queryWithColumns(ENDLESS_BLOBS, [16], { maxRows: 1000 })
    expect([counted.rows.length, counted.truncated, counted.rows.at(-1)?.[0]]).toEqual([1000, 'rows', 1000])
  })

  it('refuses a limit that is not a non-negative safe integer before SQLite reads anything', async () => {
    const { db } = await openItems(driverCase, 'refusals')
    let started = 0
    const removeHook = db.onBeforeQuery(() => {
      started++
    })
    for (const limit of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 53, '5', 5n, null, true]) {
      for (const key of ['maxRows', 'maxBytes']) {
        await expect(
          db.queryWithColumns('SELECT id FROM items', [], { [key]: limit }),
          `${key} ${String(limit)}`,
        ).rejects.toMatchObject({
          code: 'INVALID_READ_LIMIT',
        })
      }
    }
    removeHook()
    expect(started).toBe(0)
  })

  it('runs the query hooks, refuses internal tables, and lets a hook read through queryWithColumns again', async () => {
    const { db, path } = await openItems(driverCase, 'hooks')
    if (!supported()) return
    const seen: string[] = []
    const nested: unknown[] = []
    const removeBefore = db.onBeforeQuery(context => {
      seen.push(`before ${context.sql}`)
      if (context.sql === 'SELECT id FROM items ORDER BY id') {
        nested.push(db.queryWithColumns('SELECT count(*) AS n FROM items'))
      }
      if (context.sql.includes('refused by the hook')) throw new Error('the hook refused it')
    })
    const removeAfter = db.onAfterQuery(context => {
      seen.push(`after ${context.sql} ${context.error === undefined ? 'ok' : 'failed'}`)
    })

    const read = await db.queryWithColumns('SELECT id FROM items ORDER BY id', [], { maxRows: 2 })
    await expect(db.queryWithColumns("SELECT 'refused by the hook'")).rejects.toThrow('the hook refused it')
    await expect(db.queryWithColumns("SELECT * FROM '_sirannon_meta'")).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    const inner = (await Promise.all(nested)) as QueryWithColumnsResult[]
    removeBefore()
    removeAfter()

    expect(read).toMatchObject({ rows: [[1], [2]], truncated: 'rows' })
    expect(inner.map(result => result.rows)).toEqual([[[3]]])
    expect(seen).toContain('before SELECT id FROM items ORDER BY id')
    expect(seen).toContain('after SELECT id FROM items ORDER BY id ok')
    expect(seen).toContain('after SELECT count(*) AS n FROM items ok')
    expect(seen).not.toContain("after SELECT 'refused by the hook' ok")

    if (driverCase.details === 'full') {
      await expect(db.queryWithColumns('INSERT INTO items (id) VALUES (99)')).rejects.toMatchObject({
        code: 'QUERY_ERROR',
      })
      expect(columnRaw(path, 'SELECT count(*) FROM items WHERE id = 99')).toEqual([0n])
    }
  })
})
