import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { QueryError } from '../../../core/errors.js'
import { columnRaw, countRaw, createWorkspace, DRIVER_PLACEMENTS, readRaw, type Workspace } from './harness.js'

const ITEMS_SCHEMA =
  'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, qty INTEGER NOT NULL CHECK (qty >= 0))'
const INSERT_ITEM = 'INSERT INTO items (name, qty) VALUES (?, ?)'
const SEEDED_ITEMS = 12
const MORE_STATEMENTS_THAN_ANY_CACHE = 300
const LARGE_BATCH = 5000

type Call = [label: string, call: (db: Database, sql: string) => Promise<unknown>]

const CALLS: Call[] = [
  ['query', (db, sql) => db.query(sql)],
  ['queryOne', (db, sql) => db.queryOne(sql)],
  ['execute', (db, sql) => db.execute(sql)],
  ['executeBatch', (db, sql) => db.executeBatch(sql, [[]])],
  ['executeTransaction', (db, sql) => db.executeTransaction([{ sql }])],
  ['transaction query', (db, sql) => db.transaction(tx => tx.query(sql))],
  ['transaction execute', (db, sql) => db.transaction(tx => tx.execute(sql))],
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('reads-and-writes')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(DRIVER_PLACEMENTS)('reads and writes with $label', ({ driver, writerWorker }) => {
  async function openItems(): Promise<{ db: Database; path: string }> {
    const path = workspace.path('items.db')
    const db = await workspace.registry({ driver: driver() }).open('items', path, { writerWorker })
    await db.execute(ITEMS_SCHEMA)
    await db.executeBatch(
      INSERT_ITEM,
      Array.from({ length: SEEDED_ITEMS }, (_, index) => [`item-${index}`, index]),
    )
    return { db, path }
  }

  it('reads rows and reports the changes of each write as the file records them', async () => {
    const { db, path } = await openItems()
    const threshold = 5
    const matching = SEEDED_ITEMS - threshold

    const updated = await db.execute('UPDATE items SET qty = qty + 100 WHERE qty >= ?', [threshold])
    const deleted = await db.execute('DELETE FROM items WHERE qty < :limit', { limit: 2 })
    const untouched = await db.execute('UPDATE items SET qty = 0 WHERE name = ?', ['no-such-item'])

    expect(updated.changes).toBe(matching)
    expect(deleted.changes).toBe(2)
    expect(untouched.changes).toBe(0)
    const raw = readRaw<{ id: bigint; name: string; qty: bigint }>(path, 'SELECT id, name, qty FROM items ORDER BY id')
    expect(await db.query('SELECT id, name, qty FROM items ORDER BY id')).toEqual(
      raw.map(row => ({ id: Number(row.id), name: row.name, qty: Number(row.qty) })),
    )
    expect(await db.queryOne('SELECT name FROM items WHERE qty > ? ORDER BY id', [100])).toEqual({
      name: `item-${threshold}`,
    })
    expect(await db.queryOne('SELECT name FROM items WHERE qty < 0')).toBeUndefined()
    expect(await db.query('SELECT name FROM items WHERE qty < 0')).toEqual([])
  })

  it('raises QueryError carrying the statement for every statement that SQLite cannot prepare or execute', async () => {
    const { db, path } = await openItems()
    const before = readRaw(path, 'SELECT * FROM items ORDER BY id')
    const failing = [
      'SELEC name FROM items',
      'SELECT * FROM missing_table',
      'SELECT missing_column FROM items',
      'INSERT INTO items (name, qty) VALUES (NULL, 1)',
      "INSERT INTO items (name, qty) VALUES ('item-0', 1)",
      'UPDATE items SET qty = -1',
      '',
      '   ',
      '-- a comment and nothing else',
    ]

    for (const sql of failing) {
      for (const [label, call] of CALLS) {
        const outcome = await call(db, sql).then(
          () => null,
          (err: unknown) => err,
        )
        expect(outcome, `${label}: ${JSON.stringify(sql)}`).toBeInstanceOf(QueryError)
        expect(outcome, `${label}: ${JSON.stringify(sql)}`).toMatchObject({ code: 'QUERY_ERROR', sql })
      }
    }

    expect(readRaw(path, 'SELECT * FROM items ORDER BY id')).toEqual(before)
  })

  it('returns correct results after more distinct statements than any statement cache holds', async () => {
    const { db, path } = await openItems()

    for (let index = 0; index < MORE_STATEMENTS_THAN_ANY_CACHE; index++) {
      await db.execute(`INSERT INTO items (name, qty) VALUES ('generated-${index}', ${index})`)
      expect(await db.queryOne(`SELECT ? * ${index + 1} AS product`, [3])).toEqual({ product: 3 * (index + 1) })
    }
    await expect(db.execute("INSERT INTO items (name, qty) VALUES ('generated-0', 0)")).rejects.toMatchObject({
      code: 'QUERY_ERROR',
    })
    await db.execute(`INSERT INTO items (name, qty) VALUES ('generated-${MORE_STATEMENTS_THAN_ANY_CACHE}', 1)`)

    expect(await db.queryOne('SELECT ? * 1 AS product', [7])).toEqual({ product: 7 })
    expect(countRaw(path, 'items')).toBe(SEEDED_ITEMS + MORE_STATEMENTS_THAN_ANY_CACHE + 1)
  })

  it('prepares a statement again once its table exists, and reads the columns that a schema change adds', async () => {
    const { db, path } = await openItems()
    const readRefunds = 'SELECT * FROM refunds ORDER BY id'
    const insertRefund = 'INSERT INTO refunds (amount) VALUES (?)'

    await expect(db.query(readRefunds)).rejects.toMatchObject({ code: 'QUERY_ERROR', sql: readRefunds })
    await expect(db.execute(insertRefund, [5])).rejects.toMatchObject({ code: 'QUERY_ERROR', sql: insertRefund })
    await db.execute('CREATE TABLE refunds (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)')
    await db.execute(insertRefund, [5])
    expect(await db.query(readRefunds)).toEqual([{ id: 1, amount: 5 }])

    await db.execute("ALTER TABLE refunds ADD COLUMN reason TEXT NOT NULL DEFAULT 'damaged'")
    await db.execute(insertRefund, [7])
    expect(await db.query(readRefunds)).toEqual([
      { id: 1, amount: 5, reason: 'damaged' },
      { id: 2, amount: 7, reason: 'damaged' },
    ])
    expect(columnRaw(path, 'SELECT reason FROM refunds ORDER BY id')).toEqual(['damaged', 'damaged'])
  })

  it('applies a batch completely or not at all', async () => {
    const { db, path } = await openItems()
    const before = readRaw(path, 'SELECT * FROM items ORDER BY id')

    await expect(
      db.executeBatch(INSERT_ITEM, [
        ['batch-a', 1],
        ['batch-b', 2],
        ['item-3', 3],
        ['batch-d', 4],
      ]),
    ).rejects.toMatchObject({ code: 'QUERY_ERROR', sql: INSERT_ITEM })
    await expect(
      db.executeBatch('UPDATE items SET qty = qty - ? WHERE name = ?', [
        [0, 'item-1'],
        [5, 'item-2'],
      ]),
    ).rejects.toMatchObject({
      code: 'QUERY_ERROR',
    })
    expect(readRaw(path, 'SELECT * FROM items ORDER BY id')).toEqual(before)

    await expect(db.executeBatch(INSERT_ITEM, [])).resolves.toEqual([])
    const loaded = await db.executeBatch(
      INSERT_ITEM,
      Array.from({ length: LARGE_BATCH }, (_, index) => [`bulk-${index}`, index]),
    )
    expect(loaded).toHaveLength(LARGE_BATCH)
    expect(loaded.every(result => result.changes === 1)).toBe(true)
    expect(loaded.at(-1)?.lastInsertRowId).toBe(SEEDED_ITEMS + LARGE_BATCH)
    expect(countRaw(path, 'items')).toBe(SEEDED_ITEMS + LARGE_BATCH)
  })
})
