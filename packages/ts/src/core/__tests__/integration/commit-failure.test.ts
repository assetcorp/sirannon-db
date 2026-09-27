import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string
let sirannon: Sirannon

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-commit-failure-'))
  sirannon = new Sirannon({ driver: testDriver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

const SCHEMA = [
  'PRAGMA foreign_keys = ON',
  'CREATE TABLE customers (id INTEGER PRIMARY KEY)',
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED)',
]

describe('a transaction that SQLite refuses to commit', () => {
  it('fails with TRANSACTION_ERROR and keeps none of its writes', async () => {
    const db = await sirannon.open('main', join(tempDir, 'commit.db'))
    for (const sql of SCHEMA) await db.execute(sql)

    await expect(
      db.transaction(async tx => {
        await tx.execute('INSERT INTO orders (id, customer_id) VALUES (1, 42)')
      }),
    ).rejects.toMatchObject({ code: 'TRANSACTION_ERROR' })

    const rows = await db.query<{ count: number }>('SELECT count(*) AS count FROM orders')
    expect(rows[0]?.count).toBe(0)
  })

  it('fails a statement list with TRANSACTION_ERROR and keeps none of its writes', async () => {
    const db = await sirannon.open('main', join(tempDir, 'statements.db'))
    for (const sql of SCHEMA) await db.execute(sql)

    await expect(
      db.executeTransaction([
        { sql: 'INSERT INTO customers (id) VALUES (1)' },
        { sql: 'INSERT INTO orders (id, customer_id) VALUES (1, 42)' },
      ]),
    ).rejects.toMatchObject({ code: 'TRANSACTION_ERROR' })

    const rows = await db.query<{ count: number }>('SELECT count(*) AS count FROM customers')
    expect(rows[0]?.count).toBe(0)
  })

  it.each([
    { writer: 'the main thread', writerWorker: false },
    { writer: 'the writer worker', writerWorker: true },
  ])('commits valid writes that share a group with a write SQLite refuses to commit, on $writer', async ({
    writerWorker,
  }) => {
    const db = await sirannon.open('main', join(tempDir, 'grouped.db'), { writerWorker })
    for (const sql of SCHEMA) await db.execute(sql)

    const [valid, invalid, alsoValid] = await Promise.allSettled([
      db.execute('INSERT INTO customers (id) VALUES (1)'),
      db.execute('INSERT INTO orders (id, customer_id) VALUES (1, 42)'),
      db.execute('INSERT INTO customers (id) VALUES (2)'),
    ])

    expect(valid.status).toBe('fulfilled')
    expect(alsoValid.status).toBe('fulfilled')
    expect(invalid).toMatchObject({ status: 'rejected', reason: { code: 'TRANSACTION_ERROR' } })
    const customers = await db.query<{ id: number }>('SELECT id FROM customers ORDER BY id')
    expect(customers.map(row => Number(row.id))).toEqual([1, 2])
    const orders = await db.query<{ count: number }>('SELECT count(*) AS count FROM orders')
    expect(orders[0]?.count).toBe(0)
  })

  it('gives each write in a group its own outcome when one statement fails and SQLite refuses the commit', async () => {
    const db = await sirannon.open('main', join(tempDir, 'isolated.db'))
    for (const sql of SCHEMA) await db.execute(sql)
    await db.execute('INSERT INTO customers (id) VALUES (1)')

    const [duplicate, orphan, valid] = await Promise.allSettled([
      db.execute('INSERT INTO customers (id) VALUES (1)'),
      db.execute('INSERT INTO orders (id, customer_id) VALUES (1, 42)'),
      db.execute('INSERT INTO orders (id, customer_id) VALUES (2, 1)'),
    ])

    expect(duplicate).toMatchObject({ status: 'rejected', reason: { code: 'QUERY_ERROR' } })
    expect(orphan).toMatchObject({ status: 'rejected', reason: { code: 'TRANSACTION_ERROR' } })
    expect(valid.status).toBe('fulfilled')
    const orders = await db.query<{ id: number }>('SELECT id FROM orders')
    expect(orders.map(row => Number(row.id))).toEqual([2])
  })
})
