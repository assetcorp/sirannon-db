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
})
