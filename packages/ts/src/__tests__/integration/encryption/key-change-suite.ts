import { randomBytes } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import { filesHoldingText, readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'

const SEEDED_ORDERS = 3000
const WRITE_STEPS_AFTER_SNAPSHOT = 40
const ROWS_PER_SHAPE = 400
const GAP_RANGE = { from: 100, to: 140 }
const GAP_EVERY = 7

type Row = Record<string, unknown>

export type IndependentReader = <T>(path: string, key: Uint8Array | null, sql: string) => T[]

interface RowidShape {
  table: string
  statements: string[]
  insert: (n: number) => { sql: string; params: unknown[] }
}

const ROWID_SHAPES: RowidShape[] = [
  {
    table: 'visit_log',
    statements: ['CREATE TABLE visit_log (note TEXT NOT NULL)'],
    insert: n => ({ sql: 'INSERT INTO visit_log (note) VALUES (?)', params: [`visit-${n}`] }),
  },
  {
    table: 'searches',
    statements: ['CREATE TABLE searches (note TEXT NOT NULL)', 'CREATE INDEX searches_note ON searches (note)'],
    insert: n => ({ sql: 'INSERT INTO searches (note) VALUES (?)', params: [`search-${n}`] }),
  },
  {
    table: 'coupons',
    statements: ['CREATE TABLE coupons (code TEXT PRIMARY KEY, note TEXT NOT NULL)'],
    insert: n => ({ sql: 'INSERT INTO coupons (code, note) VALUES (?, ?)', params: [`C-${n}`, `coupon-${n}`] }),
  },
  {
    table: 'payments',
    statements: ['CREATE TABLE payments (id INTEGER PRIMARY KEY, note TEXT NOT NULL)'],
    insert: n => ({ sql: 'INSERT INTO payments (note) VALUES (?)', params: [`payment-${n}`] }),
  },
  {
    table: 'invoices',
    statements: ['CREATE TABLE invoices (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT NOT NULL)'],
    insert: n => ({ sql: 'INSERT INTO invoices (note) VALUES (?)', params: [`invoice-${n}`] }),
  },
]

function rowidsAndNotes(rows: readonly Row[]): [number, string][] {
  return rows.map(row => [Number(row.r), String(row.note)])
}

function keyFunction(bytes: Uint8Array, id?: string) {
  return () => (id === undefined ? Uint8Array.from(bytes) : { key: Uint8Array.from(bytes), id })
}

function keyWritingAfterSnapshot(bytes: Uint8Array, id: string, writes: () => Promise<void>) {
  let calls = 0
  return async () => {
    calls++
    if (calls === 2) await writes()
    return { key: Uint8Array.from(bytes), id }
  }
}

function numbers(rows: readonly Row[], column: string): number[] {
  return rows.map(row => Number(row[column]))
}

class Oracle {
  readonly orders = new Map<number, { customer: string; total: number }>()
  readonly tags = new Set<string>()
  readonly ledger: number[] = []
  readonly priced = new Map<number, number>()
  readonly notes = new Map<number, string>()
  readonly addresses = new Map<number, number>()
  ordersInserted = 0
}

async function createSchema(db: Database): Promise<void> {
  for (const statement of [
    'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total REAL NOT NULL)',
    'CREATE TABLE audit (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL)',
    'CREATE TRIGGER orders_audit AFTER INSERT ON orders BEGIN INSERT INTO audit (order_id) VALUES (NEW.id); END',
    'CREATE TABLE tags (name TEXT NOT NULL, weight INTEGER NOT NULL, PRIMARY KEY (name, weight)) WITHOUT ROWID',
    'CREATE TABLE ledger (n INTEGER NOT NULL, secret TEXT NOT NULL)',
    'CREATE TABLE priced (id INTEGER PRIMARY KEY, net INTEGER NOT NULL, gross INTEGER AS (net * 2) STORED)',
    'CREATE VIRTUAL TABLE notes USING fts5(body)',
    'CREATE TABLE customers (id INTEGER PRIMARY KEY)',
    'CREATE TABLE addresses (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers (id) ON DELETE CASCADE)',
  ]) {
    await db.execute(statement)
  }
}

async function writeStep(db: Database, oracle: Oracle, step: number): Promise<void> {
  const customer = `customer-${step.toString().padStart(6, '0')}-confidential`
  const order = Number(
    (await db.execute('INSERT INTO orders (customer, total) VALUES (?, ?)', [customer, step * 1.5])).lastInsertRowId,
  )
  oracle.orders.set(order, { customer, total: step * 1.5 })
  oracle.ordersInserted++
  const [oldest] = oracle.orders.keys()
  if (step % 3 === 0 && oldest !== undefined) {
    await db.execute('UPDATE orders SET total = total + 1 WHERE id = ?', [oldest])
    const entry = oracle.orders.get(oldest)
    if (entry) entry.total += 1
  }
  if (step % 5 === 0 && oldest !== undefined) {
    await db.execute('DELETE FROM orders WHERE id = ?', [oldest])
    oracle.orders.delete(oldest)
  }
  const tag = `tag-${step}:${step % 7}`
  await db.execute('INSERT INTO tags (name, weight) VALUES (?, ?)', [`tag-${step}`, step % 7])
  oracle.tags.add(tag)
  await db.execute('INSERT INTO ledger (n, secret) VALUES (?, ?)', [step, customer])
  oracle.ledger.push(step)
  if (step % 6 === 0) {
    const removed = oracle.ledger.shift()
    await db.execute('DELETE FROM ledger WHERE n = ?', [removed])
  }
  const priced = Number((await db.execute('INSERT INTO priced (net) VALUES (?)', [step])).lastInsertRowId)
  oracle.priced.set(priced, step)
  const note = Number(
    (await db.execute('INSERT INTO notes (body) VALUES (?)', [`alpha${step} shared`])).lastInsertRowId,
  )
  oracle.notes.set(note, `alpha${step} shared`)
  const [oldestNote] = oracle.notes.keys()
  if (step % 4 === 0 && oldestNote !== undefined) {
    await db.execute('DELETE FROM notes WHERE rowid = ?', [oldestNote])
    oracle.notes.delete(oldestNote)
  }
  const owner = Number((await db.execute('INSERT INTO customers DEFAULT VALUES')).lastInsertRowId)
  for (let n = 0; n < 2; n++) {
    const address = Number(
      (await db.execute('INSERT INTO addresses (customer_id) VALUES (?)', [owner])).lastInsertRowId,
    )
    oracle.addresses.set(address, owner)
  }
  if (step % 5 === 0) {
    const [first] = oracle.addresses.values()
    if (first !== undefined) {
      await db.execute('DELETE FROM customers WHERE id = ?', [first])
      for (const [address, ownerId] of oracle.addresses) if (ownerId === first) oracle.addresses.delete(address)
    }
  }
}

async function expectMatchesOracle(db: Database, oracle: Oracle): Promise<void> {
  const orders = await db.query<Row>('SELECT id, customer, total FROM orders ORDER BY id')
  expect(orders.map(row => [Number(row.id), row.customer, Number(row.total)])).toEqual(
    [...oracle.orders].map(([id, entry]) => [id, entry.customer, entry.total]),
  )
  expect(Number((await db.query<Row>('SELECT count(*) AS n FROM audit'))[0]?.n)).toBe(oracle.ordersInserted)
  expect((await db.query<Row>("SELECT name || ':' || weight AS tag FROM tags")).map(row => row.tag).sort()).toEqual(
    [...oracle.tags].sort(),
  )
  expect(numbers(await db.query<Row>('SELECT n FROM ledger ORDER BY rowid'), 'n')).toEqual(oracle.ledger)
  const priced = await db.query<Row>('SELECT id, net, gross FROM priced ORDER BY id')
  expect(priced.map(row => [Number(row.id), Number(row.net), Number(row.gross)])).toEqual(
    [...oracle.priced].map(([id, net]) => [id, net, net * 2]),
  )
  const matched = await db.query<Row>("SELECT rowid AS id, body FROM notes WHERE notes MATCH 'shared' ORDER BY rowid")
  expect(matched.map(row => [Number(row.id), row.body])).toEqual([...oracle.notes])
  const addresses = await db.query<Row>('SELECT id, customer_id FROM addresses ORDER BY id')
  expect(addresses.map(row => [Number(row.id), Number(row.customer_id)])).toEqual([...oracle.addresses])
}

export function describeKeyChange(
  label: string,
  newSirannon: () => Sirannon,
  readIndependently: IndependentReader,
): void {
  describe(`a key change on ${label}`, () => {
    let dir: string
    let sirannon: Sirannon

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'sirannon-key-change-'))
      sirannon = newSirannon()
    })

    afterEach(async () => {
      await sirannon.shutdown()
      rmSync(dir, { recursive: true, force: true })
    })

    it('keeps every row, rowid, trigger, FTS5 index, cascade, and generated column of a database written to during the change', async () => {
      const path = join(dir, 'shop.db')
      const firstKey = randomBytes(32)
      const db = await sirannon.open('shop', path, { encryption: { key: keyFunction(firstKey) } })
      await createSchema(db)
      const oracle = new Oracle()
      for (let step = 0; step < SEEDED_ORDERS; step++) await writeStep(db, oracle, step)

      const secondKey = randomBytes(32)
      const afterSnapshot = async () => {
        for (let step = SEEDED_ORDERS; step < SEEDED_ORDERS + WRITE_STEPS_AFTER_SNAPSHOT; step++) {
          await writeStep(db, oracle, step)
        }
      }
      await db.execute('CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT NOT NULL)')
      const events: number[] = []
      let writing = true
      const eventWriter = (async () => {
        while (writing) {
          events.push(Number((await db.execute("INSERT INTO events (label) VALUES ('tick')")).lastInsertRowId))
          await new Promise<void>(resolve => setImmediate(resolve))
        }
      })()
      const counted: number[] = []
      const readFailures: unknown[] = []
      const eventReader = (async () => {
        while (writing) {
          try {
            counted.push(Number((await db.query<Row>('SELECT count(*) AS n FROM events'))[0]?.n))
          } catch (err) {
            readFailures.push(err)
          }
          await new Promise<void>(resolve => setImmediate(resolve))
        }
      })()
      const status = await db
        .changeEncryption({ key: keyWritingAfterSnapshot(secondKey, 'second', afterSnapshot) })
        .finally(() => {
          writing = false
        })
      await eventWriter
      await eventReader

      expect(readFailures).toEqual([])
      expect(counted.length).toBeGreaterThan(0)
      expect(counted.every((count, at) => at === 0 || count >= (counted[at - 1] ?? 0))).toBe(true)
      expect(status).toMatchObject({ encrypted: true, keyId: 'second', change: { state: 'done' } })
      expect(oracle.ordersInserted).toBe(SEEDED_ORDERS + WRITE_STEPS_AFTER_SNAPSHOT)
      expect(numbers(await db.query<Row>('SELECT id FROM events ORDER BY id'), 'id')).toEqual(events)
      await expectMatchesOracle(db, oracle)
      await writeStep(db, oracle, SEEDED_ORDERS * 10)
      await expectMatchesOracle(db, oracle)

      await sirannon.close('shop')
      const independent = readIndependently<Row>(path, secondKey, 'SELECT id, customer FROM orders ORDER BY id')
      expect(numbers(independent, 'id')).toEqual([...oracle.orders.keys()])
      expect(() => readIndependently(path, firstKey, 'SELECT count(*) FROM orders')).toThrow()
      expect(() => readWithStockSqlite(path, 'SELECT count(*) FROM orders')).toThrow()
      const secrets = [...oracle.orders.values()].slice(-50).map(entry => entry.customer)
      expect(filesHoldingText([path, `${path}-wal`], secrets)).toEqual([])
    })

    it('never hands out an AUTOINCREMENT id again after a key change, even one whose row was deleted during the change', async () => {
      const db = await sirannon.open('invoices', join(dir, 'invoices.db'), {
        encryption: { key: keyFunction(randomBytes(32)) },
      })
      await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY AUTOINCREMENT, number TEXT NOT NULL)')
      const issued: number[] = []
      for (let n = 0; n < 200; n++) {
        issued.push(
          Number((await db.execute('INSERT INTO invoices (number) VALUES (?)', [`INV-${n}`])).lastInsertRowId),
        )
      }
      const voidHighest = async () => {
        const id = Number((await db.execute('INSERT INTO invoices (number) VALUES (?)', ['INV-void'])).lastInsertRowId)
        issued.push(id)
        await db.execute('DELETE FROM invoices WHERE id = ?', [id])
      }
      const status = await db.changeEncryption({
        key: keyWritingAfterSnapshot(randomBytes(32), 'invoices', voidHighest),
      })

      expect(status.change.state).toBe('done')
      const next = Number((await db.execute('INSERT INTO invoices (number) VALUES (?)', ['INV-after'])).lastInsertRowId)
      expect(issued).toHaveLength(201)
      expect(next).toBeGreaterThan(Math.max(...issued))
    })

    it('either removes a row that INSERT OR REPLACE deleted during the change, or stops and leaves the database as it was', async () => {
      const firstKey = randomBytes(32)
      const path = join(dir, 'members.db')
      const db = await sirannon.open('members', path, { encryption: { key: keyFunction(firstKey) } })
      await db.execute('CREATE TABLE members (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)')
      const members = new Map<number, string>()
      for (let id = 1; id <= 2000; id++) {
        await db.execute('INSERT INTO members (id, email) VALUES (?, ?)', [id, `member${id}@example.com`])
        members.set(id, `member${id}@example.com`)
      }
      const replaceAndRemove = async () => {
        for (let id = 1; id <= 10; id++) {
          await db.execute('INSERT OR REPLACE INTO members (id, email) VALUES (?, ?)', [100_000 + id, members.get(id)])
          await db.execute('DELETE FROM members WHERE id = ?', [100_000 + id])
          members.delete(id)
        }
      }
      const outcome = await db
        .changeEncryption({ key: keyWritingAfterSnapshot(randomBytes(32), 'members', replaceAndRemove) })
        .then(
          status => status.change.state,
          (err: { code?: string }) => err.code,
        )

      expect(members.size).toBe(1990)
      expect(['done', 'ENCRYPTION_CHANGE_ABORTED']).toContain(outcome)
      const rows = await db.query<Row>('SELECT id, email FROM members ORDER BY id')
      expect(rows.map(row => [Number(row.id), row.email])).toEqual([...members])
      if (outcome === 'ENCRYPTION_CHANGE_ABORTED') {
        await sirannon.close('members')
        expect(readIndependently<Row>(path, firstKey, 'SELECT count(*) AS n FROM members')[0]?.n).toBe(members.size)
      }
    })

    it('keeps the rowid of every row in every kind of rowid table, with gaps from deleted rows and writes during the change', async () => {
      const path = join(dir, 'rowids.db')
      const db = await sirannon.open('rowids', path, { encryption: { key: keyFunction(randomBytes(32)) } })
      for (const shape of ROWID_SHAPES) {
        for (const statement of shape.statements) await db.execute(statement)
        for (let n = 1; n <= ROWS_PER_SHAPE; n++) {
          const { sql, params } = shape.insert(n)
          await db.execute(sql, params)
        }
        await db.execute(
          `DELETE FROM ${shape.table} WHERE rowid % ${GAP_EVERY} = 3 OR rowid BETWEEN ${GAP_RANGE.from} AND ${GAP_RANGE.to}`,
        )
      }
      const expected = new Map<string, Map<number, string>>()
      for (const shape of ROWID_SHAPES) {
        const rows = await db.query<Row>(`SELECT rowid AS r, note FROM ${shape.table} ORDER BY rowid`)
        expected.set(shape.table, new Map(rowidsAndNotes(rows)))
      }
      const writeDuringChange = async () => {
        for (const shape of ROWID_SHAPES) {
          const rows = expected.get(shape.table)
          if (rows === undefined) throw new Error(`no rows recorded for ${shape.table}`)
          const [deletedRowid, updatedRowid] = [...rows.keys()].slice(200, 202)
          if (deletedRowid === undefined || updatedRowid === undefined) throw new Error('too few rows recorded')
          await db.execute(`DELETE FROM ${shape.table} WHERE note = ?`, [rows.get(deletedRowid)])
          rows.delete(deletedRowid)
          await db.execute(`UPDATE ${shape.table} SET note = note || '-revised' WHERE note = ?`, [
            rows.get(updatedRowid),
          ])
          rows.set(updatedRowid, `${rows.get(updatedRowid)}-revised`)
          const { sql, params } = shape.insert(ROWS_PER_SHAPE + 1)
          rows.set(Number((await db.execute(sql, params)).lastInsertRowId), String(params.at(-1)))
        }
      }
      const secondKey = randomBytes(32)
      const status = await db.changeEncryption({ key: keyWritingAfterSnapshot(secondKey, 'rowids', writeDuringChange) })

      expect(status.change.state).toBe('done')
      for (const shape of ROWID_SHAPES) {
        const rows = await db.query<Row>(`SELECT rowid AS r, note FROM ${shape.table} ORDER BY rowid`)
        expect(rowidsAndNotes(rows), shape.table).toEqual([...(expected.get(shape.table) ?? [])])
      }
      await sirannon.close('rowids')
      for (const shape of ROWID_SHAPES) {
        const rows = readIndependently<Row>(
          path,
          secondKey,
          `SELECT rowid AS r, note FROM ${shape.table} ORDER BY rowid`,
        )
        expect(rowidsAndNotes(rows), shape.table).toEqual([...(expected.get(shape.table) ?? [])])
      }
    })
  })
}
