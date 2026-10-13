import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery } from '../../../core/live/types.js'
import {
  allSettle,
  canonicalRows,
  createWorkspace,
  type Expectation,
  FAST_POLL_MS,
  readRaw,
  rowsOrStatus,
  settleToQueries,
  type Workspace,
  waitFor,
} from './harness.js'

const RANDOM_STEPS = 80
const RANDOM_SEEDS = [20_261_012, 7, 41]
const GOODS = `CREATE TABLE goods (
  id INTEGER PRIMARY KEY,
  label TEXT COLLATE NOCASE,
  price REAL,
  stock INTEGER,
  tag BLOB
)`
const LEDGER = 'CREATE TABLE ledger (entry TEXT NOT NULL, amount INTEGER NOT NULL)'
const LABELS = ['apple', 'Apple', 'a_b', 'aB', 'Banana', 'banana ', '', 'zebra', 'Ünïcode', '10', '9', 'a']
const PRICES = [null, -0, 0, 0.1, -1.5, 2, 1e308, -1e308, 5e-324, 19.99, 3]
const STOCKS: (number | bigint | null)[] = [
  null,
  0,
  1,
  -1,
  7,
  9007199254740991,
  9007199254740993n,
  -9223372036854775808n,
  9223372036854775807n,
]

interface RawCase {
  query: LiveQuery<unknown>
  sql: string
  params: unknown[]
}

let workspace: Workspace
const opened: LiveQuery<unknown>[] = []

beforeEach(() => {
  workspace = createWorkspace('hostile')
})

afterEach(async () => {
  for (const query of opened.splice(0)) await query.close().catch(() => {})
  await workspace.dispose()
})

function seededRandom(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state)
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296
  }
}

async function openCases(db: Database, specs: readonly [string, unknown[]][]): Promise<RawCase[]> {
  const cases: RawCase[] = []
  for (const [sql, params] of specs) {
    const query = (await db.live(sql, params, { rereadJitterMs: 3 })) as LiveQuery<unknown>
    opened.push(query)
    cases.push({ query, sql, params })
  }
  return cases
}

async function matchRawFile(path: string, cases: readonly RawCase[], quietMs = 0): Promise<void> {
  const expectations: Expectation[] = cases.map(entry => ({
    query: entry.query,
    rows: readRaw(path, entry.sql, entry.params),
    label: entry.sql,
  }))
  await allSettle(expectations, quietMs)
}

describe('live queries under random writes', () => {
  it.each(RANDOM_SEEDS)('match the same SELECT on a separate connection after every step, with seed %i', async seed => {
    const random = seededRandom(seed)
    const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)]
    const db = await workspace.open('random')
    await db.execute(GOODS)
    await db.execute(LEDGER)
    for (let id = 1; id <= 12; id++) {
      await db.execute('INSERT INTO goods VALUES (?, ?, ?, ?, ?)', [id, pick(LABELS), pick(PRICES), pick(STOCKS), null])
    }
    await db.execute("INSERT INTO ledger VALUES ('opening', 2), ('opening', 4), ('fee', 3)")
    const cases = await openCases(db, [
      ['SELECT id, label, price FROM goods WHERE stock > ? ORDER BY price DESC NULLS LAST, id', [0]],
      ['SELECT id, stock FROM goods ORDER BY stock, id LIMIT 7', []],
      ['SELECT id, label FROM goods ORDER BY label, id LIMIT 4 OFFSET 3', []],
      ["SELECT * FROM goods WHERE label LIKE 'a%' ORDER BY id DESC", []],
      ['SELECT entry, amount FROM ledger WHERE amount % 2 = 0 ORDER BY amount, entry', []],
      ['SELECT id, tag FROM goods WHERE tag IS NOT NULL ORDER BY tag, id', []],
      ['SELECT id FROM goods WHERE price IS NULL OR stock IS NULL ORDER BY id LIMIT 3', []],
    ])
    let nextId = 100

    const randomTag = (): Uint8Array | null =>
      random() < 0.3
        ? null
        : new Uint8Array(Array.from({ length: Math.floor(random() * 4) }, () => Math.floor(random() * 256)))
    const anyId = async (): Promise<number> => {
      const rows = await db.query<{ id: number }>('SELECT id FROM goods ORDER BY id')
      return rows.length === 0 ? -1 : pick(rows).id
    }
    const randomWrite = async (): Promise<{ sql: string; params: unknown[] }> => {
      const choice = random()
      if (choice < 0.3) {
        return {
          sql: 'INSERT INTO goods VALUES (?, ?, ?, ?, ?)',
          params: [nextId++, pick(LABELS), pick(PRICES), pick(STOCKS), randomTag()],
        }
      }
      if (choice < 0.55) {
        const column = pick(['label', 'price', 'stock', 'tag'])
        const value =
          column === 'label'
            ? pick(LABELS)
            : column === 'price'
              ? pick(PRICES)
              : column === 'stock'
                ? pick(STOCKS)
                : randomTag()
        return { sql: `UPDATE goods SET ${column} = ? WHERE id = ?`, params: [value, await anyId()] }
      }
      if (choice < 0.65) return { sql: 'UPDATE goods SET id = ? WHERE id = ?', params: [nextId++, await anyId()] }
      if (choice < 0.8) return { sql: 'DELETE FROM goods WHERE id = ?', params: [await anyId()] }
      if (choice < 0.87)
        return {
          sql: 'INSERT INTO ledger VALUES (?, ?)',
          params: [pick(['fee', 'sale', 'refund']), Math.floor(random() * 10)],
        }
      if (choice < 0.93)
        return {
          sql: 'UPDATE ledger SET amount = amount + 1 WHERE rowid = (SELECT max(rowid) FROM ledger)',
          params: [],
        }
      return {
        sql: 'UPDATE ledger SET rowid = (SELECT max(rowid) FROM ledger) + 1 WHERE rowid = (SELECT min(rowid) FROM ledger)',
        params: [],
      }
    }

    for (let step = 0; step < RANDOM_STEPS; step++) {
      const shape = random()
      if (shape < 0.6) {
        const write = await randomWrite()
        await db.execute(write.sql, write.params)
      } else if (shape < 0.9) {
        const writes = []
        for (let count = 1 + Math.floor(random() * 12); count > 0; count--) writes.push(await randomWrite())
        await db.executeTransaction(writes)
      } else {
        await db.execute('UPDATE goods SET stock = coalesce(stock, 0) % 5, price = price * -1 WHERE id % 2 = ?', [
          step % 2,
        ])
      }
      await matchRawFile(workspace.path('random'), cases)
    }
    await matchRawFile(workspace.path('random'), cases, 150)
  })
})

describe('values at the edges of each storage class', () => {
  it('pass through incremental changes exactly as SQLite stores them', async () => {
    const db = await workspace.open('edges')
    await db.execute('CREATE TABLE edges (id INTEGER PRIMARY KEY, v, w TEXT, x BLOB)')
    await db.execute("INSERT INTO edges VALUES (0, 'seed', 'seed', x'00')")
    const cases = await openCases(db, [
      ['SELECT id, v, w, x FROM edges ORDER BY id', []],
      ['SELECT id, v FROM edges ORDER BY v DESC, id LIMIT 6', []],
      ['SELECT id, x FROM edges WHERE x IS NOT NULL ORDER BY x, id', []],
    ])
    const edges: unknown[][] = [
      [1, 9223372036854775807n, '', new Uint8Array([])],
      [2, -9223372036854775808n, ' leading and trailing ', new Uint8Array([0, 0, 0])],
      [3, 9007199254740993n, 'x'.repeat(100_000), new Uint8Array(65_536).fill(255)],
      [4, -0, 'line\nbreak\ttab', null],
      [5, 1.7976931348623157e308, '\u{1f600} emoji and \u0000 nul', new Uint8Array([1, 2, 3])],
      [6, 5e-324, '123', new Uint8Array([0x7b, 0x7d])],
      [7, '9007199254740993', "it's quoted", null],
      [8, Number.NEGATIVE_INFINITY, '{"__sirannon_blob":"00"}', new Uint8Array([0])],
      [9, null, null, null],
    ]
    for (const row of edges) {
      await db.execute('INSERT INTO edges VALUES (?, ?, ?, ?)', row)
      await matchRawFile(workspace.path('edges'), cases)
    }
    await db.execute('UPDATE edges SET v = w, w = v WHERE id IN (1, 2, 3, 6)')
    await db.execute('UPDATE edges SET x = NULL WHERE id = 3')
    await matchRawFile(workspace.path('edges'), cases)
    await settleToQueries(db, cases, 150)
  })
})

describe('live queries over a table whose schema changes', () => {
  it('follow each change once a write reaches them, and report the error that the same SELECT raises', async () => {
    const db = await workspace.open('schema-changes')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.execute("INSERT INTO notes VALUES (1, 'alpha'), (2, 'beta')")
    const statements = [
      'SELECT * FROM notes ORDER BY id',
      'SELECT id, body FROM notes WHERE body LIKE ? ORDER BY body, id',
    ]
    const params = [[], ['%a%']]
    const queries: LiveQuery<unknown>[] = []
    for (const [index, sql] of statements.entries()) {
      const query = (await db.live(sql, params[index], { rereadJitterMs: 0 })) as LiveQuery<unknown>
      opened.push(query)
      queries.push(query)
    }

    const steps: string[][] = [
      ['ALTER TABLE notes ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0', "INSERT INTO notes VALUES (3, 'gamma', 1)"],
      ['CREATE INDEX notes_by_body ON notes (body)', "UPDATE notes SET body = 'delta' WHERE id = 1"],
      ['ALTER TABLE notes RENAME COLUMN body TO text', "INSERT INTO notes VALUES (4, 'epsilon', 0)"],
      ["UPDATE notes SET text = 'zeta' WHERE id = 2"],
      ['ALTER TABLE notes RENAME COLUMN text TO body', "INSERT INTO notes VALUES (5, 'eta', 1)"],
      [
        'ALTER TABLE notes ADD COLUMN score REAL',
        'UPDATE notes SET score = id * 1.5',
        'DELETE FROM notes WHERE id = 3',
      ],
    ]
    for (const step of steps) {
      await db.transaction(async tx => {
        for (const sql of step) await tx.execute(sql)
      })
      for (const [index, sql] of statements.entries()) {
        const oracle = await db.query(sql, params[index]).then(
          rows => canonicalRows(rows),
          (error: Error) => `error: ${error.message}`,
        )
        await waitFor(
          () => JSON.stringify(rowsOrStatus(queries[index])) === JSON.stringify(oracle),
          `${sql} to match ${JSON.stringify(oracle)} after ${step.join('; ')}, holding ${JSON.stringify(rowsOrStatus(queries[index]))}`,
        )
      }
    }
  })
})

describe('a live query across restarts and other writers', () => {
  it('picks up writes from another connection while open, and matches the file when opened again after a restart', async () => {
    const path = workspace.path('restarts')
    const first = workspace.registry()
    const db = await first.open('restarts', path, { cdcPollInterval: FAST_POLL_MS })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)')
    await db.execute("INSERT INTO orders VALUES (1, 'open'), (2, 'paid')")
    const sql = "SELECT id, status FROM orders WHERE status != 'void' ORDER BY id"
    const cases = await openCases(db, [[sql, []]])

    const outsider = new BetterSqlite3(path)
    outsider.pragma('busy_timeout = 5000')
    outsider.prepare("INSERT INTO orders VALUES (3, 'open')").run()
    outsider.prepare("UPDATE orders SET status = 'void' WHERE id = 1").run()
    await matchRawFile(path, cases, 100)

    await first.shutdown()
    outsider.prepare("INSERT INTO orders VALUES (4, 'open')").run()
    outsider.prepare('DELETE FROM orders WHERE id = 2').run()
    outsider.close()

    const second = workspace.registry()
    const reopened = await second.open('restarts', path, { cdcPollInterval: FAST_POLL_MS })
    const again = await openCases(reopened, [[sql, []]])
    expect(canonicalRows(await reopened.query(sql))).toEqual(canonicalRows(readRaw(path, sql)))
    await matchRawFile(path, again)
    await reopened.execute("INSERT INTO orders VALUES (5, 'paid')")
    await reopened.execute("UPDATE orders SET status = 'void' WHERE id = 3")
    await matchRawFile(path, again, 100)
  })
})
