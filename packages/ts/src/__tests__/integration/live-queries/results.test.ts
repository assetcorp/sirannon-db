import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery } from '../../../core/live/types.js'
import type { Params } from '../../../core/types.js'
import {
  canonicalRows,
  createWorkspace,
  pause,
  QUIET_PERIOD_MS,
  type QueryCase,
  readyRows,
  replicate,
  settlesTo,
  settlesToQuery,
  settleToQueries,
  type Workspace,
} from './harness.js'

const ITEMS = `CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  qty INTEGER,
  bucket TEXT NOT NULL
)`

type ItemRow = [number, string, number | null, string]

let workspace: Workspace
const opened: LiveQuery<unknown>[] = []

beforeEach(() => {
  workspace = createWorkspace('results')
})

afterEach(async () => {
  for (const query of opened.splice(0)) await query.close().catch(() => {})
  await workspace.dispose()
})

async function openItems(name: string, rows: ItemRow[] = []): Promise<Database> {
  const db = await workspace.open(name)
  await db.execute(ITEMS)
  if (rows.length > 0) await db.executeTransaction(inserts(rows))
  return db
}

async function live<T = Record<string, unknown>>(db: Database, sql: string, params?: Params): Promise<LiveQuery<T>> {
  const query = await db.live<T>(sql, params, { rereadJitterMs: 0 })
  opened.push(query as LiveQuery<unknown>)
  return query
}

async function openCases(db: Database, specs: readonly [string, Params?][]): Promise<QueryCase[]> {
  const cases: QueryCase[] = []
  for (const [sql, params] of specs) cases.push({ query: await live(db, sql, params), sql, params })
  return cases
}

async function stepThrough(db: Database, cases: readonly QueryCase[], steps: readonly string[]): Promise<void> {
  for (const step of steps) {
    await db.execute(step)
    await settleToQueries(db, cases)
  }
}

function inserts(rows: readonly ItemRow[]): { sql: string; params: unknown[] }[] {
  return rows.map(params => ({ sql: 'INSERT INTO items (id, name, qty, bucket) VALUES (?, ?, ?, ?)', params }))
}

function numbered(count: number, firstId: number, label: string): ItemRow[] {
  return Array.from({ length: count }, (_, index) => [firstId + index, `${label}-${index}`, index, 'a'])
}

describe('the first read', () => {
  it('holds the rows that the statement matches in its order, and an empty answer as ready with no rows', async () => {
    const db = await openItems('first-read', [
      [1, 'kente', 4, 'a'],
      [2, 'shea', 9, 'b'],
      [3, 'gari', 1, 'a'],
      [4, 'cocoa', null, 'a'],
    ])
    const sql = 'SELECT id, name FROM items WHERE bucket = ? ORDER BY name DESC'

    const filtered = await live<{ id: number; name: string }>(db, sql, ['a'])
    const empty = await live(db, "SELECT id FROM items WHERE bucket = 'none' ORDER BY id")

    expect(canonicalRows(readyRows(filtered))).toEqual(canonicalRows(await db.query(sql, ['a'])))
    expect(readyRows(filtered).map(row => row.name)).toEqual(['kente', 'gari', 'cocoa'])
    expect(empty.getState()).toEqual({ status: 'ready', rows: [], revalidating: false })
  })
})

describe('changes after the first read', () => {
  it('places each inserted row where SQLite orders it, by one term or several, in either direction', async () => {
    const db = await openItems('placement', [
      [1, 'aa', 5, 'x'],
      [2, 'cc', 5, 'y'],
      [3, 'ee', null, 'x'],
    ])
    const cases = await openCases(db, [
      ['SELECT name FROM items ORDER BY name'],
      ['SELECT id, qty FROM items ORDER BY qty DESC, id'],
      ['SELECT id, bucket, qty FROM items ORDER BY bucket, qty DESC NULLS FIRST, id DESC'],
      ['SELECT id, qty FROM items ORDER BY qty NULLS LAST, name DESC'],
    ])
    const replicas = cases.map(entry => replicate(entry.query))

    const arrivals: ItemRow[] = [
      [4, 'bb', 5, 'x'],
      [5, 'a', null, 'y'],
      [6, 'zz', -1, 'x'],
      [7, 'dd', 5, 'y'],
      [8, 'cc', 100, 'x'],
    ]
    for (const arrival of arrivals) {
      await db.executeTransaction(inserts([arrival]))
      await settleToQueries(db, cases)
    }

    for (const [index, replica] of replicas.entries()) {
      expect(canonicalRows(replica.rows), cases[index].sql).toEqual(canonicalRows(readyRows(cases[index].query)))
      expect(replica.kinds, cases[index].sql).toEqual(arrivals.map(() => 'ops'))
    }
  })

  it('carries a row into and out of the filter, moves it when its sort value changes, and stays silent for rows it never matched', async () => {
    const db = await openItems('membership', [
      [1, 'nut', 4, 'a'],
      [2, 'bolt', 9, 'b'],
      [3, 'gear', 2, 'a'],
    ])
    const cases = await openCases(db, [['SELECT id, qty FROM items WHERE bucket = ? ORDER BY qty', ['a']]])
    const replica = replicate(cases[0].query)

    await stepThrough(db, cases, [
      "UPDATE items SET bucket = 'a' WHERE id = 2",
      'UPDATE items SET qty = 0 WHERE id = 2',
      "UPDATE items SET bucket = 'c' WHERE id = 1",
      'DELETE FROM items WHERE id = 3',
      "UPDATE items SET name = 'renamed' WHERE id = 2",
    ])
    const updatesSoFar = replica.kinds.length

    await db.execute("INSERT INTO items VALUES (10, 'washer', 1, 'b')")
    await db.execute("UPDATE items SET qty = 50 WHERE bucket = 'b'")
    await db.execute("DELETE FROM items WHERE bucket = 'c'")
    await pause(QUIET_PERIOD_MS)

    expect(replica.kinds.length).toBe(updatesSoFar)
    expect(canonicalRows(replica.rows)).toEqual(canonicalRows(await db.query(cases[0].sql, cases[0].params)))
  })

  it('follows a primary key change, a swap of two keys inside one transaction, and a composite key', async () => {
    const db = await openItems('keys', [
      [1, 'aa', 1, 'a'],
      [2, 'bb', 2, 'a'],
      [3, 'cc', 3, 'a'],
    ])
    await db.execute(
      'CREATE TABLE stock (shelf TEXT, slot INTEGER, label TEXT, PRIMARY KEY (shelf, slot)) WITHOUT ROWID',
    )
    await db.execute("INSERT INTO stock VALUES ('a', 1, 'one'), ('a', 2, 'two'), ('b', 1, 'three')")
    const items = await live(db, 'SELECT id, name FROM items ORDER BY id')
    const cases = await openCases(db, [['SELECT shelf, slot, label FROM stock ORDER BY label']])

    await db.execute('UPDATE items SET id = 9 WHERE id = 1')
    await db.transaction(async tx => {
      await tx.execute('UPDATE items SET id = 100 WHERE id = 2')
      await tx.execute('UPDATE items SET id = 2 WHERE id = 3')
      await tx.execute('UPDATE items SET id = 3 WHERE id = 100')
    })
    await settlesTo(
      items,
      [
        { id: 2, name: 'cc' },
        { id: 3, name: 'bb' },
        { id: 9, name: 'aa' },
      ],
      'items after the key swap',
    )
    expect(readyRows(items)).toEqual(await db.query('SELECT id, name FROM items ORDER BY id'))

    await stepThrough(db, cases, [
      "UPDATE stock SET shelf = 'c', label = 'moved' WHERE shelf = 'a' AND slot = 2",
      "DELETE FROM stock WHERE shelf = 'a' AND slot = 1",
      "INSERT INTO stock VALUES ('a', 1, 'again')",
    ])
  })

  it('keeps a table without a declared primary key current by its rowid, including after a rowid changes', async () => {
    const db = await workspace.open('rowid')
    await db.execute('CREATE TABLE notes (body TEXT NOT NULL, tag TEXT NOT NULL)')
    await db.execute("INSERT INTO notes VALUES ('one', 'x'), ('two', 'y'), ('two', 'x')")
    const cases = await openCases(db, [
      ['SELECT body, tag FROM notes WHERE tag = ? ORDER BY body', ['x']],
      ['SELECT body FROM notes ORDER BY body DESC LIMIT 2'],
    ])

    await stepThrough(db, cases, [
      "UPDATE notes SET tag = 'x' WHERE tag = 'y'",
      "INSERT INTO notes VALUES ('one', 'x')",
      'UPDATE notes SET rowid = 50 WHERE rowid = 1',
      'UPDATE notes SET rowid = 60 WHERE rowid = 2',
      'DELETE FROM notes WHERE rowid = 50',
      "UPDATE notes SET body = 'zero' WHERE rowid = 3",
    ])
  })

  it('keeps generated columns current in the select list, the filter, and the order', async () => {
    const db = await workspace.open('generated')
    await db.execute(`CREATE TABLE lines (
      id INTEGER PRIMARY KEY,
      price REAL NOT NULL,
      quantity INTEGER NOT NULL,
      total REAL GENERATED ALWAYS AS (price * quantity) VIRTUAL,
      label TEXT AS (upper(substr('item-' || id, 1, 12))) STORED
    )`)
    await db.execute('INSERT INTO lines (id, price, quantity) VALUES (1, 2.5, 4), (2, 10, 1)')
    const cases = await openCases(db, [
      ['SELECT * FROM lines ORDER BY id'],
      ['SELECT id, total FROM lines WHERE total >= ? ORDER BY total DESC, id', [10]],
      ['SELECT label, quantity FROM lines ORDER BY label LIMIT 2'],
    ])

    await stepThrough(db, cases, [
      'INSERT INTO lines (id, price, quantity) VALUES (3, 1, 50)',
      'UPDATE lines SET quantity = 1 WHERE id = 1',
      'UPDATE lines SET id = 0 WHERE id = 2',
      'DELETE FROM lines WHERE id = 3',
    ])
  })

  it('keeps a statement current when it names the rowid as rowid, oid, or _rowid_', async () => {
    const db = await workspace.open('rowid-in-statement')
    await db.execute('CREATE TABLE notes (body TEXT NOT NULL)')
    await db.execute('CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL)')
    await db.execute("INSERT INTO notes VALUES ('one'), ('two')")
    await db.execute("INSERT INTO tasks VALUES (10, 'first')")
    const cases = await openCases(db, [
      ['SELECT rowid AS r, body FROM notes ORDER BY body'],
      ['SELECT body FROM notes WHERE rowid > 1 ORDER BY oid'],
      ['SELECT body FROM notes WHERE "rowid" < 10 ORDER BY body'],
      ['SELECT _rowid_ AS r, title FROM tasks ORDER BY title'],
      ['SELECT title FROM tasks WHERE oid BETWEEN 5 AND 15 ORDER BY id'],
    ])

    await stepThrough(db, cases, [
      "INSERT INTO notes VALUES ('three')",
      "INSERT INTO tasks VALUES (20, 'second')",
      'UPDATE notes SET rowid = 40 WHERE rowid = 1',
      "UPDATE notes SET body = 'zero' WHERE rowid = 2",
      'UPDATE tasks SET id = 12 WHERE id = 20',
      'UPDATE tasks SET rowid = 30 WHERE id = 10',
      'DELETE FROM notes WHERE rowid = 3',
    ])
  })

  it('refuses with CDC_ERROR a statement that names the rowid of a table whose primary key is not its rowid', async () => {
    const db = await workspace.open('rowid-hidden-by-key')
    await db.execute('CREATE TABLE codes (code TEXT PRIMARY KEY, label TEXT)')
    await db.execute('CREATE TABLE counters (n INT PRIMARY KEY, label TEXT)')
    await db.execute('CREATE TABLE named (id INTEGER PRIMARY KEY, oid TEXT)')
    await db.execute("INSERT INTO named VALUES (1, 'kept')")

    for (const sql of ['SELECT rowid, label FROM codes', 'SELECT label FROM counters ORDER BY _rowid_']) {
      await expect(db.live(sql), sql).rejects.toMatchObject({ code: 'CDC_ERROR' })
    }
    const declared = await live(db, 'SELECT oid FROM named ORDER BY id')
    expect(readyRows(declared)).toEqual([{ oid: 'kept' }])
  })

  it('orders text by the collation that the column declares or the statement names', async () => {
    const db = await workspace.open('collations')
    await db.execute(
      `CREATE TABLE people (
        id INTEGER PRIMARY KEY,
        name TEXT COLLATE NOCASE,
        code TEXT COLLATE RTRIM,
        plain TEXT,
        tagline TEXT DEFAULT 'collate nocase' CHECK (tagline <> 'x' COLLATE NOCASE),
        UNIQUE (id, plain COLLATE NOCASE)
      )`,
    )
    await db.execute("INSERT INTO people VALUES (1, 'delta', 'b  ', 'b', 'b'), (2, 'Bravo', 'b', 'B', 'B')")
    const cases = await openCases(db, [
      ['SELECT name FROM people ORDER BY name, id'],
      ['SELECT code, id FROM people ORDER BY code, id'],
      ['SELECT plain, id FROM people ORDER BY plain COLLATE NOCASE, id'],
      ['SELECT name, id FROM people ORDER BY name COLLATE BINARY DESC, id'],
      ['SELECT tagline, id FROM people ORDER BY tagline, id'],
    ])

    const arrivals = [
      [3, 'charlie', 'a ', 'a_b'],
      [4, 'a_b', 'b ', 'aB'],
      [5, 'aB', 'B', '[x]'],
      [6, 'Z', 'a', 'z'],
      [7, '_lead', '', '`tick'],
      [8, 'É', 'é ', 'é'],
    ]
    for (const arrival of arrivals) {
      await db.execute('INSERT INTO people VALUES (?, ?, ?, ?, ?)', [...arrival, arrival[3]])
      await settleToQueries(db, cases)
    }
  })

  it('keeps working when the statement carries comments, quoted keywords, an alias, ordinals, named parameters, and a trailing semicolon', async () => {
    const db = await openItems('statement-text', [
      [1, 'aa', 1, 'a'],
      [2, 'bb', 2, 'b'],
    ])
    await db.execute('CREATE TABLE "order" ("group" TEXT, "select" INTEGER PRIMARY KEY, "limit" INTEGER)')
    await db.execute(`INSERT INTO "order" VALUES ('g', 1, 5)`)
    const cases = await openCases(db, [
      ["SELECT id -- projected\nFROM items WHERE bucket = 'a' -- filtered, ORDER BY name\nORDER BY id"],
      ['SELECT id, name /* JOIN items ON 1 */ FROM items /* GROUP BY */ ORDER BY id;'],
      ['SELECT "group", "select", "limit" FROM "order" ORDER BY "limit" DESC, "select"'],
      ['SELECT i.id, i.qty AS amount FROM items AS i WHERE i.qty >= :least ORDER BY amount DESC', { least: 1 }],
      ['SELECT name, id FROM items i ORDER BY 2 DESC'],
      ["SELECT id FROM items WHERE name = 'order by group' OR bucket = ? ORDER BY id", ['b']],
      ['SELECT id, qty FROM items ORDER BY qty * :direction, id', { direction: -1 }],
    ])

    await stepThrough(db, cases, [
      "INSERT INTO items VALUES (3, 'order by group', 7, 'a')",
      'UPDATE items SET qty = -2 WHERE id = 1',
      `INSERT INTO "order" VALUES ('h', 2, 9)`,
      "DELETE FROM items WHERE bucket = 'b'",
    ])
  })
})

describe('a window over the result', () => {
  it('refills a LIMIT window from the rows past it when a row inside it leaves or moves out', async () => {
    const db = await openItems('limit-refill', numbered(8, 1, 'row'))
    const cases = await openCases(db, [
      ['SELECT id FROM items ORDER BY id LIMIT 3'],
      ['SELECT id, qty FROM items ORDER BY qty DESC LIMIT ?', [2n]],
      ['SELECT id FROM items WHERE bucket = ? ORDER BY id LIMIT 0', ['a']],
      ['SELECT id FROM items ORDER BY id LIMIT 20'],
    ])

    await stepThrough(db, cases, [
      'DELETE FROM items WHERE id = 2',
      'UPDATE items SET qty = -5 WHERE id = 8',
      "UPDATE items SET bucket = 'b', id = 99 WHERE id = 1",
      'DELETE FROM items WHERE id IN (3, 4, 5)',
      'DELETE FROM items WHERE id > 6',
    ])
  })

  it('pushes the last row out of a LIMIT window when a row enters ahead of it, and stays silent for rows that sort after it', async () => {
    const db = await openItems('limit-push', [
      [10, 'a', 1, 'a'],
      [20, 'b', 2, 'a'],
      [30, 'c', 3, 'a'],
      [40, 'd', 4, 'a'],
    ])
    const cases = await openCases(db, [['SELECT id FROM items ORDER BY id LIMIT 3']])
    const replica = replicate(cases[0].query)

    await db.execute("INSERT INTO items VALUES (5, 'first', 0, 'a')")
    await settlesTo(cases[0].query, [{ id: 5 }, { id: 10 }, { id: 20 }], 'a row entering ahead')
    const updatesSoFar = replica.kinds.length

    await db.execute("INSERT INTO items VALUES (50, 'last', 9, 'a')")
    await db.execute('UPDATE items SET qty = 100 WHERE id = 40')
    await pause(QUIET_PERIOD_MS)

    expect(replica.kinds.length).toBe(updatesSoFar)
    await settleToQueries(db, cases)
    expect(canonicalRows(replica.rows)).toEqual(canonicalRows(readyRows(cases[0].query)))
  })

  it('moves an OFFSET window when rows before it come and go, in both spellings of the window', async () => {
    const db = await openItems('offset', numbered(10, 1, 'row'))
    const cases = await openCases(db, [
      ['SELECT id FROM items ORDER BY id LIMIT 3 OFFSET 3'],
      ['SELECT id FROM items ORDER BY id LIMIT 3, 2'],
      ['SELECT id FROM items ORDER BY id DESC LIMIT ? OFFSET ?', [2, 1]],
      ['SELECT id FROM items ORDER BY id LIMIT 5 OFFSET 40'],
    ])

    await stepThrough(db, cases, [
      'DELETE FROM items WHERE id = 1',
      "INSERT INTO items VALUES (0, 'zero', 0, 'a')",
      'UPDATE items SET id = 5000 WHERE id = 5',
      'UPDATE items SET qty = 77 WHERE id = 6',
      `INSERT INTO items (id, name, qty, bucket) SELECT value + 100, 'bulk', value, 'a' FROM json_each('${JSON.stringify(Array.from({ length: 35 }, (_, index) => index + 1))}')`,
      'DELETE FROM items WHERE id < 8',
    ])
  })
})

describe('a transaction', () => {
  it('reaches listeners as one update that holds every change it made', async () => {
    const db = await openItems('atomic', numbered(4, 1, 'row'))
    const cases = await openCases(db, [['SELECT id, qty FROM items ORDER BY qty']])
    const replica = replicate(cases[0].query)

    await db.transaction(async tx => {
      await tx.execute("INSERT INTO items VALUES (5, 'ee', -1, 'a')")
      await tx.execute('UPDATE items SET qty = 10 WHERE id = 1')
      await tx.execute('DELETE FROM items WHERE id = 2')
      await tx.execute("INSERT INTO items VALUES (6, 'ff', 6, 'a')")
    })
    await settleToQueries(db, cases)

    expect(replica.published).toEqual([5])
    expect(replica.kinds).toEqual(['ops'])
    expect(canonicalRows(replica.rows)).toEqual(canonicalRows(readyRows(cases[0].query)))
  })

  it('reaches listeners as one update when it spans more than one batch of the change log', async () => {
    const db = await openItems('long-transaction', numbered(1200, 1, 'row'))
    const sql = 'SELECT id FROM items ORDER BY id'
    const query = await live(db, sql)
    const replica = replicate(query)

    await db.executeTransaction(inserts(numbered(1100, 10_000, 'bulk')))
    await settlesToQuery(query, db, sql)

    expect(replica.published).toEqual([2300])
    expect(replica.kinds).toEqual(['ops'])
    expect(canonicalRows(replica.rows)).toEqual(canonicalRows(readyRows(query)))
  })
})
