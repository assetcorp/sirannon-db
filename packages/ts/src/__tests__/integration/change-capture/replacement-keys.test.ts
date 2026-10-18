import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { ChangeEvent } from '../../../core/types.js'
import { type Collected, collect, createWorkspace, readRaw, type Workspace, waitFor } from './harness.js'

type Row = Record<string, unknown>

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('replacement-keys')
})

afterEach(async () => {
  await workspace.dispose()
})

class Mirror {
  readonly rows = new Map<string, Row>()

  constructor(
    private readonly label: string,
    private readonly keyOf: (row: Row) => string,
  ) {}

  apply(event: ChangeEvent): void {
    const rowId = event.rowId
    if (rowId === undefined) throw new Error(`${this.label}: a ${event.type} event with no row id`)
    if (event.type === 'delete') {
      expect(this.rows.get(rowId), `${this.label}: the old image of deleted row ${rowId}`).toEqual(event.oldRow)
      this.rows.delete(rowId)
      return
    }
    if (event.type === 'insert') {
      expect(this.rows.has(rowId), `${this.label}: an insert of row ${rowId}, which the mirror already holds`).toBe(
        false,
      )
    } else if (event.oldRow !== undefined) {
      const oldKey = this.keyOf(event.oldRow)
      expect(this.rows.get(oldKey), `${this.label}: the old image of updated row ${oldKey}`).toEqual(event.oldRow)
      this.rows.delete(oldKey)
    }
    this.rows.set(rowId, { ...event.row })
  }
}

function storedRows(path: string, sql: string, keyOf: (row: Row) => string): Map<string, Row> {
  const rows = new Map<string, Row>()
  for (const row of readRaw<Row>(path, sql)) {
    const plain: Row = {}
    for (const [column, value] of Object.entries(row)) plain[column] = typeof value === 'bigint' ? Number(value) : value
    rows.set(keyOf(plain), plain)
  }
  return rows
}

function seededRandom(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296
  }
}

interface Watched {
  table: string
  read: string
  mirror: Mirror
  seen: Collected
  applied: number
}

async function settleAll(db: Database, marks: Collected, watched: Watched[], mark: number): Promise<void> {
  await db.execute('INSERT INTO marks VALUES (?)', [mark])
  await waitFor(() => marks.events.length >= mark, `mark ${mark}`)
  for (const entry of watched) {
    expect(entry.seen.errors).toEqual([])
    for (; entry.applied < entry.seen.events.length; entry.applied++)
      entry.mirror.apply(entry.seen.events[entry.applied])
  }
}

function expectMirrorsMatch(path: string, watched: Watched[], context: string): void {
  for (const entry of watched) {
    const keyOf = (row: Row) => String(row[entry.table === 'tags' ? 'name' : 'id'])
    expect(entry.mirror.rows, `${entry.table} after ${context}`).toEqual(storedRows(path, entry.read, keyOf))
  }
}

function watchedTable(db: Database, table: string, read: string, keyColumn: string): Watched {
  return {
    table,
    read,
    mirror: new Mirror(table, row => String(row[keyColumn])),
    seen: collect(db.on(table)),
    applied: 0,
  }
}

describe('rows that a REPLACE deletes through a unique index over an expression or a partial unique index', () => {
  it.each([
    20261012, 7, 41,
  ])('reach every subscriber as deletes, so that a mirror built from the events equals the table after every statement, with seed %i', async seed => {
    const path = workspace.path('expression-keys')
    const db = await workspace.open('expression-keys')
    await db.execute(
      'CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT, handle TEXT, active INTEGER NOT NULL, region TEXT)',
    )
    await db.execute('CREATE UNIQUE INDEX accounts_by_email ON accounts (lower(email))')
    await db.execute('CREATE UNIQUE INDEX accounts_live_handle ON accounts (handle) WHERE active = 1')
    await db.execute(
      'CREATE UNIQUE INDEX accounts_by_region ON accounts (/* trimmed */ trim(region) COLLATE NOCASE, length(handle) DESC) WHERE region IS NOT NULL AND length(handle) > 1',
    )
    await db.execute('CREATE TABLE tags (name TEXT PRIMARY KEY, label TEXT, weight INTEGER) WITHOUT ROWID')
    await db.execute('CREATE UNIQUE INDEX tags_by_label ON tags (upper(label), weight % 3)')
    await db.execute('CREATE TABLE marks (n INTEGER PRIMARY KEY)')
    for (const table of ['accounts', 'tags', 'marks']) await db.watch(table)
    const marks = collect(db.on('marks'))
    const watched = [
      watchedTable(db, 'accounts', 'SELECT * FROM accounts', 'id'),
      watchedTable(db, 'tags', 'SELECT * FROM tags', 'name'),
    ]

    const random = seededRandom(seed)
    const pick = <T>(values: readonly T[]): T => values[Math.floor(random() * values.length)]
    const emails = ['ama@x.gh', 'AMA@x.gh', 'kofi@x.gh', 'Kofi@X.gh', 'esi@x.gh', null]
    const handles = ['ab', 'abc', 'xy', 'xyz', 'a', null]
    const regions = ['west', ' WEST ', 'east', null]
    const account = () => [pick([1, 2, 3, 4, 5, 6, 7, null]), pick(emails), pick(handles), pick([0, 1]), pick(regions)]
    const tag = () => [pick(['red', 'blue', 'green', 'gold']), pick(['warm', 'WARM', 'cool', null]), pick([1, 2, 4, 5])]
    const statements: Array<() => [string, unknown[]]> = [
      () => ['INSERT OR REPLACE INTO accounts VALUES (?, ?, ?, ?, ?)', account()],
      () => ['REPLACE INTO accounts VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)', [...account(), ...account()]],
      () => ['INSERT OR IGNORE INTO accounts VALUES (?, ?, ?, ?, ?)', account()],
      () => ['INSERT INTO accounts VALUES (?, ?, ?, ?, ?)', account()],
      () => [
        'INSERT INTO accounts VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET email = excluded.email, active = excluded.active',
        account(),
      ],
      () => [
        'UPDATE OR REPLACE accounts SET handle = ?, active = ? WHERE id = ?',
        [pick(handles), pick([0, 1]), pick([1, 2, 3, 4, 5, 6, 7])],
      ],
      () => [
        'UPDATE OR REPLACE accounts SET email = ?, region = ? WHERE id = ?',
        [pick(emails), pick(regions), pick([1, 2, 3, 4, 5, 6, 7])],
      ],
      () => [
        'UPDATE OR REPLACE accounts SET id = ? WHERE id = ?',
        [pick([1, 2, 3, 4, 5, 6, 7, 8]), pick([1, 2, 3, 4, 5, 6, 7])],
      ],
      () => ['DELETE FROM accounts WHERE id = ?', [pick([1, 2, 3, 4, 5, 6, 7])]],
      () => ['INSERT OR REPLACE INTO tags VALUES (?, ?, ?)', tag()],
      () => ['INSERT OR IGNORE INTO tags VALUES (?, ?, ?)', tag()],
      () => [
        'UPDATE OR REPLACE tags SET label = ?, weight = ? WHERE name = ?',
        [pick(['warm', 'WARM', 'cool', null]), pick([1, 2, 4, 5]), pick(['red', 'blue', 'green', 'gold'])],
      ],
      () => [
        'UPDATE OR REPLACE tags SET name = ? WHERE name = ?',
        [pick(['red', 'blue', 'green', 'gold', 'pink']), pick(['red', 'blue', 'green', 'gold'])],
      ],
    ]

    let refused = 0
    for (let step = 1; step <= 150; step++) {
      const [sql, params] = pick(statements)()
      try {
        await db.execute(sql, params)
      } catch (error) {
        expect(error, sql).toMatchObject({ code: 'QUERY_ERROR' })
        refused++
      }
      await settleAll(db, marks, watched, step)
      expectMirrorsMatch(path, watched, `step ${step}: ${sql} with ${JSON.stringify(params)}`)
    }

    const deletes = watched.flatMap(entry => entry.seen.events).filter(event => event.type === 'delete')
    expect(deletes.length).toBeGreaterThan(10)
    expect(refused).toBeLessThan(150)
  })
})

describe('a REPLACE that a user trigger runs', () => {
  it('reaches subscribers of its own table and of another watched table as deletes ahead of the rows that replace them', async () => {
    const path = workspace.path('nested')
    const db = await workspace.open('nested')
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, code TEXT UNIQUE, note TEXT)')
    await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, code TEXT UNIQUE, total INTEGER)')
    await db.execute('CREATE TABLE stock (id INTEGER PRIMARY KEY, code TEXT UNIQUE, note TEXT)')
    await db.execute('CREATE TABLE marks (n INTEGER PRIMARY KEY)')
    await db.execute(
      'CREATE TRIGGER orders_to_ledger AFTER INSERT ON orders BEGIN INSERT OR REPLACE INTO ledger (id, code, total) VALUES (NEW.id, NEW.code, length(NEW.note)); END',
    )
    await db.execute(
      "CREATE TRIGGER orders_twin AFTER INSERT ON orders WHEN NEW.note LIKE 'twin%' BEGIN INSERT OR REPLACE INTO orders (id, code, note) VALUES (NEW.id + 100, NEW.code || '-twin', 'made by a trigger'); END",
    )
    for (const table of ['orders', 'ledger', 'stock', 'marks']) await db.watch(table)
    await db.execute(
      "CREATE TRIGGER stock_twin AFTER INSERT ON stock WHEN NEW.note LIKE 'twin%' BEGIN INSERT OR REPLACE INTO stock (id, code, note) VALUES (NEW.id + 100, NEW.code || '-twin', 'made by a trigger'); END",
    )
    await db.execute(
      "CREATE TRIGGER stock_first BEFORE INSERT ON stock WHEN NEW.note = 'first' BEGIN INSERT OR REPLACE INTO stock (id, code, note) VALUES (NEW.id + 200, NEW.code || '-first', 'made before'); END",
    )
    const marks = collect(db.on('marks'))
    const watched = [
      watchedTable(db, 'orders', 'SELECT * FROM orders', 'id'),
      watchedTable(db, 'ledger', 'SELECT * FROM ledger', 'id'),
      watchedTable(db, 'stock', 'SELECT * FROM stock', 'id'),
    ]

    const steps: Array<[string, unknown[]]> = [
      ["INSERT INTO orders VALUES (1, 'a', 'one'), (2, 'b', 'two'), (3, 'c', 'twin three')", []],
      ["INSERT OR REPLACE INTO orders VALUES (4, 'a', 'takes the code of the first')", []],
      ["INSERT OR REPLACE INTO orders VALUES (3, 'b', 'twin again, on two keys')", []],
      ["INSERT OR REPLACE INTO orders VALUES (5, 'c-twin', 'takes the code of a twin')", []],
      ["INSERT INTO stock VALUES (1, 'a', 'one'), (2, 'b', 'twin two'), (3, 'c', 'first')", []],
      ["INSERT OR REPLACE INTO stock VALUES (4, 'b-twin', 'twin replaces a twin')", []],
      ["INSERT OR REPLACE INTO stock VALUES (3, 'c', 'first')", []],
      ["INSERT OR REPLACE INTO stock VALUES (6, 'c-first', 'twin of what a trigger made')", []],
    ]
    let mark = 0
    for (const [sql, params] of steps) {
      await db.execute(sql, params)
      mark++
      await settleAll(db, marks, watched, mark)
      expectMirrorsMatch(path, watched, sql)
    }
    await db.transaction(async tx => {
      await tx.execute("INSERT OR REPLACE INTO orders VALUES (2, 'z', 'twin in a transaction')")
      await tx.execute("INSERT OR REPLACE INTO stock VALUES (2, 'z', 'twin in a transaction')")
    })
    await settleAll(db, marks, watched, mark + 1)
    expectMirrorsMatch(path, watched, 'the transaction')

    const deletes = (table: string) =>
      watched.find(entry => entry.table === table)?.seen.events.filter(event => event.type === 'delete').length
    expect(deletes('orders')).toBeGreaterThanOrEqual(4)
    expect(deletes('ledger')).toBeGreaterThanOrEqual(3)
    expect(deletes('stock')).toBeGreaterThanOrEqual(3)
  })
})
