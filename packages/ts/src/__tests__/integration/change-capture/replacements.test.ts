import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { ChangeEvent } from '../../../core/types.js'
import { settlesToQuery } from '../live-queries/harness.js'
import {
  type Collected,
  collect,
  createWorkspace,
  PLACEMENTS,
  readRaw,
  type Workspace,
  waitFor,
  waitForCount,
} from './harness.js'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('replacements')
})

afterEach(async () => {
  await workspace.dispose()
})

type Row = Record<string, unknown>

function mirrorOf(events: readonly ChangeEvent[], keyOf?: (row: Row) => string): Map<string, Row> {
  const mirror = new Map<string, Row>()
  for (const event of events) {
    const rowId = rowIdOf(event)
    if (event.type === 'delete') {
      expect(mirror.has(rowId), `a delete of row ${rowId}, which the mirror never held`).toBe(true)
      mirror.delete(rowId)
      continue
    }
    if (event.type === 'insert') {
      expect(mirror.has(rowId), `an insert of row ${rowId}, which the mirror already held`).toBe(false)
    }
    if (event.type === 'update' && keyOf !== undefined && event.oldRow !== undefined) mirror.delete(keyOf(event.oldRow))
    mirror.set(rowId, { ...event.row })
  }
  return mirror
}

function storedRows(path: string, sql: string, keyColumn: string, hiddenKey = false): Map<string, Row> {
  const rows = new Map<string, Row>()
  for (const row of readRaw<Row>(path, sql)) {
    const plain: Row = {}
    for (const [column, value] of Object.entries(row)) plain[column] = typeof value === 'bigint' ? Number(value) : value
    const key = String(plain[keyColumn])
    if (hiddenKey) delete plain[keyColumn]
    rows.set(key, plain)
  }
  return rows
}

function rowIdOf(event: ChangeEvent): string {
  if (event.rowId === undefined) throw new Error(`a ${event.type} event with no row id`)
  return event.rowId
}

function summary(events: readonly ChangeEvent[]): Array<[string, string, Row]> {
  return events.map(event => [event.type, rowIdOf(event), event.type === 'delete' ? (event.oldRow ?? {}) : event.row])
}

async function settle(seen: Collected, count: number): Promise<void> {
  await waitForCount(seen, count)
  expect(seen.errors).toEqual([])
}

describe.each(PLACEMENTS)('rows that a REPLACE deletes, with $label', placement => {
  it('reach every subscriber as deletes ahead of the row that replaces them, and nothing else changes the stream', async () => {
    const path = workspace.path('items')
    const db = await workspace.open('items', { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT UNIQUE COLLATE NOCASE, name TEXT)')
    await db.watch('items')
    const seen = collect(db.on('items'))
    const second = collect(db.on('ITEMS'))

    await db.execute("INSERT INTO items VALUES (1, 'a', 'first'), (2, 'b', 'second'), (3, 'c', 'third')")
    await db.execute("INSERT OR REPLACE INTO items VALUES (1, 'a', 'first again')")
    await db.execute("REPLACE INTO items VALUES (4, 'B', 'replaces second by its sku')")
    await db.execute("INSERT OR REPLACE INTO items VALUES (3, 'A', 'collides on both keys')")
    await db.execute("INSERT OR IGNORE INTO items VALUES (3, 'zzz', 'ignored')")
    await db.execute("INSERT INTO items VALUES (4, 'x', 'upserted') ON CONFLICT(id) DO UPDATE SET name = excluded.name")
    await expect(db.execute("INSERT INTO items VALUES (9, 'b', 'refused')")).rejects.toMatchObject({
      code: 'QUERY_ERROR',
    })
    await db.execute("UPDATE OR REPLACE items SET sku = 'a' WHERE id = 4")
    await db.transaction(async tx => {
      await tx.execute("INSERT OR REPLACE INTO items VALUES (4, 'q', 'one of two'), (5, 'Q', 'two of two')")
      await tx.execute("INSERT INTO items VALUES (6, 'r', 'moves')")
      await tx.execute('UPDATE OR REPLACE items SET id = 5 WHERE id = 6')
    })
    await settle(seen, 20)

    expect(summary(seen.events)).toEqual([
      ['insert', '1', { id: 1, sku: 'a', name: 'first' }],
      ['insert', '2', { id: 2, sku: 'b', name: 'second' }],
      ['insert', '3', { id: 3, sku: 'c', name: 'third' }],
      ['delete', '1', { id: 1, sku: 'a', name: 'first' }],
      ['insert', '1', { id: 1, sku: 'a', name: 'first again' }],
      ['delete', '2', { id: 2, sku: 'b', name: 'second' }],
      ['insert', '4', { id: 4, sku: 'B', name: 'replaces second by its sku' }],
      ['delete', '3', { id: 3, sku: 'c', name: 'third' }],
      ['delete', '1', { id: 1, sku: 'a', name: 'first again' }],
      ['insert', '3', { id: 3, sku: 'A', name: 'collides on both keys' }],
      ['update', '4', { id: 4, sku: 'B', name: 'upserted' }],
      ['delete', '3', { id: 3, sku: 'A', name: 'collides on both keys' }],
      ['update', '4', { id: 4, sku: 'a', name: 'upserted' }],
      ['delete', '4', { id: 4, sku: 'a', name: 'upserted' }],
      ['insert', '4', { id: 4, sku: 'q', name: 'one of two' }],
      ['delete', '4', { id: 4, sku: 'q', name: 'one of two' }],
      ['insert', '5', { id: 5, sku: 'Q', name: 'two of two' }],
      ['insert', '6', { id: 6, sku: 'r', name: 'moves' }],
      ['delete', '5', { id: 5, sku: 'Q', name: 'two of two' }],
      ['update', '5', { id: 5, sku: 'r', name: 'moves' }],
    ])
    expect(second.events).toEqual(seen.events)
    expect(mirrorOf(seen.events, row => String(row.id))).toEqual(storedRows(path, 'SELECT * FROM items', 'id'))
    expect(readRaw(path, 'SELECT COUNT(*) AS staged FROM _sirannon_replaced_rows')).toEqual([{ staged: 0n }])
  })
})

describe('rows that a REPLACE deletes from tables of every key shape', () => {
  it('leave a mirror built from the events equal to the table, with recursive triggers off and on', async () => {
    const path = workspace.path('shapes')
    const db = await workspace.open('shapes')
    await db.execute(
      'CREATE TABLE stock (shelf TEXT, slot INTEGER, label TEXT UNIQUE, PRIMARY KEY (shelf, slot)) WITHOUT ROWID',
    )
    await db.execute('CREATE TABLE tags (name TEXT NOT NULL, colour TEXT)')
    await db.execute('CREATE UNIQUE INDEX tags_by_name ON tags (name)')
    await db.execute('CREATE TABLE codes (code TEXT PRIMARY KEY ON CONFLICT REPLACE, label TEXT)')
    for (const table of ['stock', 'tags', 'codes']) await db.watch(table)
    const stock = collect(db.on('stock'))
    const tags = collect(db.on('tags'))
    const codes = collect(db.on('codes'))

    const steps = [
      "INSERT INTO stock VALUES ('a', 1, 'one'), ('a', 2, 'two'), ('b', 1, 'three')",
      "INSERT INTO tags VALUES ('red', 'r'), ('blue', 'b')",
      "INSERT INTO codes VALUES ('x', 'first'), ('y', 'second')",
      "REPLACE INTO stock VALUES ('a', 1, 'two')",
      "INSERT OR REPLACE INTO tags (rowid, name, colour) VALUES (2, 'red', 'again')",
      "INSERT INTO codes VALUES ('x', 'replaced by the table rule')",
      "UPDATE OR REPLACE stock SET shelf = 'a' WHERE shelf = 'b'",
      'PRAGMA recursive_triggers = ON',
      "REPLACE INTO stock VALUES ('a', 1, 'with recursive triggers')",
      "INSERT OR REPLACE INTO tags VALUES ('red', 'recursive')",
      "INSERT INTO codes VALUES ('y', 'recursive')",
      'PRAGMA recursive_triggers = OFF',
      "INSERT INTO tags VALUES ('blue', 'back')",
      "UPDATE OR REPLACE tags SET name = 'red' WHERE name = 'blue'",
      "INSERT INTO tags VALUES ('green', 'g')",
    ]
    for (const sql of steps) await db.execute(sql)
    await waitFor(() => tags.events.some(event => event.row.name === 'green'), 'the last change')
    await settle(tags, tags.events.length)

    const stockKey = (row: Row) => `${row.shelf}-${row.slot}`
    expect(mirrorOf(stock.events, stockKey)).toEqual(
      storedRows(path, "SELECT shelf || '-' || slot AS k, * FROM stock", 'k', true),
    )
    expect(mirrorOf(tags.events)).toEqual(storedRows(path, 'SELECT rowid AS k, * FROM tags', 'k', true))
    expect(mirrorOf(codes.events)).toEqual(storedRows(path, 'SELECT * FROM codes', 'code'))
    expect(stock.events.filter(event => event.type === 'delete')).toHaveLength(4)
    expect(tags.events.filter(event => event.type === 'delete')).toHaveLength(4)
    expect(summary(codes.events)).toEqual([
      ['insert', 'x', { code: 'x', label: 'first' }],
      ['insert', 'y', { code: 'y', label: 'second' }],
      ['delete', 'x', { code: 'x', label: 'first' }],
      ['insert', 'x', { code: 'x', label: 'replaced by the table rule' }],
      ['delete', 'y', { code: 'y', label: 'second' }],
      ['insert', 'y', { code: 'y', label: 'recursive' }],
    ])
  })

  it('reach the stream when an update moves the rowid under any of its names or changes a generated key through its sources', async () => {
    const path = workspace.path('moves')
    const db = await workspace.open('moves')
    await db.execute(
      "CREATE TABLE bins (shelf TEXT, code TEXT, folded TEXT AS (lower(code)), label TEXT AS (folded || '@' || shelf) STORED, note TEXT)",
    )
    await db.execute('CREATE UNIQUE INDEX bins_by_label ON bins (label)')
    await db.watch('bins')
    const bins = collect(db.on('bins'))

    const steps = [
      "INSERT INTO bins (rowid, shelf, code, note) VALUES (1, 'a', 'P', 'one'), (2, 'a', 'q', 'two'), (3, 'b', 'r', 'three'), (4, 'b', 's', 'four'), (5, 'c', 't', 'five'), (6, 'a', 'u', 'six'), (7, 'a', 't', 'seven')",
      'UPDATE OR REPLACE bins SET rowid = 2 WHERE rowid = 1',
      'UPDATE OR REPLACE bins SET OID = 3 WHERE rowid = 2',
      'UPDATE OR REPLACE bins SET _ROWID_ = 4 WHERE rowid = 3',
      "UPDATE OR REPLACE bins SET code = 'U' WHERE rowid = 4",
      "UPDATE OR REPLACE bins SET shelf = 'c' WHERE rowid = 7",
      "UPDATE OR REPLACE bins SET note = 'kept its key' WHERE rowid = 7",
    ]
    for (const sql of steps) await db.execute(sql)
    await waitFor(() => bins.events.some(event => event.row.note === 'kept its key'), 'the last change')
    await settle(bins, bins.events.length)

    const replacedThenMoved = ['2', '1', '3', '2', '4', '3']
    const replacedByGeneratedKey = ['6', '5']
    expect(mirrorOf(bins.events)).toEqual(storedRows(path, 'SELECT rowid AS k, shelf, code, note FROM bins', 'k', true))
    expect(bins.events.filter(event => event.type === 'delete').map(rowIdOf)).toEqual([
      ...replacedThenMoved,
      ...replacedByGeneratedKey,
    ])
  })

  it('keep a live query over the table equal to a fresh read', async () => {
    const db: Database = await workspace.open('live')
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, name TEXT)')
    await db.execute("INSERT INTO items VALUES (1, 'a', 'first'), (2, 'b', 'second'), (3, 'c', 'third')")
    const sql = 'SELECT id, sku, name FROM items ORDER BY sku'
    const query = await db.live(sql, undefined, { rereadJitterMs: 0 })
    try {
      for (const step of [
        "REPLACE INTO items VALUES (4, 'b', 'took the sku of the second')",
        "INSERT OR REPLACE INTO items VALUES (1, 'c', 'took two rows')",
        "UPDATE OR REPLACE items SET sku = 'b' WHERE id = 1",
      ]) {
        await db.execute(step)
        await settlesToQuery(query, db, sql)
      }
    } finally {
      await query.close()
    }
  })
})
