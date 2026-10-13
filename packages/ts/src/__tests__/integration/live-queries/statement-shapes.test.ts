import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { LiveQuery } from '../../../core/live/types.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { Params } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { createWorkspace, readRaw, settleToQueries, type Workspace } from './harness.js'

const ITEMS = `CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  qty INTEGER NOT NULL,
  bucket TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT '2026-01-01',
  randomised INTEGER NOT NULL DEFAULT 0,
  note TEXT NOT NULL DEFAULT ''
)`

let workspace: Workspace
const opened: LiveQuery<unknown>[] = []

beforeEach(() => {
  workspace = createWorkspace('statement-shapes')
})

afterEach(async () => {
  for (const query of opened.splice(0)) await query.close().catch(() => {})
  await workspace.dispose()
})

async function seedItems(db: Database): Promise<Database> {
  await db.execute(ITEMS)
  await db.execute(
    "INSERT INTO items (id, name, qty, bucket, created_at, note) VALUES (1, 'nut', 4, 'a', '2026-02-03', 'refresh now please'), (2, 'bolt', 9, 'b', '2026-03-04', '')",
  )
  return db
}

async function openItems(name: string): Promise<Database> {
  return seedItems(await workspace.open(name))
}

describe('a statement that a live query cannot keep current', () => {
  it('is refused with CDC_ERROR before anything is watched, and the database stays usable', async () => {
    const db = await openItems('refused')
    const refused: [string, Params?][] = [
      ['SELECT COUNT(*) AS total FROM items'],
      ['SELECT sum(qty) FROM items'],
      ['SELECT bucket, max(qty) FROM items GROUP BY bucket'],
      ['SELECT bucket FROM items GROUP BY bucket HAVING bucket > 1'],
      ['SELECT DISTINCT bucket FROM items'],
      ['SELECT id FROM items UNION SELECT id FROM items'],
      ['SELECT id FROM items INTERSECT SELECT id FROM items'],
      ['SELECT id FROM items EXCEPT SELECT id FROM items'],
      ['SELECT a.id FROM items a JOIN items b ON a.id = b.id'],
      ['SELECT id FROM items NATURAL JOIN items'],
      ['SELECT a.id FROM items a LEFT OUTER JOIN items b ON a.id = b.id'],
      ['SELECT a.id FROM items a, items b'],
      ['SELECT id FROM (SELECT id FROM items)'],
      ['SELECT id FROM items WHERE id IN (SELECT id FROM items)'],
      ['SELECT (SELECT 1) AS one, id FROM items'],
      ['WITH x AS (SELECT 1) SELECT id FROM items'],
      ['VALUES (1), (2)'],
      ['SELECT row_number() OVER (ORDER BY id) AS n FROM items'],
      ['SELECT id FROM items WINDOW w AS (ORDER BY id)'],
      ['SELECT id FROM items LIMIT 5'],
      ['SELECT id FROM items ORDER BY id LIMIT id + 1'],
      ['SELECT id FROM items ORDER BY id LIMIT -1'],
      ['SELECT id FROM items ORDER BY id LIMIT 2.5'],
      ['SELECT id FROM items ORDER BY id LIMIT ?', ['2']],
      ['SELECT id FROM items ORDER BY id LIMIT ?', [-1]],
      ['SELECT id FROM items ORDER BY id LIMIT ?', [1.5]],
      ['SELECT id FROM items ORDER BY id LIMIT ?'],
      ['SELECT id FROM items ORDER BY id + ?', [1]],
      ['SELECT id FROM items; DELETE FROM items'],
      ['UPDATE items SET qty = 1'],
      ['DELETE FROM items'],
      ['PRAGMA table_info(items)'],
      [''],
      ['   '],
      ['-- only a comment'],
      ['SELECT 1'],
      ['SELECT id FROM main.items'],
      ['SELECT id FROM items ORDER BY id NULLS MIDDLE'],
      ['SELECT name FROM items ORDER BY name COLLATE unicode'],
      ['SELECT * FROM items ORDER BY 2'],
      ['SELECT id FROM items ORDER BY 3'],
      ['SELECT id FROM items WHERE created_at > unixepoch() - 3600'],
      ["SELECT id FROM items WHERE created_at > datetime('now', '-1 hour')"],
      ['SELECT id FROM items WHERE created_at < CURRENT_TIMESTAMP'],
      ['SELECT id FROM items WHERE created_at < current_date'],
      ['SELECT id FROM items ORDER BY random()'],
      ['SELECT randomblob(4) AS token, id FROM items'],
      ["SELECT id FROM items WHERE created_at = strftime('%Y-%m-%d')"],
      ["SELECT id FROM items WHERE created_at > datetime(created_at, 'localtime')"],
      ['SELECT changes() AS n, id FROM items'],
      ['SELECT last_insert_rowid() AS n, id FROM items'],
      ['SELECT total_changes() AS n, id FROM items'],
    ]

    for (const [sql, params] of refused) {
      await expect(db.live(sql, params), sql).rejects.toMatchObject({ code: 'CDC_ERROR' })
    }
    await expect(db.live(operationRef('openOrders') as never)).rejects.toMatchObject({ code: 'CDC_ERROR' })

    expect(readRaw(workspace.path('refused'), "SELECT name FROM sqlite_master WHERE type = 'trigger'")).toEqual([])
    const query = await db.live('SELECT id FROM items ORDER BY id')
    opened.push(query as LiveQuery<unknown>)
    await db.execute("INSERT INTO items (id, name, qty, bucket) VALUES (3, 'gear', 1, 'a')")
    await settleToQueries(db, [{ query: query as LiveQuery<unknown>, sql: 'SELECT id FROM items ORDER BY id' }])
  })

  it('is refused with FORBIDDEN_SQL when it reaches a table that Sirannon reserves', async () => {
    const db = await openItems('reserved')
    for (const sql of [
      'SELECT * FROM _sirannon_changes',
      'SELECT * FROM "_SIRANNON_changes"',
      'SELECT id FROM items WHERE id IN (SELECT seq FROM _sirannon_changes)',
      'SELECT name FROM sqlite_master',
      'SELECT name FROM sqlite_schema ORDER BY name',
    ]) {
      await expect(db.live(sql), sql).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }
  })

  it('fails with the error SQLite gives when the statement names what the table lacks or does not parse', async () => {
    const db = await openItems('uncompilable')
    for (const sql of [
      'SELECT nope FROM items ORDER BY id',
      "SELECT id FROM items WHERE name = 'unterminated",
      'SELECT id FROM items WHERE qty > ORDER BY id',
      'SELECT id FROM items ORDER BY unknown_function(id)',
    ]) {
      const oracle = await db.query(sql).then(
        () => null,
        (error: Error) => error,
      )
      const failure = await db.live(sql).then(
        () => null,
        (error: Error) => error,
      )
      expect(oracle, sql).not.toBeNull()
      expect(failure, sql).toMatchObject({ code: 'QUERY_ERROR' })
    }
    await expect(db.live('SELECT id FROM missing ORDER BY id')).rejects.toThrow(/missing/)
    await db.execute('CREATE VIEW cheap AS SELECT id FROM items WHERE qty < 5')
    await expect(db.live('SELECT id FROM cheap ORDER BY id')).rejects.toThrow(/cheap/)
  })

  it('is refused with READ_ONLY on a read-only database', async () => {
    const writer = await openItems('read-only')
    await writer.close()
    const reader = await workspace.open('read-only', { readOnly: true })

    await expect(reader.live('SELECT id FROM items ORDER BY id')).rejects.toMatchObject({ code: 'READ_ONLY' })
    expect(await reader.query('SELECT id FROM items ORDER BY id')).toEqual([{ id: 1 }, { id: 2 }])
  })
})

const DRIVERS: { label: string; driver: () => SQLiteDriver; bindsNumberedParameters: boolean }[] = [
  { label: 'better-sqlite3', driver: () => betterSqlite3(), bindsNumberedParameters: false },
  { label: 'node:sqlite', driver: () => nodeSqlite(), bindsNumberedParameters: true },
]

describe.each(DRIVERS)('a statement that a live query keeps current on $label', ({
  label,
  driver,
  bindsNumberedParameters,
}) => {
  it('matches db.query after changes, whatever expression, window spelling, quoting, or parameter style it uses', async () => {
    const db = await workspace.open(`accepted-${label.replace(':', '-')}`, undefined, { driver: driver() })
    await seedItems(db)
    await db.execute('CREATE TABLE "group" ("order" INTEGER PRIMARY KEY, "select" TEXT, [where] TEXT, `from` INTEGER)')
    await db.execute(`INSERT INTO "group" VALUES (1, 'a', 'w', 3), (2, 'b', 'x', 1)`)
    const accepted: [string, Params?][] = [
      ['SELECT id FROM items ORDER BY id LIMIT 1, 2'],
      ['SELECT "order", "select", [where], `from` FROM "group" ORDER BY `from` DESC'],
      ['SELECT id, date(created_at) AS day FROM items ORDER BY day, id'],
      ["SELECT id FROM items WHERE created_at = strftime('%Y-%m-%d', created_at) ORDER BY id"],
      ['SELECT id, julianday(created_at) AS jd FROM items ORDER BY jd DESC'],
      ['SELECT id, randomised FROM items ORDER BY randomised, id'],
      ["SELECT id FROM items WHERE note = 'refresh now please' ORDER BY id"],
      ["SELECT id, CASE WHEN qty > 5 THEN 'many' ELSE 'few' END AS amount FROM items ORDER BY amount, id"],
      ["SELECT id FROM items WHERE bucket IN ('a', 'c') AND name LIKE '%u%' ORDER BY id"],
      ["SELECT id, json_object('n', name) AS doc FROM items ORDER BY id"],
      [
        'SELECT id, upper(name) AS shout FROM items WHERE qty BETWEEN :low AND :high ORDER BY shout',
        { low: 1, high: 50 },
      ],
      ['SELECT id, ? AS tag FROM items WHERE qty > ? AND bucket != ? ORDER BY id LIMIT ?', ['t', 0, 'z', 10]],
      ['SELECT * FROM items ORDER BY qty DESC, id'],
      ['SELECT id FROM items WHERE qty > 0;'],
      ...(bindsNumberedParameters
        ? ([
            ['SELECT id FROM items WHERE bucket = ?1 OR name = ?1 ORDER BY id LIMIT ?2', ['a', 10]],
            ['SELECT id, ?3 AS tag FROM items WHERE qty > ?2 ORDER BY id', [99, 0, 'third']],
          ] as [string, Params][])
        : []),
    ]
    const cases = []
    for (const [sql, params] of accepted) {
      const query = await db.live(sql, params, { rereadJitterMs: 0 })
      opened.push(query as LiveQuery<unknown>)
      cases.push({ query: query as LiveQuery<unknown>, sql, params })
    }

    await db.execute(
      "INSERT INTO items (id, name, qty, bucket, created_at, randomised, note) VALUES (3, 'gear', 20, 'a', '2025-12-31', 1, 'refresh now please')",
    )
    await db.execute("UPDATE items SET qty = 2, name = 'shunt' WHERE id = 2")
    await db.execute(`UPDATE "group" SET "from" = 9 WHERE "order" = 2`)
    await db.execute('DELETE FROM items WHERE id = 1')
    await settleToQueries(db, cases)
  })
})
