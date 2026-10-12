import { existsSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { columnRaw, createWorkspace, readRaw, type Workspace, WRITER_PLACEMENTS } from './harness.js'

type EntryPoint = [label: string, call: (db: Database, sql: string) => Promise<unknown>]

const ENTRY_POINTS: EntryPoint[] = [
  ['query', (db, sql) => db.query(sql)],
  ['queryOne', (db, sql) => db.queryOne(sql)],
  ['execute', (db, sql) => db.execute(sql)],
  ['executeBatch', (db, sql) => db.executeBatch(sql, [[]])],
  ['executeTransaction', (db, sql) => db.executeTransaction([{ sql: 'DELETE FROM notes WHERE id < 0' }, { sql }])],
  ['transaction execute', (db, sql) => db.transaction(tx => tx.execute(sql))],
  ['transaction query', (db, sql) => db.transaction(tx => tx.query(sql))],
  ['bulkLoad', (db, sql) => db.bulkLoad(sql, [[]])],
]

const INTERNAL_TABLE_STATEMENTS = [
  'SELECT * FROM _sirannon_changes',
  'select * from _SIRANNON_CHANGES',
  'SELECT * FROM "_sirannon_changes"',
  'SELECT * FROM [_sirannon_changes]',
  'SELECT * FROM `_sirannon_changes`',
  'SELECT * FROM main._sirannon_changes',
  'SELECT * FROM "main"."_sirannon_meta"',
  'DELETE FROM _sirannon_changes',
  'UPDATE orders SET item = (SELECT max(seq) FROM _sirannon_changes)',
  'CREATE VIEW change_log AS SELECT * FROM _sirannon_changes',
  'CREATE TABLE _sirannon_shadow (id INTEGER PRIMARY KEY)',
  'ALTER TABLE orders RENAME TO _sirannon_orders',
  'WITH log AS (SELECT * FROM _sirannon_changes) SELECT count(*) FROM log',
  'SELECT 1; DROP TABLE _sirannon_changes',
  'SELECT 1 /* harmless */ ; DELETE FROM "_sirannon_meta"',
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('guard-rails')
})

afterEach(async () => {
  await workspace.dispose()
})

function schemaSnapshot(path: string): unknown {
  return {
    schema: readRaw(path, 'SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name'),
    sequences: readRaw(path, 'SELECT name, seq FROM sqlite_sequence ORDER BY name'),
    statistics: readRaw(path, 'SELECT tbl, idx, stat FROM sqlite_stat1 ORDER BY tbl, idx'),
    newestChange: readRaw(path, 'SELECT max(seq) AS seq FROM _sirannon_changes'),
    orders: readRaw(path, 'SELECT id, item FROM orders ORDER BY id'),
  }
}

describe.each(WRITER_PLACEMENTS)('the statement guard with $label', ({ writerWorker }) => {
  async function openShop(): Promise<{ db: Database; path: string }> {
    const path = workspace.path('shop.db')
    const db = await workspace.registry().open('shop', path, { writerWorker })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, item TEXT NOT NULL)')
    await db.execute('CREATE INDEX orders_by_item ON orders (item)')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.executeBatch('INSERT INTO orders (item) VALUES (?)', [['kettle'], ['teapot'], ['kettle']])
    await db.watch('orders')
    await db.execute('INSERT INTO orders (item) VALUES (?)', ['strainer'])
    await db.execute('ANALYZE')
    return { db, path }
  }

  it('refuses every statement that reaches an internal table, through every entry point, and leaves the file as it was', async () => {
    const { db, path } = await openShop()
    const before = schemaSnapshot(path)

    for (const sql of INTERNAL_TABLE_STATEMENTS) {
      for (const [label, call] of ENTRY_POINTS) {
        await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
      }
    }
    const triggers = columnRaw(path, "SELECT name FROM sqlite_master WHERE type = 'trigger'")
    expect(triggers.length).toBeGreaterThan(0)
    for (const trigger of triggers) {
      await expect(db.execute(`DROP TRIGGER "${String(trigger)}"`)).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }

    expect(schemaSnapshot(path)).toEqual(before)
  })

  it('refuses a statement that modifies the sqlite_ catalogue, ATTACH, DETACH, and PRAGMA writable_schema in every spelling', async () => {
    const { db, path } = await openShop()
    const attached = workspace.path('attached.db')
    const before = schemaSnapshot(path)
    const refused = [
      "INSERT INTO sqlite_sequence (name, seq) VALUES ('notes', 99)",
      'UPDATE sqlite_sequence SET seq = 0',
      'DELETE FROM main.sqlite_sequence',
      "REPLACE INTO sqlite_sequence (name, seq) VALUES ('orders', 1)",
      "INSERT OR REPLACE INTO sqlite_stat1 (tbl, idx, stat) VALUES ('orders', NULL, '1')",
      'DROP TABLE sqlite_stat1',
      'WITH doomed AS (SELECT 1) DELETE FROM sqlite_sequence',
      'WITH RECURSIVE n (x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 3) UPDATE sqlite_sequence SET seq = (SELECT max(x) FROM n)',
      'with listed as (select name from sqlite_sequence) delete from orders where item in (select name from listed)',
      'WITH stale AS (SELECT tbl FROM sqlite_stat1) DELETE FROM "sqlite_stat1" WHERE tbl IN (SELECT tbl FROM stale)',
      `ATTACH DATABASE '${attached}' AS other`,
      `attach '${attached}' as other`,
      `  /* leading comment */ ATTACH '${attached}' AS other`,
      `SELECT 1; ATTACH '${attached}' AS other`,
      'DETACH DATABASE other',
      'PRAGMA writable_schema = ON',
      'pragma writable_schema=1',
      'PRAGMA main.writable_schema = 1',
      'PRAGMA "writable_schema" = 1',
      'PRAGMA [writable_schema] = 1',
      'PRAGMA main.`writable_schema` = 1',
    ]

    for (const sql of refused) {
      await expect(db.execute(sql), sql).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
      await expect(db.query(sql), sql).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }

    expect(existsSync(attached)).toBe(false)
    expect(schemaSnapshot(path)).toEqual(before)
  })

  it('refuses a reserved statement grouped with other writes and commits those writes', async () => {
    const { db, path } = await openShop()

    const settled = await Promise.allSettled([
      db.execute('INSERT INTO notes (body) VALUES (?)', ['first']),
      db.execute('DELETE FROM _sirannon_changes'),
      db.executeTransaction([
        { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['inside-refused-list'] },
        { sql: 'UPDATE sqlite_sequence SET seq = 0' },
      ]),
      db.execute('INSERT INTO notes (body) VALUES (?)', ['last']),
    ])

    expect(settled.map(outcome => (outcome.status === 'rejected' ? outcome.reason.code : 'ok'))).toEqual([
      'ok',
      'FORBIDDEN_SQL',
      'FORBIDDEN_SQL',
      'ok',
    ])
    expect(columnRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual(['first', 'last'])
    expect(readRaw(path, "SELECT seq FROM sqlite_sequence WHERE name = 'orders'")).toEqual([{ seq: 4n }])
  })

  it('reads the sqlite_ catalogue and accepts reserved words inside strings, comments, and longer names', async () => {
    const { db, path } = await openShop()
    await db.execute('CREATE TABLE my_sqlite_backup (attach TEXT, writable_schema TEXT)')
    await db.execute('CREATE TABLE orders_sirannon_archive (id INTEGER PRIMARY KEY)')
    const bodies = [
      '_sirannon_changes',
      "it's sqlite_master and _sirannon_meta",
      'ATTACH DATABASE x AS y',
      'PRAGMA writable_schema = 1',
    ]

    for (const body of bodies) {
      await db.execute(`INSERT INTO notes (body) VALUES ('${body.replaceAll("'", "''")}') /* _sirannon_changes */`)
    }
    await db.execute('INSERT INTO notes (body) VALUES (?) -- DETACH DATABASE main\n', ['parameter'])
    await db.execute('INSERT INTO my_sqlite_backup (attach, writable_schema) VALUES (?, ?)', ['a', 'w'])
    await db.execute('INSERT INTO orders_sirannon_archive (id) VALUES (1)')

    expect(await db.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")).toEqual(
      readRaw(path, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"),
    )
    expect(await db.queryOne('SELECT count(*) AS n FROM "sqlite_schema"')).toEqual({
      n: Number(readRaw<{ n: bigint }>(path, 'SELECT count(*) AS n FROM sqlite_schema')[0]?.n),
    })
    expect(
      await db.queryOne('WITH listed AS (SELECT name FROM sqlite_sequence) SELECT count(*) AS n FROM listed'),
    ).toEqual({ n: Number(readRaw<{ n: bigint }>(path, 'SELECT count(*) AS n FROM sqlite_sequence')[0]?.n) })
    expect(await db.query('SELECT attach, writable_schema FROM my_sqlite_backup')).toEqual([
      { attach: 'a', writable_schema: 'w' },
    ])
    expect(await db.query('PRAGMA table_info(orders_sirannon_archive)')).toHaveLength(1)
    expect(columnRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual([...bodies, 'parameter'])
  })
})
