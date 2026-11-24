import { existsSync } from 'node:fs'
import { DatabaseSync, constants as sqliteConstants } from 'node:sqlite'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import {
  columnRaw,
  createWorkspace,
  DRIVER_PLACEMENTS,
  STATEMENT_ENTRY_POINTS as ENTRY_POINTS,
  readRaw,
  type Workspace,
  WRITER_PLACEMENTS,
} from './harness.js'

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
  "SELECT * FROM '_sirannon_meta'",
  "select * from '_SIRANNON_CHANGES'",
  "SELECT * FROM main.'_sirannon_meta'",
  "SELECT * FROM 'main'.'_sirannon_meta'",
  "SELECT * FROM orders, '_sirannon_changes'",
  "SELECT * FROM orders LEFT JOIN '_sirannon_meta' AS m ON m.key = orders.item",
  "SELECT * FROM orders WHERE item IN '_sirannon_meta'",
  "SELECT (SELECT value FROM '_sirannon_meta') FROM orders",
  "WITH leaked AS (SELECT * FROM '_sirannon_meta') SELECT * FROM leaked",
  "INSERT INTO '_sirannon_meta' (key, value) VALUES ('a', 'b')",
  "REPLACE INTO '_sirannon_meta' VALUES ('a', 'b')",
  "INSERT INTO notes (body) SELECT value FROM '_sirannon_meta'",
  "UPDATE '_sirannon_meta' SET value = 'x'",
  "UPDATE OR IGNORE main.'_sirannon_meta' SET value = 'x'",
  "UPDATE notes SET body = m.value FROM orders, '_sirannon_meta' AS m",
  "DELETE FROM '_sirannon_changes'",
  "DROP TABLE IF EXISTS '_sirannon_meta'",
  "ALTER TABLE '_sirannon_meta' ADD COLUMN extra TEXT",
  "CREATE TABLE '_sirannon_shadow' (id INTEGER)",
  "CREATE VIEW leak AS SELECT * FROM '_sirannon_meta'",
  "CREATE INDEX meta_value ON '_sirannon_meta' (value)",
  "CREATE TRIGGER copy_out AFTER INSERT ON notes BEGIN DELETE FROM '_sirannon_meta'; END",
  "PRAGMA table_info('_sirannon_meta')",
  "SELECT * FROM pragma_table_info('_sirannon_changes')",
  "EXPLAIN QUERY PLAN SELECT * FROM '_sirannon_meta'",
  "SELECT 1; SELECT * FROM '_sirannon_meta'",
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('guard-rails')
})

afterEach(async () => {
  await workspace.dispose()
})

function processTempStoreDirectory(node = false): unknown {
  const raw = node ? new DatabaseSync(':memory:') : new BetterSqlite3(':memory:')
  try {
    return raw.prepare('PRAGMA temp_store_directory').all()
  } finally {
    raw.close()
  }
}

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

  it('refuses a statement that modifies the sqlite_ catalogue, ATTACH, DETACH, VACUUM INTO, and the writable_schema, temp_store_directory, and data_store_directory pragmas in every spelling', async () => {
    const { db, path } = await openShop()
    const attached = workspace.path('attached.db')
    const copies = [
      'copy.db',
      'main-copy.db',
      'quoted-copy.db',
      'string-copy.db',
      'joined-copy.db',
      'temp-copy.db',
    ].map(name => workspace.path(name))
    const directory = workspace.path('')
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
      "PRAGMA 'writable_schema' = 1",
      "PRAGMA main.'WRITABLE_SCHEMA' = 1",
      `VACUUM INTO '${copies[0]}'`,
      `vacuum main into '${copies[1]}'`,
      `VACUUM "main" /* then */ INTO '${copies[2]}'`,
      `VACUUM 'main' INTO '${copies[3]}'`,
      `SELECT 1; VACUUM INTO '${copies[4].slice(0, -3)}' || '.db'`,
      `VACUUM temp INTO '${copies[5]}'`,
      'PRAGMA temp_store_directory',
      `PRAGMA temp_store_directory = '${directory}'`,
      `pragma TEMP_STORE_DIRECTORY('${directory}')`,
      `PRAGMA main.temp_store_directory = '${directory}'`,
      `PRAGMA "temp_store_directory" = '${directory}'`,
      `PRAGMA [temp_store_directory] = '${directory}'`,
      `PRAGMA 'temp_store_directory' = '${directory}'`,
      `PRAGMA 'main'.'temp_store_directory' = '${directory}'`,
      `PRAGMA data_store_directory = '${directory}'`,
      `PRAGMA 'data_store_directory' = '${directory}'`,
    ]

    for (const sql of refused) {
      await expect(db.execute(sql), sql).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
      await expect(db.query(sql), sql).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }

    expect(existsSync(attached)).toBe(false)
    expect(copies.filter(copy => existsSync(copy))).toEqual([])
    expect(processTempStoreDirectory()).toEqual([])
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
    await db.execute('CREATE TABLE café_sirannon_archive (id INTEGER PRIMARY KEY)')
    await db.execute('INSERT INTO café_sirannon_archive VALUES (1)')
    expect(await db.query('SELECT id FROM café_sirannon_archive')).toEqual([{ id: 1 }])
    expect(columnRaw(path, 'SELECT id FROM café_sirannon_archive')).toEqual([1n])
    const bodies = [
      '_sirannon_changes',
      "it's sqlite_master and _sirannon_meta",
      'ATTACH DATABASE x AS y',
      'PRAGMA writable_schema = 1',
      "VACUUM INTO 'copy.db'",
      'PRAGMA temp_store_directory = /tmp',
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
    await db.execute('DELETE FROM notes')
    await db.execute('VACUUM')
    await db.execute('VACUUM main')
    expect(columnRaw(path, 'SELECT count(*) FROM notes')).toEqual([0n])
    expect(await db.query("PRAGMA journal_mode = 'wal'")).toEqual([{ journal_mode: 'wal' }])
  })

  it('refuses exactly the single-quoted names that SQLite resolves to an internal table and reads the rest as values', async () => {
    const { db, path } = await openShop()
    await db.execute("INSERT INTO notes (body) VALUES ('_sirannon_meta'), ('_sirannon_changes'), ('plain')")
    const before = schemaSnapshot(path)
    const notesBefore = columnRaw(path, 'SELECT body FROM notes ORDER BY id')

    for (const sql of QUOTED_INTERNAL_TABLES) {
      const reached = tablesSqliteReaches(path, sql)
      if (reached)
        expect(
          reached.some(name => name.startsWith('_sirannon')),
          `SQLite reaches no internal table in ${sql}`,
        ).toBe(true)
      for (const [label, call] of ENTRY_POINTS) {
        await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
      }
      await expect(db.live(sql), `live: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }
    expect(columnRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual(notesBefore)
    expect(schemaSnapshot(path)).toEqual(before)

    for (const sql of QUOTED_INTERNAL_VALUES) {
      const reached = tablesSqliteReaches(path, sql)
      if (reached)
        expect(
          reached.filter(name => name.startsWith('_sirannon')),
          sql,
        ).toEqual([])
      const expected = readRaw(path, sql).map(row =>
        Object.fromEntries(
          Object.entries(row).map(([key, value]) => [key, typeof value === 'bigint' ? Number(value) : value]),
        ),
      )
      expect(await db.query(sql), sql).toEqual(expected)
      const columns = await db.queryWithColumns(sql)
      expect(
        columns.rows.map(row =>
          Object.fromEntries(columns.columns.map((column, position) => [column.name, row[position]])),
        ),
        sql,
      ).toEqual(expected)
    }

    expect(schemaSnapshot(path)).toEqual(before)
  })
})

const QUOTED_INTERNAL_TABLES = [
  "SELECT * FROM '_sirannon_meta'",
  "select key from main.'_SIRANNON_META'",
  "SELECT * FROM orders CROSS JOIN '_sirannon_changes'",
  "SELECT * FROM orders WHERE item NOT IN (SELECT key FROM main.'_sirannon_meta')",
  "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM orders, '_sirannon_changes')",
  "WITH leaked AS (SELECT * FROM '_sirannon_meta') SELECT * FROM leaked",
  "INSERT OR REPLACE INTO '_sirannon_meta' VALUES ('a', 'b')",
  "UPDATE notes SET body = 'x' FROM '_sirannon_meta'",
  "DELETE FROM main.'_sirannon_meta' WHERE key = 'a'",
  "CREATE TABLE copied AS SELECT * FROM '_sirannon_changes'",
  "ANALYZE '_sirannon_meta'",
  "PRAGMA main.index_list('_sirannon_meta')",
  "SELECT * FROM ('_sirannon_meta')",
  "SELECT * FROM ( '_sirannon_meta' )",
  "SELECT * FROM orders JOIN ('_sirannon_meta')",
  "SELECT * FROM orders, ('_sirannon_meta')",
  "SELECT * FROM (orders, '_sirannon_meta')",
  "SELECT * FROM ((orders, ('_sirannon_meta')))",
  "SELECT * FROM (((('_sirannon_changes'))))",
  "SELECT * FROM (SELECT 1) AS one, ('_sirannon_meta')",
  "SELECT * FROM orders LEFT JOIN (orders AS o JOIN ('_sirannon_meta')) ON 1",
  "INSERT INTO notes (body) SELECT key FROM (orders, ('_sirannon_meta'))",
  "UPDATE notes SET body = 'x' FROM (('_sirannon_meta'))",
]

const QUOTED_INTERNAL_VALUES = [
  "SELECT body FROM notes WHERE body = '_sirannon_meta'",
  "SELECT '_sirannon_meta' AS name, '_sirannon_changes' AS other FROM notes WHERE body = 'plain'",
  "SELECT body FROM notes WHERE body IN ('_sirannon_meta', '_sirannon_changes') ORDER BY body",
  "SELECT body FROM notes WHERE body IS DISTINCT FROM '_sirannon_meta' ORDER BY body",
  "SELECT body FROM notes WHERE body LIKE '_sirannon%' ORDER BY body",
  "SELECT CASE WHEN body = 'plain' THEN '_sirannon_meta' ELSE '_sirannon_changes' END AS label FROM notes ORDER BY id",
  "SELECT body FROM notes WHERE body BETWEEN '_sirannon_a' AND '_sirannon_z' ORDER BY body",
  "SELECT count(*) FILTER (WHERE body <> '_sirannon_meta') AS n FROM notes",
  "SELECT coalesce(NULL, '_sirannon_meta') || '_sirannon_changes' AS joined",
  "SELECT json_extract('{\"_sirannon_meta\":1}', '$._sirannon_meta') AS found",
  "SELECT orders.item FROM orders JOIN notes ON notes.body <> '_sirannon_meta' WHERE orders.id = 1 LIMIT 1",
  "SELECT body FROM notes WHERE body IN ('_sirannon_meta') ORDER BY body",
  "SELECT * FROM (SELECT '_sirannon_meta' AS name)",
  "SELECT * FROM (VALUES ('_sirannon_meta'))",
  "SELECT * FROM (WITH listed AS (SELECT '_sirannon_changes' AS name) SELECT name FROM listed)",
  "SELECT orders.item FROM orders JOIN notes ON (notes.body = '_sirannon_meta') ORDER BY orders.id",
  'SELECT value FROM json_each((\'["_sirannon_meta"]\'))',
]

function tablesSqliteReaches(path: string, sql: string): string[] | undefined {
  const raw = new DatabaseSync(path, { readOnly: true })
  try {
    if (typeof raw.setAuthorizer !== 'function') return undefined
    const names: string[] = []
    raw.setAuthorizer((_action, first, second) => {
      for (const name of [first, second]) if (typeof name === 'string') names.push(name.toLowerCase())
      return sqliteConstants.SQLITE_OK
    })
    raw.prepare(sql)
    return names
  } finally {
    raw.close()
  }
}

describe.each(DRIVER_PLACEMENTS)('statement prefixes with $label', ({ driver, writerWorker }) => {
  it('accepts pragma values that refer to tables with reserved pragma names', async () => {
    const path = workspace.path('pragma-values.db')
    const db = await workspace.registry({ driver: driver() }).open('pragma-values', path, { writerWorker })
    for (const table of ['temp_store_directory', 'data_store_directory', 'writable_schema']) {
      await db.execute(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, body TEXT)`)
    }
    for (const sql of [
      "PRAGMA table_info('temp_store_directory')",
      'PRAGMA main.table_info = "writable_schema"',
      'PRAGMA table_info([data_store_directory])',
      'PRAGMA table_info(temp_store_directory)',
    ]) {
      const expected = readRaw(path, sql).map(row => ({
        ...row,
        cid: Number(row.cid),
        notnull: Number(row.notnull),
        pk: Number(row.pk),
      }))
      expect(expected).toHaveLength(2)
      expect(await db.query(sql)).toEqual(expected)
    }
    expect(processTempStoreDirectory()).toEqual([])
    expect(processTempStoreDirectory(true)).toEqual([])
  })

  it('refuses reserved statements after a byte-order mark across calls and database reopenings', async () => {
    const path = workspace.path('prefixes.db')
    const copy = workspace.path('copy.db')
    const attached = workspace.path('attached.db')
    const refused = [
      `\ufeffVACUUM INTO '${copy}'`,
      `; /* prefix */ \ufeff\ufeffVACUUM 'main' INTO '${copy}'`,
      `\ufeffATTACH '${attached}' AS other`,
      `\ufeffPRAGMA main.'temp_store_directory' = '${workspace.path('')}'`,
      `\ufeffPRAGMA data_store_directory = '${workspace.path('')}'`,
      '\ufeffPRAGMA writable_schema = 1',
      `\ufeffEXPLAIN PRAGMA temp_store_directory = '${workspace.path('')}'`,
      `\ufeffEXPLAIN QUERY PLAN PRAGMA temp_store_directory = '${workspace.path('')}'`,
    ]
    for (let opening = 0; opening < 2; opening++) {
      const registry = workspace.registry({ driver: driver() })
      const db = await registry.open('prefixes', path, { writerWorker })
      await db.execute('CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY)')
      for (const sql of refused) {
        for (const [label, call] of ENTRY_POINTS) {
          await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
        }
        await expect(db.live(sql)).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
      }
      await db.execute('\ufeffVACUUM')
      await db.execute('\ufeffVACUUM main')
      expect(existsSync(copy)).toBe(false)
      expect(existsSync(attached)).toBe(false)
      expect(processTempStoreDirectory()).toEqual([])
      expect(processTempStoreDirectory(true)).toEqual([])
      await registry.shutdown()
    }
  })

  it('refuses reserved pragmas after EXPLAIN during concurrent writes and query hooks', async () => {
    const path = workspace.path('explained.db')
    const db = await workspace.registry({ driver: driver() }).open('explained', path, { writerWorker })
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY)')
    const refused = [
      `EXPLAIN PRAGMA temp_store_directory = '${workspace.path('')}'`,
      `explain /* prefix */ query -- plan follows\n plan pragma 'main'.'TEMP_STORE_DIRECTORY'('${workspace.path('')}')`,
      `EXPLAIN PRAGMA data_store_directory = '${workspace.path('')}'`,
      'EXPLAIN QUERY PLAN PRAGMA "writable_schema" = 1',
    ]
    for (const sql of refused) {
      for (const [label, call] of ENTRY_POINTS) {
        await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
      }
      await expect(db.live(sql)).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }
    const sql = refused[0]
    const settled = await Promise.allSettled([
      db.execute('INSERT INTO notes VALUES (1)'),
      db.query(sql),
      db.executeTransaction([{ sql: 'INSERT INTO notes VALUES (2)' }, { sql }]),
      db.execute('INSERT INTO notes VALUES (3)'),
    ])
    expect(settled.map(outcome => (outcome.status === 'rejected' ? outcome.reason.code : 'ok'))).toEqual([
      'ok',
      'FORBIDDEN_SQL',
      'FORBIDDEN_SQL',
      'ok',
    ])
    expect(columnRaw(path, 'SELECT id FROM notes ORDER BY id')).toEqual([1n, 3n])

    const trigger = 'EXPLAIN QUERY PLAN SELECT id FROM notes'
    let nested: Promise<unknown> | undefined
    const removeHook = db.onBeforeQuery(context => {
      if (context.sql === trigger) nested = expect(db.execute(sql)).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    })
    await db.query(trigger)
    expect(nested).toBeDefined()
    await nested
    removeHook()
    for (let repeat = 0; repeat < 2; repeat++) {
      expect(await db.query(trigger)).toEqual(
        readRaw(path, trigger).map(row => ({
          ...row,
          id: Number(row.id),
          parent: Number(row.parent),
          notused: Number(row.notused),
        })),
      )
      await expect(db.execute(sql)).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }
    expect(processTempStoreDirectory()).toEqual([])
    expect(processTempStoreDirectory(true)).toEqual([])
  })
})
