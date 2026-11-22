import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import {
  columnRaw,
  createWorkspace,
  readRaw,
  STATEMENT_ENTRY_POINTS,
  type Workspace,
  WRITER_PLACEMENTS,
} from './harness.js'

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n
const UINT64_MASK = 0xffffffffffffffffn

const TRANSACTION_CONTROL = [
  'BEGIN',
  'begin immediate',
  'BEGIN EXCLUSIVE TRANSACTION',
  '/* first */ BEGIN DEFERRED',
  `${String.fromCharCode(0xfeff)}BEGIN`,
  'SELECT 1; BEGIN',
  'EXPLAIN BEGIN',
  'COMMIT',
  'commit transaction',
  'END',
  'END TRANSACTION',
  'ROLLBACK',
  'SAVEPOINT caller',
  'RELEASE caller',
  'RELEASE SAVEPOINT caller',
  'ROLLBACK TO caller',
]

const TEMPORARY_OBJECTS = [
  'CREATE TEMP VIEW orders AS SELECT 99 AS id, 0 AS total',
  'CREATE TEMPORARY TABLE orders (id INTEGER, total INTEGER)',
  'create temp table if not exists orders (id INTEGER)',
  'CREATE TABLE temp.orders (id INTEGER, total INTEGER)',
  'CREATE TABLE "temp".orders (id INTEGER)',
  "CREATE TABLE 'TEMP' . orders (id INTEGER)",
  'CREATE TABLE IF NOT EXISTS [temp]/* schema */.orders (id INTEGER)',
  'CREATE VIEW `temp`.orders AS SELECT 99 AS id',
  'CREATE TEMP TRIGGER orders_zero AFTER INSERT ON orders BEGIN UPDATE orders SET total = 0; END',
  'CREATE TRIGGER temp.orders_zero AFTER INSERT ON orders BEGIN UPDATE orders SET total = 0; END',
  'CREATE VIRTUAL TABLE temp.orders USING fts5(id)',
  'DROP TABLE temp.orders',
  'DROP VIEW IF EXISTS temp.orders',
  'DROP TRIGGER temp.orders_zero',
  'EXPLAIN CREATE TEMP TABLE orders (id INTEGER)',
  'SELECT 1; CREATE TEMP TABLE orders (id INTEGER)',
]

function migrationChecksum(sql: string): string {
  let hash = FNV_OFFSET_BASIS
  for (const byte of new TextEncoder().encode(sql.replace(/\r\n?/g, '\n').trim())) {
    hash = ((hash ^ BigInt(byte)) * FNV_PRIME) & UINT64_MASK
  }
  return hash.toString(16).padStart(16, '0')
}

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('connection-state')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(WRITER_PLACEMENTS)('statements that would change a shared connection, with $label', ({
  writerWorker,
}) => {
  async function openShop(): Promise<{ db: Database; path: string }> {
    const path = workspace.path('shop.db')
    const db = await workspace.registry().open('shop', path, { writerWorker })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.executeBatch('INSERT INTO orders (id, total) VALUES (?, ?)', [
      [1, 10],
      [2, 20],
    ])
    await db.watch('orders')
    return { db, path }
  }

  it('refuses transaction control through every entry point, so another caller commits a write of its own', async () => {
    const { db, path } = await openShop()

    for (const sql of TRANSACTION_CONTROL) {
      for (const [label, call] of STATEMENT_ENTRY_POINTS) {
        await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({
          code: 'FORBIDDEN_SQL',
          message: expect.stringContaining('transaction()'),
        })
      }
      await expect(db.live(sql), `live: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }

    await expect(db.execute('BEGIN')).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    await db.execute('INSERT INTO orders (id, total) VALUES (3, 30)')
    await expect(db.execute('ROLLBACK')).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    expect(readRaw(path, 'SELECT id, total FROM orders ORDER BY id')).toEqual([
      { id: 1n, total: 10n },
      { id: 2n, total: 20n },
      { id: 3n, total: 30n },
    ])
    await db.transaction(async tx => {
      await tx.execute('INSERT INTO orders (id, total) VALUES (4, 40)')
    })
    expect(columnRaw(path, 'SELECT id FROM orders ORDER BY id')).toEqual([1n, 2n, 3n, 4n])
  })

  it('refuses every temporary object, so reads inside a transaction return the rows that the table holds', async () => {
    const { db, path } = await openShop()

    for (const sql of TEMPORARY_OBJECTS) {
      for (const [label, call] of STATEMENT_ENTRY_POINTS) {
        await expect(call(db, sql), `${label}: ${sql}`).rejects.toMatchObject({
          code: 'FORBIDDEN_SQL',
          message: expect.stringContaining('transaction()'),
        })
      }
      await expect(db.live(sql), `live: ${sql}`).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    }

    await db.execute('INSERT INTO orders (id, total) VALUES (3, 30)')
    const stored = readRaw<{ id: bigint; total: bigint }>(path, 'SELECT id, total FROM orders ORDER BY id').map(
      row => ({
        id: Number(row.id),
        total: Number(row.total),
      }),
    )
    expect(stored).toHaveLength(3)
    expect(await db.transaction(tx => tx.query('SELECT id, total FROM orders ORDER BY id'))).toEqual(stored)
    expect(await db.query('SELECT id, total FROM orders ORDER BY id')).toEqual(stored)
  })

  it('accepts the END that closes a trigger body, in a statement and in a migration, and the triggers fire', async () => {
    const { db, path } = await openShop()
    await db.migrate([
      {
        version: 1,
        name: 'audit_orders',
        up: `CREATE TABLE audit (order_id INTEGER NOT NULL, size TEXT NOT NULL);
CREATE TRIGGER orders_audit AFTER INSERT ON orders WHEN new.total > 0 BEGIN
  INSERT INTO audit (order_id, size) VALUES (new.id, CASE WHEN new.total > 25 THEN 'large' ELSE 'small' END);
END;`,
      },
    ])
    await db.execute(
      'CREATE TRIGGER "end" AFTER DELETE ON orders BEGIN SELECT CASE WHEN old.total < 0 THEN RAISE(ROLLBACK, \'negative\') END; DELETE FROM audit WHERE order_id = old.id; END',
    )

    await db.execute('INSERT INTO orders (id, total) VALUES (3, 30)')
    await db.execute('INSERT INTO orders (id, total) VALUES (4, 5)')
    await db.execute('DELETE FROM orders WHERE id = 3')

    expect(readRaw(path, 'SELECT order_id, size FROM audit ORDER BY order_id')).toEqual([
      { order_id: 4n, size: 'small' },
    ])
    expect(
      columnRaw(
        path,
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name IN ('orders_audit', 'end') ORDER BY name",
      ),
    ).toEqual(['end', 'orders_audit'])
  })

  it('refuses a pending migration that controls the transaction or creates a temporary object before it applies any pending migration', async () => {
    const { db, path } = await openShop()
    const first = { version: 1, name: 'add_customers', up: 'CREATE TABLE customers (id INTEGER PRIMARY KEY)' }
    const refusedUps = [
      'CREATE TABLE refunds (id INTEGER PRIMARY KEY); COMMIT; BEGIN',
      'SAVEPOINT rebuild; CREATE TABLE refunds (id INTEGER); RELEASE rebuild',
      'CREATE TEMP TABLE staging AS SELECT * FROM orders; DROP TABLE staging',
      'CREATE TABLE temp.staging (id INTEGER)',
      'SELECT $marker(/*); COMMIT; CREATE TABLE refunds (id INTEGER)',
      "SELECT :marker(');BEGIN",
      'SELECT @marker(--);CREATE TEMP TABLE staging (id INTEGER)',
      'SELECT #marker(;CREATE/**/TRIGGER/**/t/**/BEGIN); END',
    ]

    for (const up of refusedUps) {
      await expect(db.migrate([first, { version: 2, name: 'rebuild_orders', up }]), up).rejects.toMatchObject({
        code: 'FORBIDDEN_SQL',
        message: expect.stringContaining('migration'),
      })
    }
    expect(columnRaw(path, "SELECT name FROM sqlite_master WHERE name IN ('customers', 'refunds')")).toEqual([])

    const applied = await db.migrate([
      first,
      {
        version: 2,
        name: 'add_refunds',
        up: 'CREATE TABLE refunds (id INTEGER PRIMARY KEY)',
        down: 'BEGIN; DROP TABLE refunds',
      },
    ])
    expect(applied.applied.map(entry => entry.version)).toEqual([1, 2])
    await expect(
      db.rollback([
        first,
        {
          version: 2,
          name: 'add_refunds',
          up: 'CREATE TABLE refunds (id INTEGER PRIMARY KEY)',
          down: 'BEGIN; DROP TABLE refunds',
        },
      ]),
    ).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    expect(columnRaw(path, 'SELECT version FROM _sirannon_migrations ORDER BY version')).toEqual([1n, 2n])
    expect(columnRaw(path, "SELECT name FROM sqlite_master WHERE name = 'refunds'")).toEqual(['refunds'])
  })

  it('applies new migrations to a database whose applied history holds a temporary table', async () => {
    const path = workspace.path('history.db')
    const historic = 'CREATE TEMP TABLE staging AS SELECT 1 AS id; CREATE TABLE kept (id INTEGER); DROP TABLE staging'
    const first = await workspace.registry().open('history', path, { writerWorker })
    await first.migrate([{ version: 1, name: 'historic', up: 'CREATE TABLE kept (id INTEGER)' }])
    const raw = new BetterSqlite3(path)
    try {
      raw.prepare('UPDATE _sirannon_migrations SET checksum = ? WHERE version = 1').run(migrationChecksum(historic))
    } finally {
      raw.close()
    }

    const migrations = [
      { version: 1, name: 'historic', up: historic },
      { version: 2, name: 'add_refunds', up: 'CREATE TABLE refunds (id INTEGER PRIMARY KEY)' },
    ]
    const result = await first.migrate(migrations)

    expect(result.applied.map(entry => entry.version)).toEqual([2])
    expect(columnRaw(path, 'SELECT version FROM _sirannon_migrations ORDER BY version')).toEqual([1n, 2n])
    expect(columnRaw(path, "SELECT name FROM sqlite_master WHERE name = 'refunds'")).toEqual(['refunds'])
  })
})
