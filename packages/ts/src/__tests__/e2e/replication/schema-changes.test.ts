import { describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/query-types.js'
import {
  caughtUp,
  columnNames,
  errorsOf,
  eventually,
  indexNames,
  readRows,
  tableNames,
  tableRows,
} from '../lib/replication-scene.js'
import { converged, PRIMARY, REPLICA, ready, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

const INVENTORY_SCHEMA = 'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL UNIQUE)'
const ACCOUNTS_SCHEMA = 'CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL)'
const NOTES_SCHEMA = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)'

async function inventoryPair(extraSchema: string[] = [], extraWatch: string[] = []) {
  const { scene } = handle
  const primary = await scene.primary(PRIMARY, {
    schema: [INVENTORY_SCHEMA, ...extraSchema],
    watch: ['inventory', ...extraWatch],
  })
  const replica = await scene.replica(REPLICA, { port: primary.port })
  await ready(replica)
  return { primary, replica }
}

describe('schema changes on a replicating primary', () => {
  it('reach a replica as an index, added columns, and the rows that use them, whether written or forwarded', async () => {
    const { primary, replica } = await inventoryPair()

    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-001')")
    await primary.engine.execute('CREATE INDEX inventory_by_sku ON inventory (sku)')
    await primary.engine.execute('ALTER TABLE inventory ADD COLUMN quantity INTEGER')
    await primary.engine.execute('UPDATE inventory SET quantity = 7 WHERE id = 1')
    await primary.engine.transaction(async tx => {
      await tx.execute("ALTER TABLE inventory ADD COLUMN status TEXT DEFAULT 'new'")
      await tx.execute('ALTER TABLE inventory ADD COLUMN weight REAL')
      await tx.execute(
        "INSERT INTO inventory (id, sku, quantity, status, weight) VALUES (2, 'sku-002', 3, 'held', 1.5)",
      )
    })
    await replica.engine.forwardStatements([
      { sql: 'ALTER TABLE inventory ADD COLUMN aisle TEXT' },
      { sql: 'ALTER TABLE inventory ADD COLUMN bay INTEGER' },
      { sql: "INSERT INTO inventory (id, sku, quantity, aisle, bay) VALUES (3, 'sku-003', 1, 'north', 4)" },
    ])
    await replica.engine.executeTransaction([
      { sql: 'CREATE INDEX inventory_by_aisle ON inventory (aisle, bay)' },
      { sql: "UPDATE inventory SET aisle = 'south' WHERE id = 1" },
    ])

    const columns = ['id', 'sku', 'quantity', 'status', 'weight', 'aisle', 'bay']
    await converged(primary, [replica], 'inventory', [
      { id: 1, sku: 'sku-001', quantity: 7, status: 'new', weight: null, aisle: 'south', bay: null },
      { id: 2, sku: 'sku-002', quantity: 3, status: 'held', weight: 1.5, aisle: null, bay: null },
      { id: 3, sku: 'sku-003', quantity: 1, status: 'new', weight: null, aisle: 'north', bay: 4 },
    ])
    expect(columnNames(primary.dbPath, 'inventory')).toEqual(columns)
    expect(columnNames(replica.dbPath, 'inventory')).toEqual(columns)
    expect(indexNames(replica.dbPath, 'inventory')).toEqual(['inventory_by_aisle', 'inventory_by_sku'])
    expect(errorsOf(replica)).toEqual([])
  })

  it('keep a subscriber on the replica receiving each change with the columns that a peer just added', async () => {
    const { primary, replica } = await inventoryPair()
    await replica.db.watch('inventory')
    const seen: ChangeEvent[] = []
    replica.db.on('inventory').subscribe(event => seen.push(event))

    await primary.engine.transaction(async tx => {
      await tx.execute('ALTER TABLE inventory ADD COLUMN bin TEXT')
      await tx.execute('ALTER TABLE inventory ADD COLUMN shelf INTEGER')
      await tx.execute("INSERT INTO inventory (id, sku, bin, shelf) VALUES (1, 'sku-001', 'b-7', 2)")
    })
    await primary.engine.execute("UPDATE inventory SET bin = 'b-9' WHERE id = 1")
    await primary.engine.execute('ALTER TABLE inventory ADD COLUMN tag TEXT')
    await primary.engine.execute(
      "INSERT INTO inventory (id, sku, bin, shelf, tag) VALUES (2, 'sku-002', NULL, 0, 'cold')",
    )

    await eventually(() => seen.length >= 3, 'three change events on the replica')
    expect(seen.map(event => [event.type, event.row])).toEqual([
      ['insert', { id: 1, sku: 'sku-001', bin: 'b-7', shelf: 2 }],
      ['update', { id: 1, sku: 'sku-001', bin: 'b-9', shelf: 2 }],
      ['insert', { id: 2, sku: 'sku-002', bin: null, shelf: 0, tag: 'cold' }],
    ])
    expect(seen.every(event => event.origin === PRIMARY)).toBe(true)
  })

  it('drop a table on the replica, stop capturing it, and capture its replacement only once it is watched again', async () => {
    const { primary, replica } = await inventoryPair([ACCOUNTS_SCHEMA], ['accounts'])
    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'before-drop')")
    await caughtUp(replica, primary)

    await primary.engine.transaction(async tx => {
      await tx.execute('DROP TABLE inventory')
      await tx.execute('ALTER TABLE accounts ADD COLUMN balance INTEGER')
      await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (1, 'amara', 500)")
    })
    await primary.engine.execute('CREATE TABLE inventory (id INTEGER PRIMARY KEY, label TEXT, qty INTEGER)')
    await primary.engine.execute("INSERT INTO inventory (id, label, qty) VALUES (1, 'unwatched', 1)")
    await caughtUp(replica, primary)

    expect(columnNames(replica.dbPath, 'inventory')).toEqual(['id', 'label', 'qty'])
    expect(tableRows(replica.dbPath, 'inventory')).toEqual([])
    await converged(primary, [replica], 'accounts', [{ id: 1, holder: 'amara', balance: 500 }])

    await primary.tracker.watch(primary.conn, 'inventory')
    await primary.engine.execute("INSERT INTO inventory (id, label, qty) VALUES (2, 'watched again', 2)")
    await primary.engine.execute('DELETE FROM inventory WHERE id = 1')
    await converged(primary, [replica], 'inventory', [{ id: 2, label: 'watched again', qty: 2 }])
    expect(errorsOf(replica)).toEqual([])
  })

  it('roll back a schema change whose transaction fails, on the primary and on every replica', async () => {
    const { primary, replica } = await inventoryPair()
    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-existing')")

    const forwarded = await replica.engine
      .forwardStatements([
        { sql: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER' },
        { sql: "INSERT INTO inventory (id, sku, quantity) VALUES (2, 'sku-existing', 1)" },
      ])
      .catch((error: unknown) => error)
    const local = await primary.engine
      .transaction(async tx => {
        await tx.execute('DROP TABLE inventory')
        await tx.execute('CREATE TABLE inventory (id INTEGER PRIMARY KEY)')
        throw new Error('the operator cancelled the rebuild')
      })
      .catch((error: unknown) => error)
    const semicolon = await primary.engine.execute('DROP TABLE inventory;').catch((error: unknown) => error)
    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (3, 'after-rollback')")

    expect(forwarded).toMatchObject({ code: 'QUERY_ERROR' })
    expect(local).toMatchObject({ message: 'the operator cancelled the rebuild' })
    expect(semicolon).toMatchObject({ code: 'REPLICATION_ERROR' })
    await converged(primary, [replica], 'inventory', [
      { id: 1, sku: 'sku-existing' },
      { id: 3, sku: 'after-rollback' },
    ])
    expect(columnNames(replica.dbPath, 'inventory')).toEqual(['id', 'sku'])
    expect(columnNames(primary.dbPath, 'inventory')).toEqual(['id', 'sku'])
  })

  it('reach a replica for a table that nobody watches, without error and without capturing its rows', async () => {
    const { primary, replica } = await inventoryPair([NOTES_SCHEMA])
    await primary.engine.execute('ALTER TABLE notes ADD COLUMN mood TEXT')
    await primary.engine.execute("INSERT INTO notes (id, body, mood) VALUES (1, 'unwatched', 'calm')")
    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'watched')")

    await converged(primary, [replica], 'inventory', [{ id: 1, sku: 'watched' }])
    expect(columnNames(replica.dbPath, 'notes')).toEqual(['id', 'body', 'mood'])
    expect(tableRows(replica.dbPath, 'notes')).toEqual([])
    expect(errorsOf(replica)).toEqual([])
  })

  it('refuse on the primary, before it commits, every schema change that replicas cannot replay, and keep the replica level', async () => {
    const { primary, replica } = await inventoryPair()
    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-001')")

    const statements = [
      'CREATE VIEW cheap AS SELECT * FROM inventory',
      'CREATE TRIGGER audit AFTER INSERT ON inventory BEGIN SELECT 1; END',
      'CREATE TABLE inventory_copy AS SELECT * FROM inventory',
      'ALTER TABLE inventory RENAME COLUMN sku TO code',
      'ALTER TABLE inventory RENAME TO stock',
    ]
    const outcomes = []
    for (const sql of statements) outcomes.push(await primary.engine.execute(sql).catch((error: unknown) => error))
    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (2, 'sku-002')")

    for (const outcome of outcomes) expect(outcome).toBeInstanceOf(Error)
    expect(tableNames(primary.dbPath)).toEqual(['inventory'])
    await converged(primary, [replica], 'inventory', [
      { id: 1, sku: 'sku-001' },
      { id: 2, sku: 'sku-002' },
    ])
    expect(
      readRows(
        replica.dbPath,
        "SELECT count(*) AS n FROM sqlite_master WHERE type IN ('view', 'trigger') AND name IN ('cheap', 'audit')",
      ),
    ).toEqual([{ n: 0 }])
  })
})
