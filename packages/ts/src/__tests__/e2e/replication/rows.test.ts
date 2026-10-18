import { describe, expect, it } from 'vitest'
import { errorsOf, eventually, type Row, readRows, tableRows } from '../lib/replication-scene.js'
import {
  converged,
  INSERT_ORDER,
  ORDERS_SCHEMA,
  order,
  PRIMARY,
  REPLICA,
  ready,
  SECOND_REPLICA,
  sceneForEachTest,
} from './harness.js'

const handle = sceneForEachTest()

const LEDGER_SCHEMA =
  'CREATE TABLE ledger (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, ratio REAL, payload BLOB, note TEXT)'
const INSERT_LEDGER = 'INSERT INTO ledger (id, label, amount, ratio, payload, note) VALUES (?, ?, ?, ?, ?, ?)'

const WIDE_BLOB = Buffer.from(Array.from({ length: 256 }, (_, at) => at))

function ledgerValues(firstId: number): unknown[][] {
  return [
    [firstId, 'parcel to Accra', 9007199254740993n, 12345.6789, Buffer.from([1, 2, 3, 255, 127, 0]), null],
    [firstId + 1, '', 0, -0.5, Buffer.alloc(0), ''],
    [firstId + 2, 'ünïcødé 🦀 "quoted" \'single\' \\ back', 9223372036854775807n, 1e300, WIDE_BLOB, 'x'.repeat(10_000)],
    [firstId + 3, null, -9223372036854775808n, Number.MIN_VALUE, null, 'NULL'],
    [firstId + 4, 'safe edge', 9007199254740991, -0.0000001, Buffer.from('00ff', 'hex'), '\u0000inside'],
  ]
}

function ledgerRow(values: unknown[]): Row {
  const [id, label, amount, ratio, payload, note] = values
  return { id, label, amount, ratio, payload, note }
}

describe('rows that a primary writes', () => {
  it('reach a replica unchanged for every SQLite value type, through the first copy and the live stream alike', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [LEDGER_SCHEMA], watch: ['ledger'] })
    const copied = ledgerValues(1)
    await primary.engine.executeBatch(INSERT_LEDGER, copied)

    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)
    const streamed = ledgerValues(101)
    for (const values of streamed) await primary.engine.execute(INSERT_LEDGER, values)

    const expected = [...copied, ...streamed].map(ledgerRow)
    await converged(primary, [replica], 'ledger', expected)
    expect(readRows(replica.dbPath, 'SELECT typeof(amount) AS kind FROM ledger WHERE id IN (3, 103)')).toEqual([
      { kind: 'integer' },
      { kind: 'integer' },
    ])
    expect(readRows(replica.dbPath, 'SELECT typeof(payload) AS kind FROM ledger WHERE id IN (2, 102)')).toEqual(
      readRows(primary.dbPath, 'SELECT typeof(payload) AS kind FROM ledger WHERE id IN (2, 102)'),
    )
  })

  it('apply on a replica in the order written, so updates, deletes, and reinserts of one key end the same way', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)

    await primary.engine.execute(INSERT_ORDER, [1, 'amara', 10])
    await primary.engine.execute(INSERT_ORDER, [2, 'kofi', 20])
    await primary.engine.execute('UPDATE orders SET total = total + 5 WHERE id = 1')
    await primary.engine.execute('DELETE FROM orders WHERE id = 2')
    await primary.engine.execute(INSERT_ORDER, [2, 'kofi again', 21])
    await primary.engine.execute('UPDATE orders SET customer = ? WHERE id = ?', ['amara mensah', 1])
    await primary.engine.execute('DELETE FROM orders WHERE id = 1')
    await primary.engine.execute(INSERT_ORDER, [1, 'returned', 0])
    await primary.engine.execute('UPDATE orders SET total = 99 WHERE id = 404')

    await converged(primary, [replica], 'orders', [order(1, 'returned', 0), order(2, 'kofi again', 21)])
    expect(errorsOf(replica)).toEqual([])
  })

  it('commit on a replica only as a whole transaction, and a rolled-back transaction leaves both files untouched', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [
        'CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL, balance INTEGER NOT NULL CHECK (balance >= 0))',
        'CREATE TABLE transfers (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, delta INTEGER NOT NULL)',
      ],
      watch: ['accounts', 'transfers'],
    })
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)

    await primary.engine.executeBatch('INSERT INTO accounts (id, holder, balance) VALUES (?, ?, ?)', [
      [1, 'amara', 1000],
      [2, 'kofi', 500],
    ])
    const returned = await primary.engine.transaction(async tx => {
      await tx.execute('UPDATE accounts SET balance = balance - 300 WHERE id = 1')
      await tx.execute('UPDATE accounts SET balance = balance + 300 WHERE id = 2')
      await tx.execute('INSERT INTO transfers (id, account_id, delta) VALUES (1, 1, -300), (2, 2, 300)')
      return 'moved'
    })
    const refusal = await primary.engine
      .transaction(async tx => {
        await tx.execute('UPDATE accounts SET balance = balance + 5000 WHERE id = 2')
        await tx.execute('INSERT INTO transfers (id, account_id, delta) VALUES (3, 1, -5000)')
        await tx.execute('UPDATE accounts SET balance = balance - 5000 WHERE id = 1')
      })
      .catch((error: unknown) => error)
    const batchRefusal = await primary.engine
      .executeTransaction([
        { sql: 'INSERT INTO transfers (id, account_id, delta) VALUES (4, 2, 1)' },
        { sql: 'INSERT INTO transfers (id, account_id, delta) VALUES (4, 2, 2)' },
      ])
      .catch((error: unknown) => error)
    const parallel = await Promise.all(
      [10, 11, 12, 13].map(id =>
        primary.engine.transaction(async tx => {
          await tx.execute('INSERT INTO transfers (id, account_id, delta) VALUES (?, 1, 0)', [id])
          await tx.execute('INSERT INTO transfers (id, account_id, delta) VALUES (?, 2, 0)', [id + 100])
          return id
        }),
      ),
    )

    expect(returned).toBe('moved')
    expect(refusal).toMatchObject({ code: 'QUERY_ERROR' })
    expect(batchRefusal).toMatchObject({ code: 'QUERY_ERROR' })
    expect(parallel).toEqual([10, 11, 12, 13])
    await converged(primary, [replica], 'accounts', [
      { id: 1, holder: 'amara', balance: 700 },
      { id: 2, holder: 'kofi', balance: 800 },
    ])
    const transferIds = [1, 2, 10, 11, 12, 13, 110, 111, 112, 113]
    expect(tableRows(replica.dbPath, 'transfers').map(row => row.id)).toEqual(transferIds)
    expect(tableRows(primary.dbPath, 'transfers').map(row => row.id)).toEqual(transferIds)
  })

  it('reach a replica that enforces foreign keys, parents first on insert and children first on delete', async () => {
    const { scene } = handle
    const schema = [
      'CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
      'CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL REFERENCES authors(id), title TEXT NOT NULL)',
      'CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL REFERENCES posts(id), body TEXT NOT NULL)',
    ]
    const primary = await scene.primary(PRIMARY, { schema, watch: ['authors', 'posts', 'comments'] })
    await primary.engine.execute("INSERT INTO authors (id, name) VALUES (1, 'ama')")
    await primary.engine.execute("INSERT INTO posts (id, author_id, title) VALUES (10, 1, 'copied')")
    await primary.engine.execute("INSERT INTO comments (id, post_id, body) VALUES (100, 10, 'first')")
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)

    await primary.engine.execute("INSERT INTO authors (id, name) VALUES (2, 'yaw')")
    await primary.engine.execute("INSERT INTO posts (id, author_id, title) VALUES (11, 2, 'streamed')")
    await primary.engine.execute("INSERT INTO comments (id, post_id, body) VALUES (101, 11, 'second')")
    await primary.engine.transaction(async tx => {
      await tx.execute('DELETE FROM comments WHERE post_id = 10')
      await tx.execute('DELETE FROM posts WHERE id = 10')
      await tx.execute('DELETE FROM authors WHERE id = 1')
    })

    await converged(primary, [replica], 'authors', [{ id: 2, name: 'yaw' }])
    await converged(primary, [replica], 'posts', [{ id: 11, author_id: 2, title: 'streamed' }])
    await converged(primary, [replica], 'comments', [{ id: 101, post_id: 11, body: 'second' }])
    expect(readRows(replica.dbPath, 'PRAGMA foreign_key_check')).toEqual([])
    expect(errorsOf(replica)).toEqual([])
  })

  it('reach a replica in batches no larger than it accepts, and a sender set larger than that limit gets nothing applied', async () => {
    const { scene } = handle
    const rowCount = 1_500
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
      config: { batchSize: 250 },
    })
    const replica = await scene.replica(REPLICA, { port: primary.port, config: { maxBatchChanges: 250 } })
    await ready(replica)
    const values = Array.from({ length: rowCount }, (_, at) => `(${at + 1}, 'bulk-${at + 1}', ${at * 3})`)
    await primary.engine.execute(`INSERT INTO orders (id, customer, total) VALUES ${values.join(', ')}`)
    const expected = Array.from({ length: rowCount }, (_, at) => order(at + 1, `bulk-${at + 1}`, at * 3))
    await converged(primary, [replica], 'orders', expected)

    const oversized = await scene.replica(SECOND_REPLICA, { port: primary.port, config: { maxBatchChanges: 249 } })
    await ready(oversized)
    await primary.engine.executeBatch(
      INSERT_ORDER,
      Array.from({ length: 250 }, (_, at) => [rowCount + at + 1, 'over the limit', 1]),
    )
    await eventually(
      () => errorsOf(oversized, 'batch-received').some(event => /exceeds max 249/.test(event.error.message)),
      'the replica to refuse a batch of 250 changes',
    )
    expect(errorsOf(oversized, 'batch-received')[0]?.error).toMatchObject({ code: 'BATCH_VALIDATION_ERROR' })
    expect(readRows(oversized.dbPath, 'SELECT count(*) AS n FROM orders WHERE id > ?', [rowCount])).toEqual([{ n: 0 }])
    await converged(primary, [replica], 'orders', [
      ...expected,
      ...Array.from({ length: 250 }, (_, at) => order(rowCount + at + 1, 'over the limit', 1)),
    ])
  })
})
