import { afterEach, describe, expect, it } from 'vitest'
import type { OperationRegistry } from '../../../core/types.js'
import { postJson } from '../lib/served-registry.js'
import {
  readFile,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  triggerNames,
  type WireMessage,
  WireSocket,
  waitFor,
} from './harness.js'

const SCHEMA = [
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL)',
  'CREATE TABLE scratch (id INTEGER PRIMARY KEY)',
]
const EVERY_ORDER_SQL = 'SELECT id, customer FROM orders ORDER BY id'

const operations: OperationRegistry = {
  shop: { reads: { everyOrder: { statement: () => ({ sql: EVERY_ORDER_SQL }) } } },
}

afterEach(releaseChangeStreams)

function shop(label: string): Promise<ServedDatabase> {
  return serveDatabase({ label, databaseId: 'shop', schema: SCHEMA, server: { operations } })
}

async function run(served: ServedDatabase, sql: string, params: unknown[] = []): Promise<void> {
  const answer = await postJson(served.served.base, '/db/shop/execute', { sql, params })
  expect(answer.status, `${sql} ${answer.text}`).toBe(200)
}

function heldRows(messages: readonly WireMessage[]): unknown[] {
  let rows: unknown[] = []
  for (const message of messages) {
    if (message.rows !== undefined) rows = [...message.rows]
    for (const op of message.ops ?? []) {
      if (op.op === 'insert') rows.splice(op.index, 0, op.row)
      else if (op.op === 'update') rows[op.index] = op.row
      else rows.splice(op.index, 1)
    }
  }
  return rows
}

describe('schema changes under open subscriptions', () => {
  it('keeps every subscription and live query delivering while tables, columns, and indexes change around them', async () => {
    const served = await shop('ddl-mix')
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await ws.request({ id: 'orders', type: 'subscribe', table: 'orders' })
    await ws.request({ id: 'every', type: 'subscribe', name: 'everyOrder' })

    await run(served, 'INSERT INTO orders (customer) VALUES (?)', ['before'])
    await run(served, 'CREATE INDEX orders_by_customer ON orders (customer)')
    await run(served, 'ALTER TABLE orders ADD COLUMN region TEXT')
    await run(served, 'INSERT INTO orders (customer, region) VALUES (?, ?)', ['with region', 'emea'])
    await run(served, 'CREATE TABLE audit (id INTEGER PRIMARY KEY, entry TEXT NOT NULL)')
    await ws.request({ id: 'audit', type: 'subscribe', table: 'audit' })
    await run(served, 'DROP INDEX orders_by_customer')
    await run(served, 'DROP TABLE scratch')
    await run(served, "UPDATE orders SET region = 'apac' WHERE id = 1")
    await run(served, 'INSERT INTO audit (entry) VALUES (?)', ['audited'])
    const orders = await ws.waitForEvents('orders', 3)
    const audit = await ws.waitForEvents('audit', 1)
    await waitFor(() => ws.of('every', 'live').length >= 2, 'the live query to follow the writes')

    expect(orders.map(event => [event.type, event.row])).toEqual([
      ['insert', { id: 1, customer: 'before' }],
      ['insert', { id: 2, customer: 'with region', region: 'emea' }],
      ['update', { id: 1, customer: 'before', region: 'apac' }],
    ])
    expect(audit.map(event => event.row)).toEqual([{ id: 1, entry: 'audited' }])
    expect(heldRows(ws.of('every'))).toEqual(
      readFile(served.path, EVERY_ORDER_SQL).map(row => ({ ...row, id: Number(row.id) })),
    )
    expect(triggerNames(served.path).filter(name => name.includes('scratch'))).toEqual([])
  })

  it('adds a column to the change rows of a table subscription when the column arrives after it subscribes', async () => {
    const served = await shop('ddl-new-column')
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await ws.request({ id: 'orders', type: 'subscribe', table: 'orders' })

    await run(served, 'ALTER TABLE orders ADD COLUMN region TEXT')
    await run(served, 'INSERT INTO orders (customer, region) VALUES (?, ?)', ['with region', 'emea'])
    const [event] = await ws.waitForEvents('orders', 1)

    expect(event?.row).toEqual({ id: 1, customer: 'with region', region: 'emea' })
  })

  it('delivers changes to a new subscription on a table dropped and created again', async () => {
    const served = await shop('ddl-recreated-table')
    const first = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await first.request({ id: 'scratch', type: 'subscribe', table: 'scratch' })

    await run(served, 'DROP TABLE scratch')
    await run(served, 'CREATE TABLE scratch (id INTEGER PRIMARY KEY)')
    const second = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const subscribed = await second.request({ id: 'scratch', type: 'subscribe', table: 'scratch' })
    await run(served, 'INSERT INTO scratch (id) VALUES (7)')
    const [event] = await second.waitForEvents('scratch', 1)

    expect(subscribed.type).toBe('subscribed')
    expect(event?.row).toEqual({ id: 7 })
  })
})
