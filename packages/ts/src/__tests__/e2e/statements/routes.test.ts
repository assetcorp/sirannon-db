import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { Transaction } from '../../../core/transaction.js'
import type { QueryOptions, ServerExecutionTarget } from '../../../core/types.js'
import {
  errorCodeOf,
  errorMessageOf,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServerMessage,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const ORDERS_SCHEMA =
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL CHECK (total >= 0))'
const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const SEEDED: Order[] = [
  { id: 1, customer: 'ama', total: 10 },
  { id: 2, customer: 'kofi', total: 20 },
  { id: 3, customer: 'esi', total: 30 },
  { id: 4, customer: 'kwame', total: 40 },
  { id: 5, customer: 'abena', total: 50 },
]
const ROW_ID_PAST_SAFE_INTEGERS = '9007199254740993'
const STATEMENT_ROUTES = ['query', 'execute', 'transaction', 'batch', 'load'] as const

interface Order {
  id: number | string
  customer: string
  total: number
}

type Route = 'query' | 'execute' | 'transaction' | 'batch'

type Outcome = { ok: unknown } | { code: string; mentions?: string }

interface StatementCase {
  label: string
  route: Route
  body: Record<string, unknown>
  expected: (orders: Order[]) => Outcome
  apply?: (orders: Order[]) => void
}

const INVALID = 'INVALID'

function selected(orders: Order[], keep: (order: Order) => boolean): Array<Record<string, unknown>> {
  return orders.filter(keep).map(order => ({ id: order.id, customer: order.customer, total: order.total }))
}

const CASES: StatementCase[] = [
  {
    label: 'a read with positional parameters',
    route: 'query',
    body: { sql: 'SELECT id, customer, total FROM orders WHERE total > ? ORDER BY id', params: [25] },
    expected: orders => ({ ok: { rows: selected(orders, order => order.total > 25) } }),
  },
  {
    label: 'a read with named parameters',
    route: 'query',
    body: { sql: 'SELECT id, customer, total FROM orders WHERE customer = :customer', params: { customer: 'kofi' } },
    expected: orders => ({ ok: { rows: selected(orders, order => order.customer === 'kofi') } }),
  },
  {
    label: 'a read that matches no row',
    route: 'query',
    body: { sql: 'SELECT id FROM orders WHERE id = ?', params: [999] },
    expected: () => ({ ok: { rows: [] } }),
  },
  {
    label: 'an insert with positional parameters',
    route: 'execute',
    body: { sql: INSERT_ORDER, params: ['yaw', 60] },
    expected: () => ({ ok: { changes: 1, lastInsertRowId: 6 } }),
    apply: orders => {
      orders.push({ id: 6, customer: 'yaw', total: 60 })
    },
  },
  {
    label: 'an update with named parameters',
    route: 'execute',
    body: { sql: 'UPDATE orders SET total = total + :bump WHERE total >= :floor', params: { bump: 5, floor: 40 } },
    expected: orders => ({
      ok: { changes: orders.filter(order => order.total >= 40).length, lastInsertRowId: expect.anything() },
    }),
    apply: orders => {
      for (const order of orders) if (order.total >= 40) order.total += 5
    },
  },
  {
    label: 'a delete',
    route: 'execute',
    body: { sql: 'DELETE FROM orders WHERE customer = ?', params: ['esi'] },
    expected: () => ({ ok: { changes: 1, lastInsertRowId: expect.anything() } }),
    apply: orders => {
      orders.splice(
        orders.findIndex(order => order.customer === 'esi'),
        1,
      )
    },
  },
  {
    label: 'a transaction mixing positional and named parameters',
    route: 'transaction',
    body: {
      statements: [
        { sql: INSERT_ORDER, params: ['afua', 15] },
        {
          sql: 'INSERT INTO orders (customer, total) VALUES (:customer, :total)',
          params: { customer: 'kojo', total: 25 },
        },
        { sql: 'UPDATE orders SET total = 0 WHERE customer = ?', params: ['ama'] },
      ],
    },
    expected: () => ({
      ok: {
        results: [
          { changes: 1, lastInsertRowId: 7 },
          { changes: 1, lastInsertRowId: 8 },
          { changes: 1, lastInsertRowId: expect.anything() },
        ],
      },
    }),
    apply: orders => {
      orders.push({ id: 7, customer: 'afua', total: 15 }, { id: 8, customer: 'kojo', total: 25 })
      for (const order of orders) if (order.customer === 'ama') order.total = 0
    },
  },
  {
    label: 'a transaction whose second statement breaks a constraint',
    route: 'transaction',
    body: {
      statements: [
        { sql: INSERT_ORDER, params: ['ghost', 1] },
        { sql: 'UPDATE orders SET total = -1 WHERE customer = ?', params: ['kofi'] },
      ],
    },
    expected: () => ({ code: 'QUERY_ERROR' }),
  },
  {
    label: 'a batch of positional parameter sets',
    route: 'batch',
    body: {
      sql: INSERT_ORDER,
      paramsBatch: [
        ['efua', 5],
        ['nana', 6],
        ['akua', 7],
      ],
    },
    expected: () => ({
      ok: {
        results: [
          { changes: 1, lastInsertRowId: 9 },
          { changes: 1, lastInsertRowId: 10 },
          { changes: 1, lastInsertRowId: 11 },
        ],
      },
    }),
    apply: orders => {
      orders.push(
        { id: 9, customer: 'efua', total: 5 },
        { id: 10, customer: 'nana', total: 6 },
        { id: 11, customer: 'akua', total: 7 },
      )
    },
  },
  {
    label: 'a batch of named parameter sets',
    route: 'batch',
    body: {
      sql: 'INSERT INTO orders (customer, total) VALUES (:customer, :total)',
      paramsBatch: [{ customer: 'adjoa', total: 8 }],
    },
    expected: () => ({ ok: { results: [{ changes: 1, lastInsertRowId: 12 }] } }),
    apply: orders => {
      orders.push({ id: 12, customer: 'adjoa', total: 8 })
    },
  },
  {
    label: 'a batch whose third parameter set repeats a primary key',
    route: 'batch',
    body: {
      sql: 'INSERT INTO orders (id, customer, total) VALUES (?, ?, ?)',
      paramsBatch: [
        [100, 'first', 1],
        [101, 'second', 1],
        [100, 'repeat', 1],
      ],
    },
    expected: () => ({ code: 'QUERY_ERROR' }),
  },
  {
    label: 'a read with no statement',
    route: 'query',
    body: { params: [] },
    expected: () => ({ code: INVALID, mentions: 'sql' }),
  },
  {
    label: 'a transaction with no statement list',
    route: 'transaction',
    body: {},
    expected: () => ({ code: INVALID, mentions: 'statements' }),
  },
  {
    label: 'a transaction with an empty statement list',
    route: 'transaction',
    body: { statements: [] },
    expected: () => ({ code: INVALID, mentions: 'at least one statement' }),
  },
  {
    label: 'a transaction whose second statement has no SQL',
    route: 'transaction',
    body: { statements: [{ sql: INSERT_ORDER, params: ['lost', 1] }, { params: [1] }] },
    expected: () => ({ code: INVALID, mentions: 'index 1' }),
  },
  {
    label: 'a transaction whose first statement binds a string as its parameters',
    route: 'transaction',
    body: { statements: [{ sql: INSERT_ORDER, params: 'lost' }] },
    expected: () => ({ code: INVALID, mentions: 'index 0' }),
  },
  {
    label: 'a batch with no parameter sets',
    route: 'batch',
    body: { sql: INSERT_ORDER },
    expected: () => ({ code: INVALID, mentions: 'paramsBatch' }),
  },
  {
    label: 'a batch with an empty list of parameter sets',
    route: 'batch',
    body: { sql: INSERT_ORDER, paramsBatch: [] },
    expected: () => ({ code: INVALID, mentions: 'paramsBatch' }),
  },
  {
    label: 'a batch whose second parameter set is a string',
    route: 'batch',
    body: { sql: INSERT_ORDER, paramsBatch: [['lost', 1], 'lost'] },
    expected: () => ({ code: INVALID, mentions: 'index 1' }),
  },
  {
    label: 'a batch with an unknown write concern level',
    route: 'batch',
    body: { sql: INSERT_ORDER, paramsBatch: [['lost', 1]], writeConcern: { level: 'everyone' } },
    expected: () => ({ code: INVALID, mentions: 'writeConcern' }),
  },
  {
    label: 'a transaction whose write concern waits for zero milliseconds',
    route: 'transaction',
    body: {
      statements: [{ sql: INSERT_ORDER, params: ['lost', 1] }],
      writeConcern: { level: 'majority', timeoutMs: 0 },
    },
    expected: () => ({ code: INVALID, mentions: 'timeoutMs' }),
  },
  {
    label: 'a read with an unknown read concern level',
    route: 'query',
    body: { sql: 'SELECT 1', readConcern: { level: 'strong' } },
    expected: () => ({ code: INVALID, mentions: 'readConcern' }),
  },
  {
    label: 'a statement that SQLite cannot parse',
    route: 'query',
    body: { sql: 'SELEC id FROM orders' },
    expected: () => ({ code: 'QUERY_ERROR' }),
  },
  {
    label: 'a read from a table that does not exist',
    route: 'query',
    body: { sql: 'SELECT * FROM refunds' },
    expected: () => ({ code: 'QUERY_ERROR', mentions: 'refunds' }),
  },
  {
    label: 'an insert whose row id is past the safe integer range',
    route: 'execute',
    body: { sql: `INSERT INTO orders (id, customer, total) VALUES (${ROW_ID_PAST_SAFE_INTEGERS}, 'big', 1)` },
    expected: () => ({ ok: { changes: 1, lastInsertRowId: ROW_ID_PAST_SAFE_INTEGERS } }),
    apply: orders => {
      orders.push({ id: ROW_ID_PAST_SAFE_INTEGERS, customer: 'big', total: 1 })
    },
  },
]

function outcomeOverHttp(status: number, body: unknown): Outcome & { status: number } {
  if (status === 200) return { status, ok: body }
  const code = errorCodeOf(body) ?? 'none'
  return { status, code: code === 'INVALID_REQUEST' ? INVALID : code, mentions: errorMessageOf(body) }
}

function outcomeOverWebSocket(reply: ServerMessage): Outcome {
  if (reply.type === 'result') return { ok: reply.data }
  const code = reply.error?.code ?? 'none'
  return { code: code === 'INVALID_MESSAGE' ? INVALID : code, mentions: reply.error?.message ?? '' }
}

function expectOutcome(actual: Outcome, expected: Outcome, label: string): void {
  if ('ok' in expected) {
    expect('ok' in actual ? actual.ok : actual, label).toEqual(expected.ok)
    return
  }
  expect('code' in actual && actual.code, label).toBe(expected.code)
  if (expected.mentions !== undefined && 'code' in actual) {
    expect(actual.mentions, label).toContain(expected.mentions)
  }
}

function storedOrders(path: string): Order[] {
  return readSqlite<{ id: bigint; customer: string; total: bigint }>(
    path,
    'SELECT id, customer, total FROM orders ORDER BY id',
  ).map(row => ({
    id: row.id > BigInt(Number.MAX_SAFE_INTEGER) ? row.id.toString() : Number(row.id),
    customer: row.customer,
    total: Number(row.total),
  }))
}

async function seededDatabase(sirannon: Sirannon, id: string, path: string): Promise<Database> {
  const db = await sirannon.open(id, path)
  await db.execute(ORDERS_SCHEMA)
  await db.executeBatch(
    INSERT_ORDER,
    SEEDED.map(order => [order.customer, order.total]),
  )
  return db
}

function recordingTarget(database: Database, seen: Array<[string, QueryOptions | undefined]>): ServerExecutionTarget {
  return {
    query: (sql, params, options) => {
      seen.push(['query', options])
      return database.query(sql, params, options)
    },
    execute: (sql, params, options) => {
      seen.push(['execute', options])
      return database.execute(sql, params, options)
    },
    transaction: <T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions) => {
      seen.push(['transaction', options])
      return database.transaction(fn)
    },
    executeTransaction: (statements, options) => {
      seen.push(['executeTransaction', options])
      return database.executeTransaction(statements)
    },
  }
}

let dir: string

beforeEach(() => {
  dir = scratchDirectory('statement-routes')
})

afterEach(releaseServedRegistries)

describe('statements sent to the server', () => {
  it('answers every statement the same way over HTTP and WebSocket and leaves each file as the statements describe', async () => {
    const sirannon = trackedRegistry()
    const httpPath = join(dir, 'over-http.db')
    const wsPath = join(dir, 'over-ws.db')
    await seededDatabase(sirannon, 'over-http', httpPath)
    await seededDatabase(sirannon, 'over-ws', wsPath)
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const ws = await RawSocket.open(`${served.wsBase}/db/over-ws`)
    const model = SEEDED.map(order => ({ ...order }))

    for (const [index, statement] of CASES.entries()) {
      const expected = statement.expected(model)
      const http = await postJson(served.base, `/db/over-http/${statement.route}`, statement.body)
      const reply = await ws.request({ id: `case-${index}`, type: statement.route, ...statement.body })
      const overHttp = outcomeOverHttp(http.status, http.body)

      expect(overHttp.status, statement.label).toBe('ok' in expected ? 200 : 400)
      expectOutcome(overHttp, expected, `${statement.label} over HTTP`)
      expectOutcome(outcomeOverWebSocket(reply), expected, `${statement.label} over WebSocket`)
      statement.apply?.(model)
    }

    expect(storedOrders(httpPath)).toEqual(model)
    expect(storedOrders(wsPath)).toEqual(model)
  })

  it('answers every statement route for a database that is not open with 404 DATABASE_NOT_FOUND', async () => {
    const sirannon = trackedRegistry()
    await seededDatabase(sirannon, 'orders', join(dir, 'orders.db'))
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const body = {
      sql: INSERT_ORDER,
      params: ['nobody', 1],
      paramsBatch: [['nobody', 1]],
      statements: [{ sql: 'SELECT 1' }],
    }

    for (const route of STATEMENT_ROUTES) {
      for (const id of ['refunds', 'orders.db', 'ORDERS']) {
        const answer = await postJson(served.base, `/db/${id}/${route}`, body)
        expect([answer.status, errorCodeOf(answer.body)], `${route} on ${id}`).toEqual([404, 'DATABASE_NOT_FOUND'])
      }
    }
    expect(storedOrders(join(dir, 'orders.db'))).toEqual(SEEDED)
  })

  it('reaches a database whose identifier the client percent-encodes, over HTTP, WebSocket, and the published client', async () => {
    const sirannon = trackedRegistry()
    const ids = ['north wing', 'café', 'a/b']
    for (const [index, id] of ids.entries()) await seededDatabase(sirannon, id, join(dir, `encoded-${index}.db`))
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const client = new SirannonClient(served.base, { transport: 'http' })

    for (const id of ids) {
      const answer = await postJson(served.base, `/db/${encodeURIComponent(id)}/query`, {
        sql: 'SELECT count(*) AS n FROM orders',
      })
      const ws = await RawSocket.open(`${served.wsBase}/db/${encodeURIComponent(id)}`)
      const reply = await ws.request({ id: 'count', type: 'query', sql: 'SELECT count(*) AS n FROM orders' })

      expect([answer.status, answer.body], id).toEqual([200, { rows: [{ n: SEEDED.length }] }])
      expect(reply, id).toMatchObject({ type: 'result', data: { rows: [{ n: SEEDED.length }] } })
      expect(await client.database(id).query('SELECT count(*) AS n FROM orders'), id).toEqual([{ n: SEEDED.length }])
    }
    client.close()
  })

  it('hands the read and write concern of each statement to the execution target that serves it', async () => {
    const sirannon = trackedRegistry()
    const path = join(dir, 'orders.db')
    const database = await seededDatabase(sirannon, 'orders', path)
    const seen: Array<[string, QueryOptions | undefined]> = []
    const served = await serveRegistry(sirannon, {
      acceptSql: true,
      operations: {
        orders: {
          reads: { everyOrder: { statement: () => ({ sql: 'SELECT id FROM orders' }) } },
          writes: {
            addOrder: { args: ['customer'], statements: args => ({ sql: INSERT_ORDER, params: [args.customer, 5] }) },
          },
        },
      },
      resolveExecutionTarget: id => (id === 'orders' ? recordingTarget(database, seen) : null),
    })
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)

    await postJson(served.base, '/db/orders/query', { sql: 'SELECT 1', readConcern: { level: 'linearizable' } })
    await postJson(served.base, '/db/orders/query', { sql: 'SELECT 1' })
    await postJson(served.base, '/db/orders/execute', {
      sql: INSERT_ORDER,
      params: ['by-http', 1],
      writeConcern: { level: 'majority', timeoutMs: 1_500 },
    })
    await postJson(served.base, '/db/orders/transaction', {
      statements: [{ sql: INSERT_ORDER, params: ['by-transaction', 2] }],
      writeConcern: { level: 'all' },
    })
    await postJson(served.base, '/db/orders/batch', {
      sql: INSERT_ORDER,
      paramsBatch: [['by-batch', 3]],
      writeConcern: { level: 'local' },
    })
    await ws.request({ id: 'read', type: 'query', sql: 'SELECT 1', readConcern: { level: 'majority' } })
    await ws.request({ id: 'named', type: 'query', name: 'everyOrder', readConcern: { level: 'local' } })
    await ws.request({
      id: 'batch',
      type: 'batch',
      sql: INSERT_ORDER,
      paramsBatch: [['by-ws-batch', 4]],
      writeConcern: { level: 'majority', timeoutMs: 20 },
    })
    await ws.request({ id: 'execute', type: 'execute', sql: INSERT_ORDER, params: ['by-ws-execute', 5] })
    await ws.request({
      id: 'transaction',
      type: 'transaction',
      statements: [{ sql: INSERT_ORDER, params: ['by-ws-transaction', 6] }],
      writeConcern: { level: 'local' },
    })
    await ws.request({
      id: 'named-write',
      type: 'execute',
      name: 'addOrder',
      args: { customer: 'by-ws-named' },
      writeConcern: { level: 'all', timeoutMs: 75 },
    })

    expect(seen).toEqual([
      ['query', { readConcern: { level: 'linearizable' } }],
      ['query', undefined],
      ['execute', { writeConcern: { level: 'majority', timeoutMs: 1_500 } }],
      ['executeTransaction', { writeConcern: { level: 'all' } }],
      ['transaction', { writeConcern: { level: 'local' } }],
      ['query', { readConcern: { level: 'majority' } }],
      ['query', { readConcern: { level: 'local' } }],
      ['transaction', { writeConcern: { level: 'majority', timeoutMs: 20 } }],
      ['execute', undefined],
      ['executeTransaction', { writeConcern: { level: 'local' } }],
      ['executeTransaction', { writeConcern: { level: 'all', timeoutMs: 75 } }],
    ])
    expect(storedOrders(path).map(order => order.customer)).toEqual([
      ...SEEDED.map(order => order.customer),
      'by-http',
      'by-transaction',
      'by-batch',
      'by-ws-batch',
      'by-ws-execute',
      'by-ws-transaction',
      'by-ws-named',
    ])
  })

  it('returns what the file holds through the published client over each transport', async () => {
    const sirannon = trackedRegistry()
    const path = join(dir, 'orders.db')
    await seededDatabase(sirannon, 'orders', path)
    const served = await serveRegistry(sirannon, { acceptSql: true })

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(served.base, { transport, autoReconnect: false })
      const orders = client.database('orders')
      const inserted = await orders.execute(INSERT_ORDER, [`${transport}-one`, 70])
      const committed = await orders.transaction([
        { sql: INSERT_ORDER, params: [`${transport}-two`, 71] },
        { sql: 'UPDATE orders SET total = total + 1 WHERE customer = :customer', params: { customer: 'kofi' } },
      ])
      const batched = await orders.batch(INSERT_ORDER, [[`${transport}-three`, 72]])
      const rows = await orders.query<Order>('SELECT id, customer, total FROM orders ORDER BY id')
      const stored = storedOrders(path)

      expect(rows, transport).toEqual(stored)
      expect(inserted.changes, transport).toBe(1)
      expect(stored.find(order => order.id === inserted.lastInsertRowId)?.customer, transport).toBe(`${transport}-one`)
      expect(
        committed.map(result => result.changes),
        transport,
      ).toEqual([1, 1])
      expect(stored.find(order => order.id === batched[0]?.lastInsertRowId)?.customer, transport).toBe(
        `${transport}-three`,
      )
      client.close()
    }
  })

  it('refuses a scalar parameter value with INVALID_REQUEST over HTTP and INVALID_MESSAGE over WebSocket', async () => {
    const sirannon = trackedRegistry()
    const path = join(dir, 'orders.db')
    await seededDatabase(sirannon, 'orders', path)
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)

    for (const [route, body] of [
      ['query', { sql: 'SELECT ? AS v', params: 5 }],
      ['query', { sql: 'SELECT ? AS v', params: 'kofi' }],
      ['execute', { sql: 'DELETE FROM orders WHERE customer = ?', params: 'kofi' }],
    ] as const) {
      const answer = await postJson(served.base, `/db/orders/${route}`, body)
      const reply = await ws.request({ id: `${route}-scalar`, type: route, ...body })

      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(body)).toEqual([400, 'INVALID_REQUEST'])
      expect(reply.error?.code, JSON.stringify(body)).toBe('INVALID_MESSAGE')
    }
    expect(storedOrders(path)).toEqual(SEEDED)
  })

  it('refuses an empty statement with INVALID_REQUEST over HTTP and INVALID_MESSAGE over WebSocket', async () => {
    const sirannon = trackedRegistry()
    await seededDatabase(sirannon, 'orders', join(dir, 'orders.db'))
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)

    for (const route of ['query', 'execute', 'batch', 'load'] as const) {
      const body = { sql: '', paramsBatch: [[]] }
      const answer = await postJson(served.base, `/db/orders/${route}`, body)
      const reply = await ws.request({ id: `${route}-empty`, type: route, ...body })

      expect([answer.status, errorCodeOf(answer.body)], route).toEqual([400, 'INVALID_REQUEST'])
      expect(reply.error?.code, route).toBe('INVALID_MESSAGE')
    }
  })
})
