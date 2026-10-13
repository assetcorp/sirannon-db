import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  countSqlite,
  errorCodeOf,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const INSERT_NAMED = 'INSERT INTO orders (customer, total) VALUES (:customer, :total)'
const SEEDED_ORDERS = 4
const STATEMENT_ROUTES = ['query', 'execute', 'transaction', 'batch', 'load'] as const
const NESTING_DEPTH = 100_000
const HTTP_CLIENTS = 20
const WEBSOCKET_CLIENTS = 10
const ROWS_PER_CLIENT = 6

let path: string
let served: ServedRegistry

async function stillServes(): Promise<void> {
  const answer = await postJson(served.base, '/db/orders/query', { sql: 'SELECT count(*) AS n FROM orders' })
  expect(answer.body).toEqual({ rows: [{ n: countSqlite(path, 'orders') }] })
}

function customers(): string[] {
  return readSqlite<{ customer: string }>(path, 'SELECT customer FROM orders ORDER BY id').map(row => row.customer)
}

beforeEach(async () => {
  path = join(scratchDirectory('hostile-statements'), 'orders.db')
  const sirannon = trackedRegistry()
  const db = await sirannon.open('orders', path)
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)')
  await db.executeBatch(
    INSERT_ORDER,
    Array.from({ length: SEEDED_ORDERS }, (_, index) => [`seed-${index}`, index]),
  )
  served = await serveRegistry(sirannon, { acceptSql: true })
})

afterEach(releaseServedRegistries)

describe('statements built to break the server', () => {
  it('answers each malformed body on every statement route with 400 and the code that names the fault', async () => {
    const before = customers()
    const bodies: Array<[label: string, body: string, code: string]> = [
      ['truncated JSON', '{"sql":"DELETE FROM orders"', 'INVALID_JSON'],
      ['plain text', 'DELETE FROM orders', 'INVALID_JSON'],
      ['a trailing comma', '{"sql":"DELETE FROM orders",}', 'INVALID_JSON'],
      ['an empty body', '', 'EMPTY_BODY'],
      ['an empty list', '[]', 'INVALID_REQUEST'],
      ['a list holding a statement', '[{"sql":"DELETE FROM orders"}]', 'INVALID_REQUEST'],
      ['null', 'null', 'INVALID_REQUEST'],
      ['a number', '5', 'INVALID_REQUEST'],
      ['a string', '"DELETE FROM orders"', 'INVALID_REQUEST'],
      ['true', 'true', 'INVALID_REQUEST'],
      [
        `parameters nested ${NESTING_DEPTH} lists deep`,
        `{"sql":"DELETE FROM orders","params":${'['.repeat(NESTING_DEPTH)}${']'.repeat(NESTING_DEPTH)},"statements":[{"sql":"DELETE FROM orders","params":${'['.repeat(NESTING_DEPTH)}${']'.repeat(NESTING_DEPTH)}}],"paramsBatch":[${'['.repeat(NESTING_DEPTH)}${']'.repeat(NESTING_DEPTH)}]}`,
        'INVALID_REQUEST',
      ],
    ]

    for (const route of STATEMENT_ROUTES) {
      for (const [label, body, code] of bodies) {
        const answer = await sendRaw(served.base, 'POST', `/db/orders/${route}`, body)
        expect([answer.status, errorCodeOf(answer.body)], `${label} on ${route}`).toEqual([400, code])
      }
    }
    expect(customers()).toEqual(before)
    await stillServes()
  })

  it('refuses each field of the wrong type over HTTP and WebSocket alike and writes nothing', async () => {
    const before = customers()
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)
    const wrong: Array<[route: string, body: Record<string, unknown>]> = [
      ['query', { sql: 5 }],
      ['query', { sql: { text: 'SELECT 1' } }],
      ['query', { sql: ['SELECT 1'] }],
      ['query', { sql: null }],
      ['query', { sql: true }],
      ['query', { sql: 'SELECT 1', readConcern: 'local' }],
      ['query', { sql: 'SELECT 1', readConcern: ['local'] }],
      ['query', { sql: 'SELECT 1', readConcern: {} }],
      ['query', { sql: 'SELECT 1', readConcern: { level: 'local', extra: 1 } }],
      ['query', { sql: 'SELECT 1', readConcern: { level: 5 } }],
      ['execute', { sql: 7, params: [] }],
      ['transaction', { statements: { sql: 'DELETE FROM orders' } }],
      ['transaction', { statements: 'DELETE FROM orders' }],
      ['transaction', { statements: [null] }],
      ['transaction', { statements: [5] }],
      ['transaction', { statements: [['DELETE FROM orders']] }],
      ['transaction', { statements: [{ sql: 5 }] }],
      ['transaction', { statements: [{ sql: '' }] }],
      ['transaction', { statements: [{ sql: 'DELETE FROM orders', params: 5 }] }],
      ['transaction', { statements: [{ sql: 'DELETE FROM orders' }], writeConcern: 'majority' }],
      [
        'transaction',
        { statements: [{ sql: 'DELETE FROM orders' }], writeConcern: { level: 'majority', timeoutMs: -1 } },
      ],
      [
        'transaction',
        { statements: [{ sql: 'DELETE FROM orders' }], writeConcern: { level: 'majority', timeoutMs: 1.5 } },
      ],
      [
        'transaction',
        { statements: [{ sql: 'DELETE FROM orders' }], writeConcern: { level: 'majority', timeoutMs: '5' } },
      ],
      ['transaction', { statements: [{ sql: 'DELETE FROM orders' }], writeConcern: { level: 'all', extra: true } }],
      ['batch', { paramsBatch: [[]] }],
      ['batch', { sql: 'DELETE FROM orders', paramsBatch: { rows: [] } }],
      ['batch', { sql: 'DELETE FROM orders', paramsBatch: 'rows' }],
      ['batch', { sql: 'DELETE FROM orders', paramsBatch: [null] }],
      ['batch', { sql: 'DELETE FROM orders', paramsBatch: [5] }],
      ['batch', { sql: 'DELETE FROM orders', paramsBatch: [true] }],
      [
        'batch',
        { sql: 'DELETE FROM orders', paramsBatch: [[]], writeConcern: { level: 'majority', timeoutMs: 2 ** 53 } },
      ],
      ['load', { sql: INSERT_ORDER, paramsBatch: { rows: [] } }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [] }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [null] }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [['lost', 1]], durability: 'sometimes' }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [['lost', 1]], durability: 5 }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [['lost', 1]], durability: null }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [['lost', 1]], checkpoint: 'yes' }],
      ['load', { sql: INSERT_ORDER, paramsBatch: [['lost', 1]], checkpoint: 1 }],
    ]

    for (const [index, [route, body]] of wrong.entries()) {
      const label = `${route} ${JSON.stringify(body)}`
      const answer = await postJson(served.base, `/db/orders/${route}`, body)
      const reply = await ws.request({ id: `wrong-${index}`, type: route, ...body })

      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([400, 'INVALID_REQUEST'])
      expect([reply.type, reply.error?.code], label).toEqual(['error', 'INVALID_MESSAGE'])
    }
    expect(customers()).toEqual(before)
    expect(ws.open).toBe(true)
    await stillServes()
  })

  it('keeps keys that name the object prototype as plain data and never pollutes the prototype', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)
    const bodies: Array<[route: string, body: string, status: number]> = [
      ['query', '{"__proto__":{"sql":"DELETE FROM orders"}}', 400],
      ['query', '{"constructor":{"sql":"DELETE FROM orders"}}', 400],
      [
        'execute',
        `{"sql":"${INSERT_NAMED}","params":{"customer":"from-proto","total":1,"__proto__":{"polluted":true}}}`,
        200,
      ],
      ['execute', '{"sql":"DELETE FROM orders WHERE customer = :constructor","params":{"constructor":"nobody"}}', 200],
      ['execute', '{"sql":"DELETE FROM orders WHERE customer = :prototype","params":{"prototype":"nobody"}}', 200],
      [
        'transaction',
        '{"statements":[{"sql":"DELETE FROM orders WHERE customer = ?","params":["nobody"],"__proto__":{"sql":"DELETE FROM orders"}}]}',
        200,
      ],
      [
        'batch',
        `{"sql":"${INSERT_NAMED}","paramsBatch":[{"__proto__":{"customer":"x"},"customer":"from-batch","total":2}]}`,
        200,
      ],
      [
        'batch',
        '{"sql":"DELETE FROM orders","paramsBatch":[[]],"writeConcern":{"level":"local","__proto__":{"timeoutMs":5}}}',
        400,
      ],
      ['query', '{"sql":"SELECT 1","readConcern":{"level":"local","constructor":{}}}', 400],
    ]

    for (const [route, body, status] of bodies) {
      const answer = await sendRaw(served.base, 'POST', `/db/orders/${route}`, body)
      expect(answer.status, `${route} ${body}`).toBe(status)
    }
    ws.sendText('{"__proto__":{"type":"execute"},"id":"proto-type","sql":"DELETE FROM orders"}')
    const protoType = await ws.next()
    const shadowed = await ws.request({ id: 'shadowed', type: 'query', sql: 'SELECT count(*) AS n FROM orders' })
    ws.sendText('{"type":"query","id":"proto-sql","sql":"SELECT 1 AS v","__proto__":{"sql":"DELETE FROM orders"}}')
    const protoSql = await ws.next()

    expect(protoType.error?.code).toBe('INVALID_MESSAGE')
    expect(shadowed.data).toEqual({ rows: [{ n: SEEDED_ORDERS + 2 }] })
    expect(protoSql.data).toEqual({ rows: [{ v: 1 }] })
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()
    expect(Object.hasOwn(Object.prototype, 'sql')).toBe(false)
    expect(Object.hasOwn(Object.prototype, 'type')).toBe(false)
    expect(customers()).toEqual([
      ...Array.from({ length: SEEDED_ORDERS }, (_, index) => `seed-${index}`),
      'from-proto',
      'from-batch',
    ])
  })

  it('answers every malformed WebSocket frame on the open connection and keeps the connection serving', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)
    const frames: Array<[frame: string, id: string, code: string]> = [
      ['DELETE FROM orders', '', 'INVALID_JSON'],
      ['{"type":"execute","id":"cut","sql":"DELETE FROM orders"', '', 'INVALID_JSON'],
      ['["execute","DELETE FROM orders"]', '', 'INVALID_MESSAGE'],
      ['5', '', 'INVALID_MESSAGE'],
      ['"execute"', '', 'INVALID_MESSAGE'],
      ['null', '', 'INVALID_MESSAGE'],
      ['{"id":"no-type","sql":"DELETE FROM orders"}', '', 'INVALID_MESSAGE'],
      ['{"type":5,"id":"numeric-type"}', '', 'INVALID_MESSAGE'],
      ['{"type":"execute","sql":"DELETE FROM orders"}', '', 'INVALID_MESSAGE'],
      ['{"type":"execute","id":42,"sql":"DELETE FROM orders"}', '42', 'INVALID_MESSAGE'],
      ['{"type":"execute","id":{"nested":1},"sql":"DELETE FROM orders"}', '', 'INVALID_MESSAGE'],
      ['{"type":"drop","id":"unknown"}', 'unknown', 'UNKNOWN_TYPE'],
      ['{"type":"QUERY","id":"upper","sql":"SELECT 1"}', 'upper', 'UNKNOWN_TYPE'],
      ['{"type":"","id":"blank"}', 'blank', 'UNKNOWN_TYPE'],
    ]

    for (const [frame, id, code] of frames) {
      ws.sendText(frame)
      const reply = await ws.next()
      expect([reply.type, reply.id, reply.error?.code], frame).toEqual(['error', id, code])
    }
    const answered = await ws.request({ id: 'after', type: 'query', sql: 'SELECT count(*) AS n FROM orders' })

    expect(answered.data).toEqual({ rows: [{ n: SEEDED_ORDERS }] })
    expect(countSqlite(path, 'orders')).toBe(SEEDED_ORDERS)
  })

  it('keeps every write that many clients send at once and shows each read a count between the first and the last', async () => {
    const counts: number[] = []
    const expected: string[] = []

    const overHttp = Array.from({ length: HTTP_CLIENTS }, async (_, client) => {
      const name = (row: number) => `http-${client}-${row}`
      expected.push(...Array.from({ length: ROWS_PER_CLIENT }, (_, row) => name(row)))
      const answers = await Promise.all([
        postJson(served.base, '/db/orders/execute', { sql: INSERT_ORDER, params: [name(0), client] }),
        postJson(served.base, '/db/orders/batch', {
          sql: INSERT_ORDER,
          paramsBatch: [1, 2, 3].map(row => [name(row), client]),
        }),
        postJson(served.base, '/db/orders/transaction', {
          statements: [4, 5].map(row => ({ sql: INSERT_NAMED, params: { customer: name(row), total: client } })),
        }),
        postJson(served.base, '/db/orders/query', { sql: 'SELECT count(*) AS n FROM orders' }),
      ])
      for (const answer of answers) expect(answer.status, answer.text).toBe(200)
      counts.push((answers[3]?.body as { rows: Array<{ n: number }> }).rows[0]?.n ?? -1)
    })

    const overWebSocket = Array.from({ length: WEBSOCKET_CLIENTS }, async (_, client) => {
      const name = (row: number) => `ws-${client}-${row}`
      expected.push(...Array.from({ length: ROWS_PER_CLIENT }, (_, row) => name(row)))
      const ws = await RawSocket.open(`${served.wsBase}/db/orders`)
      const replies = await Promise.all([
        ws.request({ id: 'one', type: 'execute', sql: INSERT_ORDER, params: [name(0), client] }),
        ws.request({
          id: 'many',
          type: 'batch',
          sql: INSERT_ORDER,
          paramsBatch: [1, 2, 3].map(row => [name(row), client]),
        }),
        ws.request({
          id: 'together',
          type: 'transaction',
          statements: [4, 5].map(row => ({ sql: INSERT_ORDER, params: [name(row), client] })),
        }),
        ws.request({ id: 'count', type: 'query', sql: 'SELECT count(*) AS n FROM orders' }),
      ])
      for (const reply of replies) expect(reply.type, JSON.stringify(reply)).toBe('result')
      counts.push((replies[3]?.data as { rows: Array<{ n: number }> }).rows[0]?.n ?? -1)
    })

    await Promise.all([...overHttp, ...overWebSocket])

    const final = SEEDED_ORDERS + (HTTP_CLIENTS + WEBSOCKET_CLIENTS) * ROWS_PER_CLIENT
    expect(countSqlite(path, 'orders')).toBe(final)
    expect(customers().slice(SEEDED_ORDERS).sort()).toEqual([...expected].sort())
    for (const count of counts) {
      expect(count).toBeGreaterThanOrEqual(SEEDED_ORDERS)
      expect(count).toBeLessThanOrEqual(final)
    }
    await stillServes()
  })
})
