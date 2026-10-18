import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { operationRef } from '../../../core/operation-registry.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  readSqlite,
  scratchDirectory,
  sendRaw,
  trackedRegistry,
} from '../lib/served-registry.js'
import { LEAKED_DETAIL, ordersOperations, type Tenant, tenantFromHeaders } from './orders-operations.js'
import {
  type Order,
  openOrders,
  openSocketWithHeaders,
  releaseAccessServers,
  SEEDED_ORDERS,
  serveTyped,
  storedOrders,
} from './served-access.js'

const PLACE_ORDER = operationRef<{ customer: string; total: number }>('placeOrder')
const ORDERS_OF_TENANT = operationRef<Record<string, never>, { id: number; customer: string; total: number }>(
  'ordersOfTenant',
)
const HTTP_CLIENTS = 20
const WEBSOCKET_CLIENTS = 10
const ORDERS_PER_CLIENT = 5
const TOTAL_PAST_SAFE_INTEGERS = 2n ** 62n + 1n

let dir: string

beforeEach(() => {
  dir = scratchDirectory('named-calls')
})

afterEach(releaseAccessServers)

function ofTenant(orders: readonly Order[], tenant: string) {
  return orders
    .filter(order => order.tenant === tenant)
    .map(order => ({ id: order.id, customer: order.customer, total: order.total }))
}

describe('registered operations called by name', () => {
  it('executes each registered read and write over HTTP, WebSocket, and the published client, leaving the file as the calls describe', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, {
      acceptSql: true,
      operations: ordersOperations,
      authenticate: tenantFromHeaders,
    })
    const model: Order[] = SEEDED_ORDERS.map(order => ({ ...order }))
    const place = (customer: string, total: number, tenant: string) => {
      model.push({ id: model.length + 1, customer, total, tenant })
      return model.length
    }

    const httpWrite = await sendRaw(
      served.base,
      'POST',
      '/db/orders/execute/placeOrder',
      JSON.stringify({ args: { customer: 'yaw', total: 60 } }),
      { 'x-tenant': 'globex' },
    )
    const httpId = place('yaw', 60, 'globex')
    const socket = await openSocketWithHeaders(`${served.wsBase}/db/orders`, { 'x-tenant': 'initech' })
    const wsWrite = await socket.request({
      id: 'w',
      type: 'execute',
      name: 'placeOrder',
      args: { customer: 'adjoa', total: 15 },
    })
    const wsId = place('adjoa', 15, 'initech')
    const wsRead = await socket.request({ id: 'r', type: 'query', name: 'ordersOfTenant' })
    const httpRead = await sendRaw(served.base, 'POST', '/db/orders/query/ordersOfTenant', undefined, {
      'x-tenant': 'globex',
    })

    expect([httpWrite.status, httpWrite.body]).toEqual([200, { results: [{ changes: 1, lastInsertRowId: httpId }] }])
    expect(wsWrite).toMatchObject({ type: 'result', data: { results: [{ changes: 1, lastInsertRowId: wsId }] } })
    expect(wsRead).toMatchObject({ type: 'result', data: { rows: ofTenant(model, 'initech') } })
    expect([httpRead.status, httpRead.body]).toEqual([200, { rows: ofTenant(model, 'globex') }])
    expect(storedOrders(path)).toEqual(model)
    const statement = await postJson(served.base, '/db/orders/query', { sql: 'SELECT count(*) AS n FROM orders' })
    expect(statement.body).toEqual({ rows: [{ n: model.length }] })

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(served.base, {
        transport,
        autoReconnect: false,
        headers: { 'x-tenant': `tenant-${transport}` },
      })
      const orders = client.database('orders')
      const results = await orders.execute(PLACE_ORDER, { customer: `by-${transport}`, total: 7 })
      const id = place(`by-${transport}`, 7, `tenant-${transport}`)
      expect(results, transport).toEqual([{ changes: 1, lastInsertRowId: id }])
      expect(await orders.query(ORDERS_OF_TENANT, {}), transport).toEqual(ofTenant(model, `tenant-${transport}`))
      expect(await orders.query(ORDERS_OF_TENANT, {}), transport).toEqual(
        ofTenant(storedOrders(path), `tenant-${transport}`),
      )
      client.close()
    }
    expect(storedOrders(path)).toEqual(model)

    const bigWrite = await postJson(served.base, '/db/orders/execute/placeOrder', {
      args: { customer: 'big', total: { __sirannon_int: TOTAL_PAST_SAFE_INTEGERS.toString() } },
    })
    const bigRead = await postJson(served.base, '/db/orders/query/ordersOfCustomer', { args: { customer: 'big' } })
    const [stored] = readSqlite<{ total: bigint }>(path, "SELECT total FROM orders WHERE customer = 'big'")
    expect(bigWrite.status).toBe(200)
    expect(stored?.total).toBe(TOTAL_PAST_SAFE_INTEGERS)
    expect(bigRead.body).toEqual({
      rows: [{ id: model.length + 1, customer: 'big', total: { __sirannon_int: TOTAL_PAST_SAFE_INTEGERS.toString() } }],
    })
  })

  it('executes every statement of a write in one transaction, and refuses a write or read whose statements fail, throw, or come back empty', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, { operations: ordersOperations, authenticate: tenantFromHeaders })
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)

    const moved = await postJson(served.base, '/db/orders/execute/transfer', {
      args: { from: 'ama', to: 'kofi', amount: 20 },
    })
    const afterMove = storedOrders(path)
    expect([moved.status, (moved.body as { results: unknown[] }).results]).toEqual([
      200,
      [
        { changes: 1, lastInsertRowId: expect.anything() },
        { changes: 1, lastInsertRowId: expect.anything() },
      ],
    ])
    expect(afterMove.map(order => [order.customer, order.total])).toEqual([
      ['ama', 100],
      ['kofi', 100],
      ['esi', 45],
    ])

    const refusals: Array<[route: string, name: string, args: Record<string, unknown>, status: number, code: string]> =
      [
        ['execute', 'transfer', { from: 'esi', to: 'kofi', amount: 46 }, 400, 'QUERY_ERROR'],
        ['execute', 'emptyWrite', {}, 500, 'INTERNAL_ERROR'],
        ['query', 'refusingRead', {}, 403, 'NOT_TODAY'],
        ['query', 'brokenRead', {}, 500, 'INTERNAL_ERROR'],
      ]
    for (const [index, [route, name, args, status, code]] of refusals.entries()) {
      const answer = await postJson(served.base, `/db/orders/${route}/${name}`, { args })
      const reply = await ws.request({ id: `refused-${index}`, type: route, name, args })
      expect([answer.status, errorCodeOf(answer.body)], name).toEqual([status, code])
      expect([reply.type, reply.error?.code], name).toEqual(['error', code])
      expect(answer.text, name).not.toContain(LEAKED_DETAIL)
      expect(JSON.stringify(reply), name).not.toContain(LEAKED_DETAIL)
    }
    expect(storedOrders(path)).toEqual(afterMove)
    expect(ws.open).toBe(true)
  })

  it('keeps every named write that many clients send at once over both transports', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, { operations: ordersOperations, authenticate: tenantFromHeaders })
    const expected: string[] = []

    const overHttp = Array.from({ length: HTTP_CLIENTS }, async (_, client) => {
      const names = Array.from({ length: ORDERS_PER_CLIENT }, (_, at) => `http-${client}-${at}`)
      expected.push(...names)
      const answers = await Promise.all(
        names.map(customer =>
          sendRaw(
            served.base,
            'POST',
            '/db/orders/execute/placeOrder',
            JSON.stringify({ args: { customer, total: client } }),
            { 'x-tenant': `http-${client}` },
          ),
        ),
      )
      for (const answer of answers) expect(answer.status, answer.text).toBe(200)
    })
    const overWebSocket = Array.from({ length: WEBSOCKET_CLIENTS }, async (_, client) => {
      const names = Array.from({ length: ORDERS_PER_CLIENT }, (_, at) => `ws-${client}-${at}`)
      expected.push(...names)
      const socket = await openSocketWithHeaders(`${served.wsBase}/db/orders`, { 'x-tenant': `ws-${client}` })
      const replies = await Promise.all(
        names.map((customer, at) =>
          socket.request({ id: `w-${at}`, type: 'execute', name: 'placeOrder', args: { customer, total: client } }),
        ),
      )
      for (const reply of replies) expect(reply.type, JSON.stringify(reply)).toBe('result')
    })
    await Promise.all([...overHttp, ...overWebSocket])

    const stored = storedOrders(path)
    expect(stored).toHaveLength(SEEDED_ORDERS.length + (HTTP_CLIENTS + WEBSOCKET_CLIENTS) * ORDERS_PER_CLIENT)
    expect(
      stored
        .slice(SEEDED_ORDERS.length)
        .map(order => order.customer)
        .sort(),
    ).toEqual([...expected].sort())
    for (const order of stored.slice(SEEDED_ORDERS.length)) {
      expect(order.tenant, order.customer).toBe(order.customer.split('-').slice(0, 2).join('-'))
    }
  })
})
