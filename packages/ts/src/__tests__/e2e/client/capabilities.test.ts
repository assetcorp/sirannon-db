import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { OperationRegistry } from '../../../core/types.js'
import {
  DATABASE_ID,
  INSERT_ORDER,
  middlebox,
  recordingProxy,
  releaseClientServers,
  requestTally,
  type StoredOrder,
  servedShop,
  storedOrders,
} from './harness.js'

const everyOrder = operationRef<Record<string, never>, StoredOrder>('everyOrder')

const OPERATIONS: OperationRegistry = {
  [DATABASE_ID]: {
    reads: { everyOrder: { statement: () => ({ sql: 'SELECT id, customer, total FROM orders ORDER BY id' }) } },
  },
}

const STATEMENT_ROUTE = /^\/db\/[^/]+\/(query|execute|transaction|batch|load)$/

afterEach(releaseClientServers)

function codeOf(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    () => 'resolved',
    (err: { code?: string }) => err.code ?? 'none',
  )
}

function everyStatementShape(client: SirannonClient): Array<Promise<string>> {
  const db = client.database(DATABASE_ID)
  return [
    codeOf(db.query('SELECT * FROM orders')),
    codeOf(db.execute('DELETE FROM orders')),
    codeOf(db.transaction([{ sql: 'DELETE FROM orders' }])),
    codeOf(db.batch(INSERT_ORDER, [['lost', 1]])),
    codeOf(db.load(INSERT_ORDER, [['lost', 1]])),
    codeOf(db.loadAll(INSERT_ORDER, [['lost', 1]])),
  ]
}

describe('what a client learns from GET /capabilities', () => {
  it('keeps every statement inside the client when the server accepts no SQL, and still runs registered reads', async () => {
    const tally = requestTally()
    const shop = await servedShop({
      server: { acceptSql: false, operations: OPERATIONS, authenticate: tally.authenticate },
    })
    const proxy = await recordingProxy(shop.served.port)
    const box = await middlebox(shop.served.port)
    const before = storedOrders(shop.path)
    const overHttp = new SirannonClient(proxy.base, { transport: 'http' })
    const overWebSocket = new SirannonClient(box.base, { transport: 'websocket', autoReconnect: false })

    const httpCodes = await Promise.all(everyStatementShape(overHttp))
    const webSocketCodes = await Promise.all(everyStatementShape(overWebSocket))
    const statementsReachingServer = tally.requests.length + tally.upgrades.length
    const named = await overWebSocket.database(DATABASE_ID).query(everyOrder, {})
    overHttp.close()
    overWebSocket.close()

    expect(httpCodes).toEqual(Array(6).fill('SQL_NOT_ACCEPTED'))
    expect(webSocketCodes).toEqual(Array(6).fill('SQL_NOT_ACCEPTED'))
    expect(statementsReachingServer).toBe(0)
    expect(proxy.requests.map(request => `${request.method} ${request.path}`)).toEqual(['GET /capabilities'])
    expect(box.messages('client').filter(message => typeof message.sql === 'string')).toEqual([])
    expect(named).toEqual(before)
    expect(storedOrders(shop.path)).toEqual(before)
  })

  it('reads the answer once for each client and reads it again after a read that failed', async () => {
    const shop = await servedShop()
    const proxy = await recordingProxy(shop.served.port)
    const capabilityReads = () => proxy.requestsTo('GET', /^\/capabilities$/).length
    let dropsLeft = 1
    proxy.answerWith(request => {
      if (request.path !== '/capabilities' || dropsLeft === 0) return undefined
      dropsLeft -= 1
      return { kind: 'drop' }
    })
    const first = new SirannonClient(proxy.base, { transport: 'http' })
    const db = first.database(DATABASE_ID)

    const failed = await codeOf(db.execute(INSERT_ORDER, ['after-drop', 1]))
    const readsAfterFailure = capabilityReads()
    for (let index = 0; index < 20; index++) await db.execute(INSERT_ORDER, [`first-${index}`, index])
    await db.query('SELECT count(*) AS n FROM orders')
    await db.transaction([{ sql: INSERT_ORDER, params: ['first-tx', 1] }])
    const readsByFirstClient = capabilityReads()
    const second = new SirannonClient(proxy.base, { transport: 'http' })
    await second.database(DATABASE_ID).query('SELECT 1 AS one')
    await second.database(DATABASE_ID).query('SELECT 2 AS two')
    first.close()
    second.close()

    expect(failed).toBe('CONNECTION_ERROR')
    expect(readsAfterFailure).toBe(1)
    expect(readsByFirstClient).toBe(2)
    expect(capabilityReads()).toBe(3)
    expect(proxy.requestsTo('POST', STATEMENT_ROUTE)).toHaveLength(24)
    expect(storedOrders(shop.path).map(order => order.customer)).not.toContain('after-drop')
  })

  it('keeps SQL inside the client when /capabilities answers 404, while registered reads and live queries still run', async () => {
    const shop = await servedShop({ server: { operations: OPERATIONS } })
    const proxy = await recordingProxy(shop.served.port)
    proxy.answerWith(request =>
      request.path === '/capabilities'
        ? { kind: 'reply', status: 404, text: '{"error":{"code":"NOT_FOUND","message":"Not found"}}' }
        : undefined,
    )
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const overWebSocket = new SirannonClient(proxy.base, { autoReconnect: false })

    const codes = await Promise.all(everyStatementShape(client))
    const named = await client.database(DATABASE_ID).query(everyOrder, {})
    const live = await overWebSocket.database(DATABASE_ID).live(everyOrder, {})
    const liveState = live.getState()
    client.close()
    overWebSocket.close()

    expect(codes).toEqual(Array(6).fill('SQL_NOT_ACCEPTED'))
    expect(liveState).toEqual({ status: 'ready', revalidating: false, rows: storedOrders(shop.path) })
    expect(proxy.requestsTo('POST', STATEMENT_ROUTE)).toEqual([])
    expect(named).toEqual(storedOrders(shop.path))
  })

  it('raises INVALID_RESPONSE for a malformed answer and CONNECTION_ERROR for one that never arrives, sending no statement', async () => {
    const shop = await servedShop()
    const proxy = await recordingProxy(shop.served.port)
    const answers: Array<[label: string, text: string | null, code: string]> = [
      ['a token list that is a string', '{"capabilities":"query.sql"}', 'INVALID_RESPONSE'],
      ['a token that is a number', '{"capabilities":["query.sql",7]}', 'INVALID_RESPONSE'],
      ['a digest that is a number', '{"capabilities":["query.sql"],"registry":{"digest":5}}', 'INVALID_RESPONSE'],
      ['a body that is not JSON', 'query.sql', 'INVALID_RESPONSE'],
      ['a list in place of the report', '["query.sql"]', 'INVALID_RESPONSE'],
      ['no answer within the request timeout', null, 'CONNECTION_ERROR'],
    ]

    for (const [label, text, code] of answers) {
      proxy.answerWith(request => {
        if (request.path !== '/capabilities') return undefined
        return text === null ? { kind: 'stall' } : { kind: 'reply', status: 200, text }
      })
      const client = new SirannonClient(proxy.base, { transport: 'http', requestTimeout: 250 })
      const started = Date.now()
      expect(await codeOf(client.database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1])), label).toBe(code)
      expect(Date.now() - started, label).toBeLessThan(5_000)
      client.close()
    }
    proxy.answerWith(request =>
      request.path === '/capabilities'
        ? { kind: 'reply', status: 200, text: '{"capabilities":["unknown.token","query.sql",""],"extra":{"a":1}}' }
        : undefined,
    )
    const tolerant = new SirannonClient(proxy.base, { transport: 'http' })
    await tolerant.database(DATABASE_ID).execute(INSERT_ORDER, ['kept', 1])
    tolerant.close()

    expect(proxy.requestsTo('POST', STATEMENT_ROUTE)).toHaveLength(1)
    expect(storedOrders(shop.path).map(order => order.customer)).toEqual(['ama', 'kofi', 'kept'])
  })
})
