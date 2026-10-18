import net from 'node:net'
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
const UNANSWERED_REQUEST_TIMEOUT_MS = 60_000
const PROMPT_REFUSAL_MS = 1_000
const SOCKET_END_DEADLINE_MS = 2_000

interface StallingListener {
  readonly base: string
  readonly requestLines: string[]
  readonly requested: Promise<void>
  readonly ended: Promise<void>
  close(): Promise<void>
}

const listeners: StallingListener[] = []

afterEach(async () => {
  for (const listener of listeners.splice(0)) await listener.close()
  await releaseClientServers()
})

async function stallingListener(): Promise<StallingListener> {
  const sockets = new Set<net.Socket>()
  const requestLines: string[] = []
  let markRequested: () => void = () => {}
  let markEnded: () => void = () => {}
  const requested = new Promise<void>(resolve => {
    markRequested = resolve
  })
  const ended = new Promise<void>(resolve => {
    markEnded = resolve
  })
  const server = net.createServer(socket => {
    sockets.add(socket)
    let text = ''
    socket.on('error', () => {})
    socket.on('data', chunk => {
      text += chunk.toString('latin1')
      if (!text.includes('\r\n\r\n')) return
      requestLines.push(text.slice(0, text.indexOf('\r\n')))
      markRequested()
    })
    socket.on('close', () => {
      sockets.delete(socket)
      markEnded()
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  const port = address !== null && typeof address === 'object' ? address.port : 0
  const listener: StallingListener = {
    base: `http://127.0.0.1:${port}`,
    requestLines,
    requested,
    ended,
    close: async () => {
      for (const socket of sockets) socket.destroy()
      await new Promise<void>(resolve => server.close(() => resolve()))
    },
  }
  listeners.push(listener)
  return listener
}

function settlesWithin(promise: Promise<unknown>, deadlineMs: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const expired = new Promise<boolean>(resolve => {
    timer = setTimeout(() => resolve(false), deadlineMs)
  })
  return Promise.race([promise.then(() => true), expired]).finally(() => clearTimeout(timer))
}

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

  it('sends statements to a server that predates GET /capabilities, reading the 404 once for each client, while registered reads and live queries still run', async () => {
    const shop = await servedShop({ server: { operations: OPERATIONS } })
    const proxy = await recordingProxy(shop.served.port)
    proxy.answerWith(request =>
      request.path === '/capabilities'
        ? { kind: 'reply', status: 404, text: '{"error":{"code":"NOT_FOUND","message":"Not found"}}' }
        : undefined,
    )
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const overWebSocket = new SirannonClient(proxy.base, { autoReconnect: false })
    const before = storedOrders(shop.path)

    await client.database(DATABASE_ID).execute(INSERT_ORDER, ['over-http', 3])
    await overWebSocket.database(DATABASE_ID).execute(INSERT_ORDER, ['over-websocket', 4])
    await client.database(DATABASE_ID).transaction([{ sql: INSERT_ORDER, params: ['in-transaction', 5] }])
    const counted = await overWebSocket.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    const named = await client.database(DATABASE_ID).query(everyOrder, {})
    const live = await overWebSocket.database(DATABASE_ID).live(everyOrder, {})
    const liveState = live.getState()
    client.close()
    overWebSocket.close()

    const stored = storedOrders(shop.path)
    expect(stored.map(order => order.customer)).toEqual([
      ...before.map(order => order.customer),
      'over-http',
      'over-websocket',
      'in-transaction',
    ])
    expect(counted).toEqual([{ n: stored.length }])
    expect(named).toEqual(stored)
    expect(liveState).toEqual({ status: 'ready', revalidating: false, rows: stored })
    expect(proxy.requestsTo('POST', STATEMENT_ROUTE)).toHaveLength(2)
    expect(proxy.requestsTo('GET', /^\/capabilities$/)).toHaveLength(2)
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

  it('fails a statement that waits on an unanswered capability read with TRANSPORT_ERROR as soon as the database closes, and ends that read', async () => {
    const stalled = await stallingListener()
    const client = new SirannonClient(stalled.base, {
      transport: 'http',
      requestTimeout: UNANSWERED_REQUEST_TIMEOUT_MS,
    })
    const db = client.database(DATABASE_ID)

    const waiting = codeOf(db.execute(INSERT_ORDER, ['lost', 1]))
    await stalled.requested
    const closedAt = Date.now()
    db.close()
    const code = await waiting
    const refusedAfterMs = Date.now() - closedAt
    const socketEnded = await settlesWithin(stalled.ended, SOCKET_END_DEADLINE_MS)
    const later = await codeOf(db.query('SELECT 1 AS one'))
    client.close()

    expect(code).toBe('TRANSPORT_ERROR')
    expect(refusedAfterMs).toBeLessThan(PROMPT_REFUSAL_MS)
    expect(socketEnded).toBe(true)
    expect(later).toBe('TRANSPORT_ERROR')
    expect(stalled.requestLines).toEqual(['GET /capabilities HTTP/1.1'])
  })
})
