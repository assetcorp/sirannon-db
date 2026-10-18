import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError, SirannonError } from '../../../core/errors.js'
import type { RequestContext } from '../../../core/types.js'
import {
  connectRaw,
  errorCodeOf,
  postJson,
  RawSocket,
  scratchDirectory,
  sendRaw,
  trackedRegistry,
  within,
} from '../lib/served-registry.js'
import {
  closeOf,
  gate,
  openOrders,
  rawUpgrade,
  releaseAccessServers,
  SEEDED_ORDERS,
  serveTyped,
  storedOrders,
  upgradeRequestText,
} from './served-access.js'

interface Caller {
  customer: string
}

const TICKET = 'x-ticket'
const OPEN_TICKET = 'open sesame'
const REFUSAL = { error: { code: 'TICKET_REFUSED', message: 'The ticket at the door was refused' } }
const DATABASE_ROUTES: Array<[method: string, path: string, body?: string]> = [
  ['POST', '/db/orders/query', '{"sql":"SELECT 1"}'],
  ['POST', '/db/orders/execute', '{"sql":"DELETE FROM orders"}'],
  ['POST', '/db/orders/transaction', '{"statements":[{"sql":"DELETE FROM orders"}]}'],
  ['POST', '/db/orders/batch', '{"sql":"DELETE FROM orders","paramsBatch":[[]]}'],
  ['POST', '/db/orders/load', '{"sql":"DELETE FROM orders","paramsBatch":[[]]}'],
  ['POST', '/db/orders/query/ordersOfCaller', '{}'],
  ['POST', '/db/orders/execute/clearCaller', '{}'],
  ['POST', '/db/orders/changes', '{}'],
  ['POST', '/db/orders/migrations', '{}'],
  ['POST', '/db/orders/snapshot', '{}'],
  ['POST', '/db/orders/snapshot/page', '{}'],
  ['POST', '/db/orders/backup', '{}'],
  ['GET', '/db/orders/backup'],
  ['GET', '/db/orders/backup/chain'],
  ['POST', '/db/orders/backup/verify', '{"name":"anything"}'],
  ['POST', '/db/orders/backup/safe-to-delete', '{}'],
  ['POST', '/db/orders/backup/restore', '{}'],
  ['GET', '/db/orders/backup/restore'],
  ['GET', '/db/orders/cluster'],
]
const OPEN_ROUTES = ['/health', '/health/ready', '/capabilities']

let dir: string

beforeEach(() => {
  dir = scratchDirectory('authenticate-hook')
})

afterEach(releaseAccessServers)

const callerOperations = {
  orders: {
    reads: {
      ordersOfCaller: {
        fromIdentity: { customer: 'customer' as const },
        statement: (args: Record<string, unknown>) => ({
          sql: 'SELECT id, customer FROM orders WHERE customer = ? ORDER BY id',
          params: [args.customer],
        }),
      },
    },
    writes: {
      clearCaller: {
        fromIdentity: { customer: 'customer' as const },
        statements: (args: Record<string, unknown>) => ({
          sql: 'DELETE FROM orders WHERE customer = ?',
          params: [args.customer],
        }),
      },
    },
  },
}

describe('the authenticate hook in front of every database route', () => {
  it('refuses each database route with the status, code, and message that the hook throws, and serves it once the hook returns an identity', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const seen: string[] = []
    const served = await serveTyped<Caller>(sirannon, {
      acceptSql: true,
      acceptDeviceSync: true,
      acceptBackupRestore: true,
      operations: callerOperations,
      getClusterStatus: databaseId => ({ databaseId, health: 'healthy', healthReason: 'in-sync' }),
      authorizeClusterStatus: () => true,
      authenticate: ctx => {
        seen.push(`${ctx.method} ${ctx.path}`)
        if (ctx.headers[TICKET] !== OPEN_TICKET)
          throw new RequestDeniedError(401, 'TICKET_REFUSED', REFUSAL.error.message)
        return { customer: 'nobody' }
      },
    })

    for (const [method, route, body] of DATABASE_ROUTES) {
      for (const ticket of [undefined, '', 'open  sesame', 'OPEN SESAME', `${OPEN_TICKET}!`]) {
        const headers: Record<string, string> = ticket === undefined ? {} : { [TICKET]: ticket }
        const answer = await sendRaw(served.base, method, route, body, headers)
        expect([answer.status, answer.body], `${method} ${route} with ${JSON.stringify(ticket)}`).toEqual([
          401,
          REFUSAL,
        ])
      }
    }
    expect(storedOrders(path)).toEqual(SEEDED_ORDERS)
    expect(seen).toHaveLength(DATABASE_ROUTES.length * 5)

    for (const route of OPEN_ROUTES) {
      const answer = await sendRaw(served.base, 'GET', route)
      expect(answer.status, route).toBe(200)
    }
    expect(seen).toHaveLength(DATABASE_ROUTES.length * 5)

    for (const [method, route, body] of DATABASE_ROUTES) {
      const answer = await sendRaw(served.base, method, route, body, { [TICKET]: OPEN_TICKET })
      expect([answer.status, errorCodeOf(answer.body)], `${method} ${route}`).not.toContainEqual('TICKET_REFUSED')
      expect(answer.status, `${method} ${route}`).not.toBe(401)
    }
    expect(seen.slice(DATABASE_ROUTES.length * 5)).toEqual(
      DATABASE_ROUTES.map(([method, route]) => `${method.toLowerCase()} ${route}`),
    )

    const refusedSocket = new WebSocket(`${served.wsBase}/db/orders`)
    expect((await within(closeOf(refusedSocket), 'the refused upgrade')).code).toBe(4401)
  })

  it('answers 500 HOOK_ERROR and leaks nothing when the hook returns a refusal object or throws something other than a Sirannon error', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const secret = 'postgres://admin:hunter2@10.0.0.9'
    const outcomes: Record<string, () => unknown> = {
      'a returned refusal object': () => ({ status: 401, code: 'NO_TICKET', message: 'No ticket' }),
      'an Error': () => {
        throw new Error(`could not reach ${secret}`)
      },
      'a TypeError from a bug': () => (undefined as unknown as { headers: string }).headers,
      'a string': () => {
        throw secret
      },
      null: () => {
        throw null
      },
      undefined: () => {
        throw undefined
      },
      'a number': () => {
        throw 401
      },
      'a symbol': () => {
        throw Symbol(secret)
      },
      'a rejected promise with no reason': () => Promise.reject(),
      'a rejected promise with an Error': () => Promise.reject(new Error(secret)),
      'an object whose getters throw': () => {
        throw Object.defineProperty({}, 'message', {
          get() {
            throw new Error(secret)
          },
        })
      },
    }
    let behaviour = 'a returned refusal object'
    const served = await serveTyped<Caller>(sirannon, {
      acceptSql: true,
      authenticate: () => {
        const outcome = outcomes[behaviour]
        if (!outcome) return { customer: 'ama' }
        return outcome() as Caller
      },
    })

    for (const label of Object.keys(outcomes)) {
      behaviour = label
      const answer = await postJson(served.base, '/db/orders/execute', { sql: 'DELETE FROM orders' })
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([500, 'HOOK_ERROR'])
      expect(answer.text, label).not.toContain('hunter2')
      expect(answer.text, label).not.toContain('10.0.0.9')
    }
    behaviour = 'a returned refusal object'
    const refusalAnswer = await postJson(served.base, '/db/orders/query', { sql: 'SELECT 1' })
    expect((refusalAnswer.body as { error: { message: string } }).error.message).toMatch(/throw/i)

    behaviour = 'served'
    const answer = await postJson(served.base, '/db/orders/query', { sql: 'SELECT count(*) AS n FROM orders' })
    expect(answer.body).toEqual({ rows: [{ n: SEEDED_ORDERS.length }] })
    expect(storedOrders(path)).toEqual(SEEDED_ORDERS)
  })

  it('answers an error that carries a status with that status and an error without one with 500 HOOK_ERROR, whatever the class of the error', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const thrown: Array<[label: string, error: unknown, status: number, code: string]> = [
      ['a plain object with a status', { status: 401, code: 'NO_TICKET', message: 'No ticket' }, 401, 'NO_TICKET'],
      ['an Error with a status', Object.assign(new Error('Expired'), { status: 403, code: 'EXPIRED' }), 403, 'EXPIRED'],
      ['a Sirannon error with no status', new SirannonError('Bad ticket', 'QUERY_ERROR'), 500, 'HOOK_ERROR'],
    ]
    let current: unknown
    const served = await serveTyped<Caller>(sirannon, {
      acceptSql: true,
      authenticate: () => {
        throw current
      },
    })

    for (const [label, error, status, code] of thrown) {
      current = error
      const answer = await postJson(served.base, '/db/orders/query', { sql: 'SELECT 1' })
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([status, code])
    }
  })

  it('awaits a slow hook, keeps answering other callers while one hook hangs, and survives the hung caller giving up', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const hung = gate()
    const served = await serveTyped<Caller>(sirannon, {
      operations: callerOperations,
      authenticate: async ctx => {
        if (ctx.headers['x-hang'] === 'yes') await hung.promise
        await new Promise(resolve => setTimeout(resolve, Number(ctx.headers['x-delay'] ?? 0)))
        const customer = ctx.headers['x-customer']
        if (customer === undefined) throw new RequestDeniedError(401, 'NO_CUSTOMER', 'Name the customer')
        return { customer }
      },
    })
    const abandoned = new AbortController()
    const hungRequest = fetch(`${served.base}/db/orders/query/ordersOfCaller`, {
      method: 'POST',
      headers: { 'x-hang': 'yes', 'x-customer': 'kofi' },
      signal: abandoned.signal,
    }).catch((err: unknown) => err)
    const hungUpgrade = await connectRaw(served.port)
    hungUpgrade.socket.write(upgradeRequestText('/db/orders', { 'x-hang': 'yes', 'x-customer': 'kofi' }))

    const customers = SEEDED_ORDERS.map(order => order.customer)
    const answers = await Promise.all(
      Array.from({ length: 24 }, (_, at) => {
        const customer = customers[at % customers.length] ?? 'ama'
        return sendRaw(served.base, 'POST', '/db/orders/query/ordersOfCaller', '', {
          'x-customer': customer,
          'x-delay': String((at * 7) % 25),
        }).then(answer => ({ customer, answer }))
      }),
    )
    for (const { customer, answer } of answers) {
      const expected = SEEDED_ORDERS.filter(order => order.customer === customer).map(order => ({
        id: order.id,
        customer,
      }))
      expect([answer.status, answer.body], customer).toEqual([200, { rows: expected }])
    }
    const refused = await sendRaw(served.base, 'POST', '/db/orders/query/ordersOfCaller', '')
    expect([refused.status, errorCodeOf(refused.body)]).toEqual([401, 'NO_CUSTOMER'])

    expect(hungUpgrade.received()).toBe('')
    abandoned.abort()
    hungUpgrade.socket.destroy()
    expect(await hungRequest).toBeInstanceOf(Error)
    await within(hungUpgrade.closed, 'the abandoned upgrade closing')
    hung.open()
    await new Promise(resolve => setTimeout(resolve, 50))

    const write = await sendRaw(served.base, 'POST', '/db/orders/execute/clearCaller', '', { 'x-customer': 'esi' })
    expect([write.status, write.body]).toEqual([200, { results: [{ changes: 1, lastInsertRowId: expect.anything() }] }])
    expect(storedOrders(path)).toEqual(SEEDED_ORDERS.filter(order => order.customer !== 'esi'))
  })

  it('hands the hook the method, the path without its query string, the database, the remote address, and every header in lower case', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const seen: RequestContext[] = []
    const served = await serveTyped<Caller>(sirannon, {
      acceptSql: true,
      authenticate: ctx => {
        seen.push(structuredClone(ctx))
        return undefined
      },
    })

    await sendRaw(served.base, 'POST', '/db/orders/query?debug=1&sql=DROP', '{"sql":"SELECT 1"}', {
      'X-Request-Trace': 'Trace-ABC',
      AUTHORIZATION: 'Bearer MixedCase',
    })
    await sendRaw(served.base, 'GET', '/db/orders/backup/chain')
    const socket = await RawSocket.open(`${served.wsBase}/db/orders`)
    await socket.request({ id: 'one', type: 'query', sql: 'SELECT 1 AS one' })

    expect(seen).toHaveLength(3)
    const [post, get, upgrade] = seen
    expect(post).toMatchObject({ method: 'post', path: '/db/orders/query', databaseId: 'orders' })
    expect(post?.headers).toMatchObject({ 'x-request-trace': 'Trace-ABC', authorization: 'Bearer MixedCase' })
    expect(Object.keys(post?.headers ?? {}).filter(name => name !== name.toLowerCase())).toEqual([])
    expect(get).toMatchObject({ method: 'get', path: '/db/orders/backup/chain', databaseId: 'orders' })
    expect(upgrade).toMatchObject({ method: 'get', path: '/db/orders', databaseId: 'orders' })
    expect(upgrade?.headers.upgrade?.toLowerCase()).toBe('websocket')
    for (const ctx of seen) expect(ctx.remoteAddress).toMatch(/127\.0\.0\.1|^(0+:){5}ffff:7f00:1$|::1/i)
  })

  it('calls the hook before it refuses an operation name that is not valid percent-encoding', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const seen: string[] = []
    const served = await serveTyped<Caller>(sirannon, {
      operations: callerOperations,
      authenticate: ctx => {
        seen.push(ctx.path)
        throw new RequestDeniedError(401, 'TICKET_REFUSED', REFUSAL.error.message)
      },
    })

    const answer = await sendRaw(served.base, 'POST', '/db/orders/query/orders%ZZ', '{}')

    expect([answer.status, answer.body]).toEqual([401, REFUSAL])
    expect(seen).toEqual(['/db/orders/query/orders%ZZ'])
  })

  it('hands the hook the decoded database identifier and refuses one that is not valid percent-encoding only once the hook accepts', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const seen: Array<string | undefined> = []
    const served = await serveTyped<Caller>(sirannon, {
      acceptSql: true,
      operations: callerOperations,
      authenticate: ctx => {
        seen.push(ctx.databaseId)
        if (ctx.headers[TICKET] !== OPEN_TICKET) {
          throw new RequestDeniedError(401, 'TICKET_REFUSED', REFUSAL.error.message)
        }
        return { customer: 'kofi' }
      },
    })
    const undecodable: Array<[method: string, path: string, body?: string]> = [
      ['POST', '/db/orders%ZZ/execute', '{"sql":"DELETE FROM orders"}'],
      ['POST', '/db/orders%ZZ/execute/clearCaller', '{}'],
      ['POST', '/db/orders%C3%28/backup', '{}'],
      ['GET', '/db/orders%ZZ/backup/chain'],
    ]

    for (const [method, route, body] of undecodable) {
      const refused = await sendRaw(served.base, method, route, body)
      const accepted = await sendRaw(served.base, method, route, body, { [TICKET]: OPEN_TICKET })
      expect([refused.status, refused.body], route).toEqual([401, REFUSAL])
      expect([accepted.status, errorCodeOf(accepted.body)], route).toEqual([400, 'INVALID_REQUEST'])
    }
    const refusedUpgrade = await rawUpgrade(served.port, '/db/orders%ZZ', {})
    const acceptedUpgrade = await rawUpgrade(served.port, '/db/orders%ZZ', { [TICKET]: OPEN_TICKET })
    const encoded = await sendRaw(served.base, 'POST', '/db/north%20wing/query', '{"sql":"SELECT 1"}', {
      [TICKET]: OPEN_TICKET,
    })

    expect(refusedUpgrade.statusLine).toMatch(/^HTTP\/1\.1 101/)
    expect(acceptedUpgrade.statusLine).toMatch(/^HTTP\/1\.1 400/)
    expect([encoded.status, errorCodeOf(encoded.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect(seen).toEqual([
      'orders%ZZ',
      'orders%ZZ',
      'orders%ZZ',
      'orders%ZZ',
      'orders%C3%28',
      'orders%C3%28',
      'orders%ZZ',
      'orders%ZZ',
      'orders%ZZ',
      'orders%ZZ',
      'north wing',
    ])
    expect(storedOrders(path)).toEqual(SEEDED_ORDERS)
  })
})
