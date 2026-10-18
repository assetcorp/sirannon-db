import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { RemoteError } from '../../../client/types.js'
import { RequestDeniedError } from '../../../core/errors.js'
import { operationRef } from '../../../core/operation-registry.js'
import {
  readBearerToken,
  readSubprotocolCredential,
  toSubprotocolCredential,
} from '../../../core/request-credentials.js'
import type { OperationRegistry } from '../../../core/types.js'
import { httpRequestText, scratchDirectory, trackedRegistry, within } from '../lib/served-registry.js'
import {
  closeOf,
  openOrders,
  rawHttp,
  rawUpgrade,
  releaseAccessServers,
  SEEDED_ORDERS,
  serveTyped,
} from './served-access.js'

interface Caller {
  customer: string
}

const PREFIX = 'sirannon.ticket.'
const ORDERS_OF_CALLER = operationRef<Record<string, never>, { id: number; customer: string }>('ordersOfCaller')
const OVERSIZED_BYTES = 256 * 1024

const operations: OperationRegistry<Caller> = {
  orders: {
    reads: {
      ordersOfCaller: {
        fromIdentity: { customer: 'customer' },
        statement: args => ({
          sql: 'SELECT id, customer FROM orders WHERE customer = ? ORDER BY id',
          params: [args.customer],
        }),
      },
    },
  },
}

function ordersOf(customer: string): Array<{ id: number; customer: string }> {
  return SEEDED_ORDERS.filter(order => order.customer === customer).map(order => ({ id: order.id, customer }))
}

function namedRead(headers: Record<string, string>): string {
  return httpRequestText('POST', '/db/orders/query/ordersOfCaller', '', headers)
}

async function failureOf(promise: Promise<unknown>): Promise<RemoteError> {
  try {
    await promise
  } catch (err) {
    if (err instanceof RemoteError) return err
    throw err
  }
  throw new Error('the call succeeded where it had to fail')
}

let dir: string

beforeEach(() => {
  dir = scratchDirectory('credentials')
})

afterEach(releaseAccessServers)

describe('credentials that a caller presents to the authenticate hook', () => {
  it('reads a bearer token whatever the casing of the header name and the scheme, and refuses every malformed or oversized one', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const tokens = new Map([
      ['tok-ama', 'ama'],
      ['tok-kofi', 'kofi'],
    ])
    const served = await serveTyped<Caller>(sirannon, {
      operations,
      authenticate: ctx => {
        const customer = tokens.get(readBearerToken(ctx) ?? '')
        if (customer === undefined) throw new RequestDeniedError(401, 'BAD_TOKEN', 'The bearer token was refused')
        return { customer }
      },
    })
    const accepted: Array<[name: string, value: string, customer: string]> = [
      ['Authorization', 'Bearer tok-ama', 'ama'],
      ['authorization', 'Bearer tok-kofi', 'kofi'],
      ['AUTHORIZATION', 'Bearer tok-ama', 'ama'],
      ['aUtHoRiZaTiOn', 'Bearer tok-kofi', 'kofi'],
      ['Authorization', 'bearer tok-ama', 'ama'],
      ['Authorization', 'BEARER tok-kofi', 'kofi'],
      ['Authorization', 'Bearer   tok-ama', 'ama'],
    ]
    const refused: Array<[name: string, value: string]> = [
      ['Authorization', 'Basic dG9rLWFtYQ=='],
      ['Authorization', 'Bearer'],
      ['Authorization', 'Bearer '],
      ['Authorization', 'Bearertok-ama'],
      ['Authorization', 'Token tok-ama'],
      ['Authorization', 'tok-ama'],
      ['Authorization', 'Bearer tok-ama2'],
      ['Authorization', 'Bearer TOK-AMA'],
      ['Proxy-Authorization', 'Bearer tok-ama'],
      ['X-Authorization', 'Bearer tok-ama'],
    ]

    for (const [name, value, customer] of accepted) {
      const answer = await rawHttp(served.port, namedRead({ [name]: value }))
      expect([answer.status, answer.body], `${name}: ${value}`).toEqual([200, { rows: ordersOf(customer) }])
    }
    for (const [name, value] of refused) {
      const answer = await rawHttp(served.port, namedRead({ [name]: value }))
      expect([answer.status, answer.body], `${name}: ${value}`).toEqual([
        401,
        { error: { code: 'BAD_TOKEN', message: 'The bearer token was refused' } },
      ])
    }
    const oversized = await rawHttp(served.port, namedRead({ Authorization: `Bearer ${'x'.repeat(OVERSIZED_BYTES)}` }))
    expect(oversized.status === 0 || oversized.status >= 400, `answered ${oversized.status}`).toBe(true)

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(served.base, {
        transport,
        autoReconnect: false,
        headers: { Authorization: 'Bearer tok-kofi' },
      })
      expect(await client.database('orders').query(ORDERS_OF_CALLER, {}), transport).toEqual(ordersOf('kofi'))
      client.close()
    }
  })

  it('carries a credential of any characters in a WebSocket subprotocol through the published client, and selects only sirannon.v1 in reply', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const tickets = new Map([
      ['ticket for device 7, issued at 09:00+01:00', 'ama'],
      ['jeton-café-🔑-日本', 'kofi'],
      ['"quoted"; \r\nset-cookie: injected=1', 'esi'],
    ])
    const offers: string[] = []
    const decodedTickets: Array<string | undefined> = []
    const served = await serveTyped<Caller>(sirannon, {
      operations,
      authenticate: ctx => {
        offers.push(ctx.headers['sec-websocket-protocol'] ?? '')
        const decoded = readSubprotocolCredential(ctx, PREFIX)
        decodedTickets.push(decoded)
        const customer = tickets.get(decoded ?? '')
        if (customer === undefined) throw new RequestDeniedError(401, 'BAD_TICKET', 'The ticket was refused')
        return { customer }
      },
    })

    for (const [ticket, customer] of tickets) {
      const offered = `${PREFIX}${Buffer.from(ticket, 'utf8').toString('base64url')}`
      expect(toSubprotocolCredential(PREFIX, ticket), ticket).toBe(offered)
      const client = new SirannonClient(served.base, { autoReconnect: false, webSocketProtocols: [offered] })
      expect(await client.database('orders').query(ORDERS_OF_CALLER, {}), ticket).toEqual(ordersOf(customer))
      client.close()
      expect(offers.at(-1), ticket).toBe(`sirannon.v1, ${offered}`)
      expect(decodedTickets.at(-1), ticket).toBe(ticket)

      const upgrade = await rawUpgrade(served.port, '/db/orders', {
        'Sec-WebSocket-Protocol': `sirannon.v1, ${offered}`,
      })
      expect(upgrade.statusLine, ticket).toBe('HTTP/1.1 101 Switching Protocols')
      expect(upgrade.headers.get('sec-websocket-protocol'), ticket).toBe('sirannon.v1')
      expect([...upgrade.headers.values()].join('\n'), ticket).not.toContain(offered.slice(PREFIX.length))
    }

    const callsBefore = offers.length
    const onlyTicket = await rawUpgrade(served.port, '/db/orders', {
      'Sec-WebSocket-Protocol': toSubprotocolCredential(PREFIX, 'jeton-café-🔑-日本'),
    })
    expect(onlyTicket.statusLine).toMatch(/^HTTP\/1\.1 400/)
    expect(offers).toHaveLength(callsBefore)

    const undecodable = [`${PREFIX}!!!!`, `${PREFIX}__8`, `${PREFIX}`, 'sirannon.ticket', `x${PREFIX}YW1h`]
    for (const offered of undecodable) {
      const client = new SirannonClient(served.base, { autoReconnect: false, webSocketProtocols: [offered] })
      const failure = await failureOf(client.database('orders').query(ORDERS_OF_CALLER, {}))
      expect([failure.code, failure.message], offered).toEqual([
        'UNAUTHORIZED',
        expect.stringContaining('BAD_TICKET: The ticket was refused'),
      ])
      client.close()
    }

    const oversizedTicket = toSubprotocolCredential(PREFIX, 'z'.repeat(OVERSIZED_BYTES))
    const oversizedClient = new SirannonClient(served.base, {
      autoReconnect: false,
      webSocketProtocols: [oversizedTicket],
    })
    const oversized = await failureOf(oversizedClient.database('orders').query(ORDERS_OF_CALLER, {}))
    expect(oversized.code).toBe('CONNECTION_ERROR')
    oversizedClient.close()

    for (const unusable of [['two words'], ['comma,separated'], [''], ['dup', 'dup'], ['"quoted"']]) {
      const secret = unusable[0] ?? ''
      let thrown: unknown
      try {
        new SirannonClient(served.base, { webSocketProtocols: unusable })
      } catch (err) {
        thrown = err
      }
      expect(thrown, JSON.stringify(unusable)).toBeInstanceOf(RemoteError)
      expect((thrown as RemoteError).code).toBe('INVALID_ARGUMENT')
      if (secret.length > 3) expect((thrown as RemoteError).message).not.toContain(secret)
    }

    const client = new SirannonClient(served.base, {
      autoReconnect: false,
      webSocketProtocols: [toSubprotocolCredential(PREFIX, 'jeton-café-🔑-日本')],
    })
    expect(await client.database('orders').query(ORDERS_OF_CALLER, {})).toEqual(ordersOf('kofi'))
    client.close()
  })

  it('closes a refused upgrade with 4401 or 4403 and the reason that the hook gave, and the published client never retries it', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const longMessage = (padding: number) => `${'x'.repeat(padding)}${'🔑'.repeat(60)}`
    const refusals: Record<string, () => never> = {
      'verbose-0': () => {
        throw new RequestDeniedError(401, 'NO_TICKET', longMessage(0))
      },
      'verbose-1': () => {
        throw new RequestDeniedError(401, 'NO_TICKET', longMessage(1))
      },
      'verbose-2': () => {
        throw new RequestDeniedError(401, 'NO_TICKET', longMessage(2))
      },
      'verbose-3': () => {
        throw new RequestDeniedError(401, 'NO_TICKET', longMessage(3))
      },
      broken: () => {
        throw new RequestDeniedError(403, 'TICKET_BROKEN', 'half a pair \ud83d here')
      },
      missing: () => {
        throw new RequestDeniedError(401, 'NO_TICKET', 'Present a ticket')
      },
      revoked: () => {
        throw new RequestDeniedError(403, 'TICKET_REVOKED', 'That ticket was revoked at 09:00')
      },
      throttled: () => {
        throw new RequestDeniedError(429, 'SLOW_DOWN', 'Too many tickets')
      },
    }
    let calls = 0
    const served = await serveTyped<Caller>(sirannon, {
      operations,
      authenticate: ctx => {
        calls++
        const refusal = refusals[readSubprotocolCredential(ctx, PREFIX) ?? 'missing']
        if (refusal) refusal()
        return { customer: 'ama' }
      },
    })

    const cases: Array<[ticket: string, code: number, clientCode: string, reason: string]> = [
      ['missing', 4401, 'UNAUTHORIZED', 'NO_TICKET: Present a ticket'],
      ['revoked', 4403, 'FORBIDDEN', 'TICKET_REVOKED: That ticket was revoked at 09:00'],
      ['broken', 4403, 'FORBIDDEN', 'TICKET_BROKEN: half a pair \u{FFFD} here'],
    ]
    for (const [ticket, code, clientCode, reason] of cases) {
      const raw = new WebSocket(`${served.wsBase}/db/orders`, ['sirannon.v1', toSubprotocolCredential(PREFIX, ticket)])
      expect(await within(closeOf(raw), `the ${ticket} close`), ticket).toEqual({ code, reason })

      const client = new SirannonClient(served.base, {
        reconnectInterval: 10,
        webSocketProtocols: [toSubprotocolCredential(PREFIX, ticket)],
      })
      const before = calls
      const failure = await failureOf(client.database('orders').query(ORDERS_OF_CALLER, {}))
      await new Promise(resolve => setTimeout(resolve, 150))
      expect([failure.code, failure.message], ticket).toEqual([clientCode, expect.stringContaining(reason)])
      expect(calls - before, ticket).toBe(1)
      client.close()
    }

    for (const padding of [0, 1, 2, 3]) {
      const verbose = new WebSocket(`${served.wsBase}/db/orders`, [
        'sirannon.v1',
        toSubprotocolCredential(PREFIX, `verbose-${padding}`),
      ])
      const closed = await within(closeOf(verbose), 'the verbose close')
      expect(closed.code, `${padding}`).toBe(4401)
      expect(Buffer.byteLength(closed.reason), `${padding}`).toBeLessThanOrEqual(123)
      expect(Buffer.byteLength(closed.reason), `${padding}`).toBeGreaterThan(115)
      expect(`NO_TICKET: ${longMessage(padding)}`.startsWith(closed.reason), `${padding}: ${closed.reason}`).toBe(true)
    }

    const throttled = await rawUpgrade(served.port, '/db/orders', {
      'Sec-WebSocket-Protocol': `sirannon.v1, ${toSubprotocolCredential(PREFIX, 'throttled')}`,
    })
    expect(throttled.statusLine).toMatch(/^HTTP\/1\.1 429/)

    const accepted = new SirannonClient(served.base, {
      autoReconnect: false,
      webSocketProtocols: [toSubprotocolCredential(PREFIX, 'welcome')],
    })
    expect(await accepted.database('orders').query(ORDERS_OF_CALLER, {})).toEqual(ordersOf('ama'))
    accepted.close()
  })
})
