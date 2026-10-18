import { request } from 'node:http'
import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { TopologyAwareClient } from '../../../client/topology.js'
import { RequestDeniedError } from '../../../core/errors.js'
import type { ChangeEvent, RequestContext } from '../../../core/types.js'
import { waitUntil } from '../lib/served-registry.js'
import {
  DATABASE_ID,
  INSERT_ORDER,
  middlebox,
  pause,
  recordingProxy,
  releaseClientServers,
  requestTally,
  servedShop,
} from './harness.js'

const NODE_TOKEN = 'Bearer node-token'
const TICKET = 'sirannon.ticket.short-lived'
const PLAIN_IDENTIFIER = 'sirannon.v1'

interface UpgradeAnswer {
  status: number
  code: string | undefined
  protocol: string | undefined
}

afterEach(releaseClientServers)

function upgradeWithOffer(base: string, offer: string | undefined): Promise<UpgradeAnswer> {
  return new Promise((resolve, reject) => {
    const headers: Record<string, string> = {
      Connection: 'Upgrade',
      Upgrade: 'websocket',
      'Sec-WebSocket-Version': '13',
      'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
    }
    if (offer !== undefined) headers['Sec-WebSocket-Protocol'] = offer
    const upgrade = request(`${base}/db/${DATABASE_ID}`, { headers })
    upgrade.on('upgrade', (response, socket) => {
      socket.destroy()
      resolve({
        status: response.statusCode ?? 0,
        code: undefined,
        protocol: response.headers['sec-websocket-protocol'],
      })
    })
    upgrade.on('response', response => {
      const chunks: Buffer[] = []
      response.on('data', chunk => chunks.push(Buffer.from(chunk)))
      response.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8')
        const parsed = text.length > 0 ? (JSON.parse(text) as { error?: { code?: string } }) : undefined
        resolve({
          status: response.statusCode ?? 0,
          code: parsed?.error?.code,
          protocol: response.headers['sec-websocket-protocol'],
        })
      })
    })
    upgrade.on('error', reject)
    upgrade.end()
  })
}

function closeOfUpgrade(wsBase: string, headers: Record<string, string>): Promise<{ code: number; reason: string }> {
  return new Promise(resolve => {
    const WebSocketWithHeaders = WebSocket as unknown as new (
      url: string,
      init: { headers: Record<string, string> },
    ) => WebSocket
    const socket = new WebSocketWithHeaders(`${wsBase}/db/${DATABASE_ID}`, { headers })
    socket.addEventListener('error', () => {})
    socket.addEventListener('close', event => resolve({ code: event.code, reason: event.reason }))
  })
}

function failureOf(promise: Promise<unknown>): Promise<{ name: string; code?: string; message: string }> {
  return promise.then(
    () => ({ name: 'resolved', message: '' }),
    (err: Error & { code?: string }) => ({ name: err.name, code: err.code, message: err.message }),
  )
}

function refuseUnless(allowed: (ctx: RequestContext) => boolean): (ctx: RequestContext) => { caller: string } {
  return ctx => {
    if (ctx.headers['x-banned'] !== undefined) throw new RequestDeniedError(403, 'FORBIDDEN', 'This caller is banned')
    if (!allowed(ctx)) throw new RequestDeniedError(401, 'UNAUTHORIZED', 'The credential was refused')
    return { caller: 'accepted' }
  }
}

describe('credentials on the WebSocket upgrade', () => {
  it('carries a header from Node and a ticket in the offer, and the server selects only the plain identifier', async () => {
    const upgrades: RequestContext[] = []
    const shop = await servedShop({
      server: {
        authenticate: ctx => {
          if (ctx.headers['sec-websocket-key'] !== undefined) upgrades.push(ctx)
          const offered = (ctx.headers['sec-websocket-protocol'] ?? '').split(',').map(value => value.trim())
          if (ctx.headers.authorization === NODE_TOKEN || offered.includes(TICKET)) return { caller: 'ok' }
          throw new RequestDeniedError(401, 'UNAUTHORIZED', 'No credential')
        },
      },
    })
    const withHeader = new SirannonClient(shop.served.base, {
      autoReconnect: false,
      headers: { Authorization: NODE_TOKEN },
    })
    const withTicket = new SirannonClient(shop.served.base, { autoReconnect: false, webSocketProtocols: [TICKET] })
    const withTicketString = new SirannonClient(shop.served.base, { autoReconnect: false, webSocketProtocols: TICKET })

    const answers = await Promise.all(
      [withHeader, withTicket, withTicketString].map(client =>
        client.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders'),
      ),
    )
    const offers: Array<[string | undefined, UpgradeAnswer]> = []
    for (const offer of [
      `${PLAIN_IDENTIFIER}, ${TICKET}`,
      `${TICKET}, ${PLAIN_IDENTIFIER}`,
      PLAIN_IDENTIFIER,
      TICKET,
      'sirannon.v2',
      undefined,
    ]) {
      offers.push([offer, await upgradeWithOffer(shop.served.base, offer)])
    }
    for (const client of [withHeader, withTicket, withTicketString]) client.close()

    expect(answers).toEqual([[{ n: 2 }], [{ n: 2 }], [{ n: 2 }]])
    expect(
      upgrades
        .slice(0, 3)
        .map(ctx => `${ctx.headers.authorization ?? '-'} | ${ctx.headers['sec-websocket-protocol'] ?? '-'}`)
        .sort(),
    ).toEqual([`- | ${PLAIN_IDENTIFIER}, ${TICKET}`, `- | ${PLAIN_IDENTIFIER}, ${TICKET}`, `${NODE_TOKEN} | -`])
    expect(offers).toEqual([
      [`${PLAIN_IDENTIFIER}, ${TICKET}`, { status: 101, code: undefined, protocol: PLAIN_IDENTIFIER }],
      [`${TICKET}, ${PLAIN_IDENTIFIER}`, { status: 101, code: undefined, protocol: PLAIN_IDENTIFIER }],
      [PLAIN_IDENTIFIER, { status: 101, code: undefined, protocol: PLAIN_IDENTIFIER }],
      [TICKET, { status: 400, code: 'UNSUPPORTED_SUBPROTOCOL', protocol: undefined }],
      ['sirannon.v2', { status: 400, code: 'UNSUPPORTED_SUBPROTOCOL', protocol: undefined }],
      [undefined, { status: 101, code: undefined, protocol: undefined }],
    ])
  })

  it('closes a refused upgrade with 4401 or 4403 and raises UNAUTHORIZED or FORBIDDEN with the reason, without trying again', async () => {
    const tally = requestTally()
    const refuse = refuseUnless(ctx => ctx.headers.authorization === NODE_TOKEN)
    const shop = await servedShop({
      server: {
        authenticate: ctx => {
          tally.authenticate(ctx)
          return refuse(ctx)
        },
      },
    })
    const box = await middlebox(shop.served.port)
    const stranger = new SirannonClient(box.base, { reconnectInterval: 20 })
    const banned = new SirannonClient(box.base, {
      reconnectInterval: 20,
      headers: { Authorization: NODE_TOKEN, 'x-banned': 'yes' },
    })

    const closes = [
      await closeOfUpgrade(shop.served.wsBase, {}),
      await closeOfUpgrade(shop.served.wsBase, { 'x-banned': 'yes' }),
    ]
    const strangerFirst = await failureOf(stranger.database(DATABASE_ID).query('SELECT 1'))
    const bannedFirst = await failureOf(
      banned
        .database(DATABASE_ID)
        .on('orders')
        .subscribe(() => {}),
    )
    const upgradesAfterRefusals = tally.upgrades.length
    const strangerAgain = await failureOf(stranger.database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1]))
    const bannedAgain = await failureOf(banned.database(DATABASE_ID).query('SELECT 1'))
    await pause(200)
    stranger.close()
    banned.close()

    expect(closes.map(close => close.code)).toEqual([4401, 4403])
    expect(closes[0]?.reason).toContain('UNAUTHORIZED')
    expect(closes[1]?.reason).toContain('FORBIDDEN')
    expect([strangerFirst.name, strangerFirst.code]).toEqual(['RemoteError', 'UNAUTHORIZED'])
    expect(strangerFirst.message).toContain('The credential was refused')
    expect([bannedFirst.name, bannedFirst.code]).toEqual(['RemoteError', 'FORBIDDEN'])
    expect(bannedFirst.message).toContain('This caller is banned')
    expect([strangerAgain.code, bannedAgain.code]).toEqual(['UNAUTHORIZED', 'FORBIDDEN'])
    expect(tally.upgrades.length).toBe(upgradesAfterRefusals)
    expect(box.upgrades).toBe(2)
  })

  it('stops reconnecting once the server refuses the credential a subscription reconnects with', async () => {
    let refusing = false
    const tally = requestTally()
    const shop = await servedShop({
      server: {
        authenticate: ctx => {
          tally.authenticate(ctx)
          if (refusing) throw new RequestDeniedError(401, 'UNAUTHORIZED', 'The token expired')
          return undefined
        },
      },
    })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: 20 })
    const received: ChangeEvent[] = []
    await client
      .database(DATABASE_ID)
      .on('orders')
      .subscribe(event => {
        received.push(event)
      })

    refusing = true
    box.killAllConnections()
    await waitUntil(() => tally.upgrades.length === 2, 'the refused reconnect')
    await pause(300)
    const attempts = tally.upgrades.length
    const after = await failureOf(client.database(DATABASE_ID).query('SELECT 1'))
    await shop.database.execute(INSERT_ORDER, ['unseen', 1])
    await pause(150)
    client.close()

    expect(attempts).toBe(2)
    expect(tally.upgrades.length).toBe(2)
    expect(after.code).toBe('UNAUTHORIZED')
    expect(received).toEqual([])
  })

  it('refuses at construction a subprotocol entry a handshake cannot carry and leaves the entry out of the message', async () => {
    const tally = requestTally()
    const shop = await servedShop({ server: { authenticate: tally.authenticate } })
    const entries: Array<string | string[]> = [
      ['ticket with space'],
      ['ticket,split'],
      ['"quoted.ticket"'],
      [''],
      [TICKET, TICKET],
      ['tïcket'],
      ['ticket\tvalue'],
      ['ticket\r\nX-Injected: yes'],
      'ticket(parens)',
      ['sirannon.ok', 'second/slash'],
    ]

    for (const entry of entries) {
      const label = JSON.stringify(entry)
      for (const build of [
        () => new SirannonClient(shop.served.base, { webSocketProtocols: entry }),
        () => new TopologyAwareClient({ primary: shop.served.base, webSocketProtocols: entry }),
      ]) {
        let thrown: (Error & { code?: string }) | undefined
        try {
          build().close()
        } catch (err) {
          thrown = err as Error & { code?: string }
        }
        expect([thrown?.name, thrown?.code], label).toEqual(['RemoteError', 'INVALID_ARGUMENT'])
        expect(thrown?.message, label).toContain('webSocketProtocols')
        for (const value of [entry].flat().filter(item => item.length > 2 && item !== 'sirannon.ok')) {
          expect(thrown?.message, label).not.toContain(value)
        }
      }
    }

    expect([...tally.requests, ...tally.upgrades]).toEqual([])
  })

  it('refuses at construction a header that a request cannot carry and leaves the credential out of the message', async () => {
    const secret = 's3cret-value'
    const shop = await servedShop()
    const proxy = await recordingProxy(shop.served.port)
    const unsendable: Array<Record<string, string>> = [
      { authorization: `Bearer ${secret}\r\nX-Injected: yes` },
      { authorization: `Bearer ${secret}\n` },
      { authorization: `Bearer ${secret}\u0000` },
      { authorization: `Bearer ${secret}☃` },
      { 'x-tenant': 'north', [`bad name ${secret}`]: 'value' },
      { [`x-${secret}:injected`]: 'value' },
      { '': secret },
    ]

    for (const headers of unsendable) {
      for (const transport of ['http', 'websocket'] as const) {
        const label = `${transport} ${JSON.stringify(headers)}`
        for (const build of [
          () => new SirannonClient(proxy.base, { transport, headers }),
          () => new TopologyAwareClient({ primary: proxy.base, transport, headers }),
        ]) {
          const outcome = await failureOf(
            (async () => {
              const client = build()
              try {
                await client.database(DATABASE_ID).query('SELECT 1')
              } finally {
                client.close()
              }
            })(),
          )
          expect([outcome.name, outcome.code], label).toEqual(['RemoteError', 'INVALID_ARGUMENT'])
          expect(outcome.message, label).toContain("'headers'")
          expect(outcome.message, label).not.toContain(secret)
        }
      }
    }
    const latin1 = new SirannonClient(proxy.base, {
      transport: 'http',
      headers: { 'x-label': 'café \t tab', authorization: 'Bearer token' },
    })
    const answered = await latin1.database(DATABASE_ID).query<{ one: number }>('SELECT 1 AS one')
    latin1.close()

    expect(answered).toEqual([{ one: 1 }])
    expect(proxy.requests.filter(request => request.path !== '/capabilities')).toHaveLength(1)
    expect(proxy.requests.every(request => request.headers.authorization === 'Bearer token')).toBe(true)
  })
})
