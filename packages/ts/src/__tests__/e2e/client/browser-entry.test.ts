import { afterEach, describe, expect, it, vi } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { TopologyAwareClient } from '../../../client/topology.js'
import { RequestDeniedError } from '../../../core/errors.js'
import type { RequestContext } from '../../../core/types.js'
import { DATABASE_ID, recordingProxy, releaseClientServers, requestTally, servedShop } from './harness.js'

const TOKEN = 'Bearer operator-token'
const TICKET = 'sirannon.browser.ticket'

afterEach(async () => {
  vi.unstubAllGlobals()
  await releaseClientServers()
})

function signalBrowserRuntime(): void {
  vi.stubGlobal('window', {})
}

function thrownBy(build: () => { close(): void }): (Error & { code?: string }) | undefined {
  try {
    build().close()
    return undefined
  } catch (err) {
    return err as Error & { code?: string }
  }
}

describe('the browser-facing client entry', () => {
  it('refuses every routing option and every unusable server address, naming the entry point that routes', async () => {
    const tally = requestTally()
    const shop = await servedShop({ server: { authenticate: tally.authenticate } })
    const routing: Array<Record<string, unknown>> = [
      { endpoints: [shop.served.base] },
      { primary: shop.served.base },
      { replicas: [shop.served.base] },
      { discovery: 'coordinator' },
      { readPreference: 'nearest' },
      { readConcern: 'majority' },
      {},
    ]

    for (const options of routing) {
      const thrown = thrownBy(() => new SirannonClient(options as unknown as string))
      const named = Object.keys(options)[0]
      expect([thrown?.name, thrown?.code], JSON.stringify(options)).toEqual(['RemoteError', 'INVALID_ARGUMENT'])
      expect(thrown?.message, JSON.stringify(options)).toContain('client/topology')
      if (named !== undefined) expect(thrown?.message, named).toContain(`'${named}'`)
    }
    for (const address of [
      'not a url',
      'file:///var/data/shop.db',
      'ftp://127.0.0.1/shop',
      `http://clerk:secret@127.0.0.1:${shop.served.port}`,
      `${shop.served.base}/?tenant=north`,
      `${shop.served.base}/#fragment`,
    ]) {
      expect(
        thrownBy(() => new SirannonClient(address)),
        address,
      ).toBeInstanceOf(Error)
    }
    const trailing = new SirannonClient(`${shop.served.base}///`, { transport: 'http' })
    const answered = await trailing.database(DATABASE_ID).query<{ one: number }>('SELECT 1 AS one')
    trailing.close()

    expect(answered).toEqual([{ one: 1 }])
    expect(tally.requests.map(ctx => ctx.path)).toEqual([`/db/${DATABASE_ID}/query`])
  })

  it('refuses a header alone on a browser WebSocket client, and sends the header over HTTP and the ticket on the upgrade', async () => {
    const tally = requestTally()
    const shop = await servedShop({ server: { authenticate: tally.authenticate } })
    const proxy = await recordingProxy(shop.served.port)
    signalBrowserRuntime()

    const headerAlone = [
      thrownBy(() => new SirannonClient(shop.served.base, { headers: { authorization: TOKEN } })),
      thrownBy(
        () => new SirannonClient(shop.served.base, { headers: { authorization: TOKEN }, webSocketProtocols: [] }),
      ),
      thrownBy(() => new TopologyAwareClient({ primary: shop.served.base, headers: { authorization: TOKEN } })),
    ]
    const overHttp = new SirannonClient(proxy.base, { transport: 'http', headers: { authorization: TOKEN } })
    const httpAnswer = await overHttp.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    overHttp.close()
    const both = new SirannonClient(shop.served.base, {
      autoReconnect: false,
      headers: { authorization: TOKEN },
      webSocketProtocols: [TICKET],
    })
    const socketAnswer = await both.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    both.close()

    for (const thrown of headerAlone) {
      expect([thrown?.name, thrown?.code]).toEqual(['RemoteError', 'INVALID_ARGUMENT'])
      expect(thrown?.message).toContain('webSocketProtocols')
    }
    expect([httpAnswer, socketAnswer]).toEqual([[{ n: 2 }], [{ n: 2 }]])
    expect(proxy.requests.map(request => [request.path, request.headers.authorization])).toEqual([
      ['/capabilities', TOKEN],
      [`/db/${DATABASE_ID}/query`, TOKEN],
    ])
    expect(tally.upgrades.map(ctx => [ctx.headers.authorization, ctx.headers['sec-websocket-protocol']])).toEqual([
      [undefined, `sirannon.v1, ${TICKET}`],
    ])
  })

  it('sends the header on coordinator discovery and the ticket on the socket from a browser topology client', async () => {
    const discoveries: RequestContext[] = []
    const upgrades: RequestContext[] = []
    let ownBase = ''
    const shop = await servedShop({
      server: {
        authenticate: ctx => {
          if (ctx.headers['sec-websocket-key'] === undefined) {
            return ctx.headers.authorization === TOKEN ? { actor: 'operator' } : undefined
          }
          upgrades.push(ctx)
          const offered = (ctx.headers['sec-websocket-protocol'] ?? '').split(',').map(value => value.trim())
          if (offered.includes(TICKET)) return { actor: 'browser' }
          throw new RequestDeniedError(401, 'UNAUTHORIZED', 'The socket needs a ticket')
        },
        authorizeClusterStatus: ctx => {
          discoveries.push(ctx)
          return ctx.headers.authorization === TOKEN
        },
        getClusterStatus: databaseId => ({
          databaseId,
          currentPrimary: { nodeId: 'node-a', endpoint: ownBase },
          primaryTerm: 1n,
          readEndpoints: [{ nodeId: 'node-a', endpoint: ownBase, readConcerns: ['local', 'majority'] }],
          health: 'healthy',
          healthReason: 'in-sync',
        }),
      },
    })
    ownBase = shop.served.base
    signalBrowserRuntime()
    const client = new TopologyAwareClient({
      endpoints: [shop.served.base],
      discovery: 'coordinator',
      transport: 'websocket',
      readPreference: 'replica',
      readConcern: 'majority',
      headers: { Authorization: TOKEN },
      webSocketProtocols: [TICKET],
    })

    const rows = await client
      .database(DATABASE_ID)
      .query<{ customer: string }>('SELECT customer FROM orders ORDER BY id')
    client.close()

    expect(rows).toEqual([{ customer: 'ama' }, { customer: 'kofi' }])
    expect(discoveries.map(ctx => [ctx.path, ctx.headers.authorization])).toEqual([
      [`/db/${DATABASE_ID}/cluster`, TOKEN],
    ])
    expect(upgrades.map(ctx => [ctx.headers.authorization, ctx.headers['sec-websocket-protocol']])).toEqual([
      [undefined, `sirannon.v1, ${TICKET}`],
    ])
  })
})
