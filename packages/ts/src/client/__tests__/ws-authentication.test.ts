import { request } from 'node:http'
import { describe, expect, it, vi } from 'vitest'
import { RequestDeniedError } from '../../core/errors.js'
import type { RequestContext } from '../../core/types.js'
import { SIRANNON_WS_SUBPROTOCOL } from '../../core/ws-handshake.js'
import { SirannonClient } from '../client.js'
import { TopologyAwareClient } from '../topology.js'
import { WebSocketTransport } from '../transport/ws.js'
import { assertHandshakeHeadersSupported, runtimeSupportsHandshakeHeaders } from '../transport/ws-headers.js'
import { RemoteError } from '../types.js'
import { until } from './helpers.js'
import { createClientServerHarness } from './server-harness.js'

const harness = createClientServerHarness()

interface UpgradeOutcome {
  status: number
  code: string | undefined
  protocol: string | undefined
}

function wsUrlFor(baseUrl: string, databaseId: string): string {
  return `${baseUrl.replace(/^http:/, 'ws:')}/db/${databaseId}`
}

function attemptUpgrade(baseUrl: string, offered: string | undefined): Promise<UpgradeOutcome> {
  return new Promise((resolve, reject) => {
    const headers: Record<string, string> = {
      Connection: 'Upgrade',
      Upgrade: 'websocket',
      'Sec-WebSocket-Version': '13',
      'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
    }
    if (offered !== undefined) {
      headers['Sec-WebSocket-Protocol'] = offered
    }

    const req = request(`${baseUrl}/db/testdb`, { headers })

    req.on('upgrade', (res, socket) => {
      socket.destroy()
      resolve({
        status: res.statusCode ?? 0,
        code: undefined,
        protocol: res.headers['sec-websocket-protocol'],
      })
    })

    req.on('response', res => {
      const chunks: Buffer[] = []
      res.on('data', chunk => chunks.push(Buffer.from(chunk)))
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf-8')
        const parsed = body.length > 0 ? (JSON.parse(body) as { error?: { code?: string } }) : undefined
        resolve({
          status: res.statusCode ?? 0,
          code: parsed?.error?.code,
          protocol: res.headers['sec-websocket-protocol'],
        })
      })
    })

    req.on('error', reject)
    req.end()
  })
}

function closeOf(url: string, protocols: string[]): Promise<{ code: number; reason: string }> {
  return new Promise(resolve => {
    const socket = new WebSocket(url, protocols)
    socket.addEventListener('error', () => {})
    socket.addEventListener('close', event => {
      resolve({ code: event.code, reason: event.reason })
    })
  })
}

function denyWith(status: number, code: string): (ctx: RequestContext) => undefined {
  return () => {
    throw new RequestDeniedError(status, code, `refused with ${code}`)
  }
}

describe('WebSocket handshake headers', () => {
  it('carries an Authorization header to the authenticate hook of a Node client', async () => {
    const offeredTokens: (string | undefined)[] = []
    const baseUrl = await harness.restart({
      authenticate: ({ headers, path }) => {
        if (!path.startsWith('/db/')) return undefined
        offeredTokens.push(headers.authorization)
        if (headers.authorization !== 'Bearer node-token') {
          throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Invalid or missing token')
        }
        return { user: 'alice' }
      },
    })

    const client = new SirannonClient(baseUrl, {
      transport: 'websocket',
      autoReconnect: false,
      headers: { Authorization: 'Bearer node-token' },
    })

    try {
      const rows = await client.database('testdb').query<{ name: string }>('SELECT name FROM users')
      expect(rows[0].name).toBe('Alice')
    } finally {
      client.close()
    }

    expect(offeredTokens).toContain('Bearer node-token')
  })

  it('reports this runtime as one that attaches a handshake header', () => {
    expect(runtimeSupportsHandshakeHeaders()).toBe(true)
    expect(runtimeSupportsHandshakeHeaders({})).toBe(false)
    expect(runtimeSupportsHandshakeHeaders({ Deno: {}, process: { versions: { node: '22.0.0' } } })).toBe(false)
    expect(runtimeSupportsHandshakeHeaders({ window: {}, process: { versions: { node: '22.0.0' } } })).toBe(false)
    expect(runtimeSupportsHandshakeHeaders({ process: { versions: { node: '22.0.0' } } })).toBe(true)
  })

  it('refuses headers a runtime cannot attach and names the subprotocol option', () => {
    expect(() => assertHandshakeHeadersSupported({ Authorization: 'Bearer t' }, undefined, {})).toThrow(RemoteError)
    expect(() => assertHandshakeHeadersSupported({ Authorization: 'Bearer t' }, undefined, {})).toThrow(
      /webSocketProtocols/,
    )
    expect(() => assertHandshakeHeadersSupported({ Authorization: 'Bearer t' }, [], {})).toThrow(RemoteError)
    expect(() => assertHandshakeHeadersSupported({ Authorization: 'Bearer t' }, '', {})).toThrow(RemoteError)
    expect(() => assertHandshakeHeadersSupported({}, undefined, {})).not.toThrow()
    expect(() => assertHandshakeHeadersSupported(undefined, undefined, {})).not.toThrow()
  })

  it('accepts headers alongside a subprotocol credential in a runtime that attaches none', () => {
    expect(() =>
      assertHandshakeHeadersSupported({ Authorization: 'Bearer t' }, ['sirannon.ticket.abc'], {}),
    ).not.toThrow()
    expect(() =>
      assertHandshakeHeadersSupported({ Authorization: 'Bearer t' }, 'sirannon.ticket.abc', {}),
    ).not.toThrow()
  })

  it('builds the socket from the global constructor when the runtime attaches no header', async () => {
    const captured: { url: string; second: unknown }[] = []

    class CapturingWebSocket extends EventTarget {
      static readonly CONNECTING = 0
      static readonly OPEN = 1
      static readonly CLOSING = 2
      static readonly CLOSED = 3

      readyState = CapturingWebSocket.CONNECTING

      constructor(url: string | URL, second?: unknown) {
        super()
        captured.push({ url: String(url), second })
        queueMicrotask(() => {
          this.readyState = CapturingWebSocket.OPEN
          this.dispatchEvent(new Event('open'))
        })
      }

      send(): void {}

      close(): void {
        this.readyState = CapturingWebSocket.CLOSED
        this.dispatchEvent(new Event('close'))
      }
    }

    vi.stubGlobal('window', {})
    vi.stubGlobal('WebSocket', CapturingWebSocket)

    try {
      const transport = new WebSocketTransport('ws://127.0.0.1:9876/db/testdb', {
        autoReconnect: false,
        requestTimeout: 1,
        headers: { Authorization: 'Bearer token' },
        protocols: ['sirannon.ticket.abc'],
      })
      await expect(transport.query('SELECT 1')).rejects.toThrow()
      transport.close()
    } finally {
      vi.unstubAllGlobals()
    }

    expect(captured[0]?.second).toEqual([SIRANNON_WS_SUBPROTOCOL, 'sirannon.ticket.abc'])
  })

  it('accepts headers on a WebSocket client in a runtime that attaches them', () => {
    const client = new SirannonClient(harness.baseUrl, { headers: { Authorization: 'Bearer node-token' } })
    expect(client.database('testdb').id).toBe('testdb')
    client.close()
  })
})

describe('a browser topology client carrying a header and a ticket', () => {
  it('sends the header on coordinator discovery and the ticket on the upgrade', async () => {
    const token = 'Bearer entitlements-token'
    const ticket = 'sirannon.entitlements.auth.abc'
    const discoveryAuthorization: (string | undefined)[] = []
    const upgradeAuthorization: (string | undefined)[] = []
    const upgradeOffers: (string | undefined)[] = []

    const baseUrl = await harness.restart({
      authenticate: ({ headers }) => {
        if (headers['sec-websocket-key'] === undefined) {
          return headers.authorization === token ? { actor: 'operator' } : undefined
        }

        upgradeAuthorization.push(headers.authorization)
        upgradeOffers.push(headers['sec-websocket-protocol'])
        const offered = (headers['sec-websocket-protocol'] ?? '').split(',').map(value => value.trim())
        if (headers.authorization === token) return { actor: 'operator' }
        if (offered.includes(ticket)) return { actor: 'browser' }
        throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Missing valid token')
      },
      authorizeClusterStatus: ({ headers }) => {
        discoveryAuthorization.push(headers.authorization)
        return headers.authorization === token
      },
      getClusterStatus: databaseId => ({
        databaseId,
        currentPrimary: { nodeId: 'node-a', endpoint: harness.baseUrl },
        primaryTerm: 1n,
        readEndpoints: [{ nodeId: 'node-a', endpoint: harness.baseUrl, readConcerns: ['local', 'majority'] }],
        health: 'healthy' as const,
        healthReason: 'in-sync' as const,
      }),
    })

    vi.stubGlobal('window', {})
    let client: TopologyAwareClient | undefined

    try {
      client = new TopologyAwareClient({
        endpoints: [baseUrl],
        discovery: 'coordinator',
        transport: 'websocket',
        readPreference: 'replica',
        readConcern: 'majority',
        headers: { Authorization: token },
        webSocketProtocols: [ticket],
      })

      const rows = await client.database('testdb').query<{ name: string }>('SELECT name FROM users')
      expect(rows[0].name).toBe('Alice')
    } finally {
      client?.close()
      vi.unstubAllGlobals()
    }

    expect(discoveryAuthorization).toContain(token)
    expect(upgradeOffers[0]).toBe(`${SIRANNON_WS_SUBPROTOCOL}, ${ticket}`)
    expect(upgradeAuthorization).toEqual([undefined])
  })
})

describe('WebSocket subprotocol negotiation', () => {
  it('selects the plain identifier and never echoes the credential', async () => {
    const offered: string[] = []
    const baseUrl = await harness.restart({
      authenticate: ({ headers }) => {
        const value = headers['sec-websocket-protocol']
        if (value !== undefined) offered.push(value)
        return undefined
      },
    })

    const ticket = 'sirannon.ticket.short-lived-value'
    const outcome = await attemptUpgrade(baseUrl, `${SIRANNON_WS_SUBPROTOCOL}, ${ticket}`)

    expect(outcome.status).toBe(101)
    expect(outcome.protocol).toBe(SIRANNON_WS_SUBPROTOCOL)
    expect(offered[0]).toContain(ticket)
  })

  it('offers the identifier alongside the configured protocols', async () => {
    const offered: string[] = []
    const baseUrl = await harness.restart({
      authenticate: ({ headers }) => {
        const value = headers['sec-websocket-protocol']
        if (value !== undefined) offered.push(value)
        return undefined
      },
    })

    const client = new SirannonClient(baseUrl, {
      autoReconnect: false,
      webSocketProtocols: ['sirannon.ticket.abc'],
    })

    try {
      await client.database('testdb').query('SELECT 1 as result')
    } finally {
      client.close()
    }

    expect(offered[0]).toBe(`${SIRANNON_WS_SUBPROTOCOL}, sirannon.ticket.abc`)
  })

  it('refuses an upgrade offering no protocol it recognises', async () => {
    const outcome = await attemptUpgrade(harness.baseUrl, 'sirannon.ticket.only')

    expect(outcome.status).toBe(400)
    expect(outcome.code).toBe('UNSUPPORTED_SUBPROTOCOL')
    expect(outcome.protocol).toBeUndefined()
  })

  it('upgrades a client that offers no subprotocol at all', async () => {
    const outcome = await attemptUpgrade(harness.baseUrl, undefined)

    expect(outcome.status).toBe(101)
    expect(outcome.protocol).toBeUndefined()
  })
})

describe('a refused WebSocket upgrade', () => {
  it('closes with 4401 for an unauthenticated caller', async () => {
    const baseUrl = await harness.restart({ authenticate: denyWith(401, 'UNAUTHORIZED') })
    const close = await closeOf(wsUrlFor(baseUrl, 'testdb'), [SIRANNON_WS_SUBPROTOCOL])

    expect(close.code).toBe(4401)
    expect(close.reason).toContain('UNAUTHORIZED')
  })

  it('closes with 4403 for a caller that is not permitted', async () => {
    const baseUrl = await harness.restart({ authenticate: denyWith(403, 'FORBIDDEN') })
    const close = await closeOf(wsUrlFor(baseUrl, 'testdb'), [SIRANNON_WS_SUBPROTOCOL])

    expect(close.code).toBe(4403)
    expect(close.reason).toContain('FORBIDDEN')
  })

  it('reports an authentication error to the client rather than a transport error', async () => {
    const baseUrl = await harness.restart({ authenticate: denyWith(401, 'UNAUTHORIZED') })
    const client = new SirannonClient(baseUrl, { autoReconnect: false })

    try {
      await expect(client.database('testdb').query('SELECT 1 as result')).rejects.toMatchObject({
        name: 'RemoteError',
        code: 'UNAUTHORIZED',
      })
    } finally {
      client.close()
    }
  })

  it('stops reconnecting once the server refuses the credential', async () => {
    const client = new SirannonClient(harness.baseUrl, { autoReconnect: true, reconnectInterval: 20 })
    const db = client.database('testdb')

    try {
      await db.on('users').subscribe(() => {})

      let upgradeAttempts = 0
      await harness.restart({
        authenticate: () => {
          upgradeAttempts += 1
          throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Invalid or missing token')
        },
      })

      await until(() => upgradeAttempts > 0, 5000)
      const attemptsAtRefusal = upgradeAttempts

      await new Promise(resolve => setTimeout(resolve, 500))

      expect(upgradeAttempts).toBe(attemptsAtRefusal)
      await expect(db.query('SELECT 1 as result')).rejects.toMatchObject({ code: 'UNAUTHORIZED' })
    } finally {
      client.close()
    }
  })
})
