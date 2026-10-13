import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { CorsOptions, NodeHealth, ReplicationStatusInfo } from '../../../core/types.js'
import { createServer } from '../../../server/server.js'
import {
  errorCodeOf,
  postJson,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const ALLOWED_ORIGIN = 'https://app.example.com'
const SECOND_ORIGIN = 'https://admin.example.com'
const FOREIGN_ORIGIN = 'https://evil.example.com'

let dir: string
let served = 0

beforeEach(() => {
  dir = scratchDirectory('operational-routes')
})

afterEach(releaseServedRegistries)

describe('the health routes', () => {
  it('answers liveness and readiness without calling the authenticate hook, and lists each open database', async () => {
    const sirannon = trackedRegistry()
    const calls: string[] = []
    const served = await serveRegistry(sirannon, {
      acceptSql: true,
      authenticate: ctx => {
        calls.push(ctx.path)
        return { user: 'operator' }
      },
    })

    const empty = await sendRaw(served.base, 'GET', '/health/ready')
    await sirannon.open('orders', join(dir, 'orders.db'))
    const setup = await sirannon.open('archive-setup', join(dir, 'archive.db'))
    await setup.execute('CREATE TABLE entries (id INTEGER PRIMARY KEY)')
    await sirannon.close('archive-setup')
    await sirannon.open('archive', join(dir, 'archive.db'), { readOnly: true })
    const closing = await sirannon.open('closing', join(dir, 'closing.db'))
    await closing.close()

    const liveness = await sendRaw(served.base, 'GET', '/health')
    const readiness = await sendRaw(served.base, 'GET', '/health/ready')
    const capabilities = await sendRaw(served.base, 'GET', '/capabilities')

    expect([empty.status, empty.body]).toEqual([200, { status: 'ok', databases: [] }])
    expect([liveness.status, liveness.headers.get('content-type'), liveness.body]).toEqual([
      200,
      'application/json',
      { status: 'ok' },
    ])
    expect(readiness.status).toBe(200)
    expect((readiness.body as { status: string }).status).toBe('ok')
    expect(
      (readiness.body as { databases: Array<{ id: string }> }).databases.sort((a, b) => a.id.localeCompare(b.id)),
    ).toEqual([
      { id: 'archive', readOnly: true, closed: false },
      { id: 'orders', readOnly: false, closed: false },
    ])
    expect(capabilities.status).toBe(200)
    expect(calls).toEqual([])

    await postJson(served.base, '/db/orders/query', { sql: 'SELECT 1' })
    expect(calls).toEqual(['/db/orders/query'])
  })

  it('reports the replication health that the node gives, with 64-bit positions as decimal strings', async () => {
    const sirannon = trackedRegistry()
    await sirannon.open('orders', join(dir, 'orders.db'))
    let health: NodeHealth = { state: 'healthy', reason: 'in-sync', canRead: true, canWrite: false }
    const status = (): ReplicationStatusInfo => ({
      role: 'replica',
      writeForwarding: true,
      peers: 2,
      localSeq: 2n ** 63n - 1n,
      primaryTerm: 2n ** 53n + 1n,
      replicationGroupId: 'orders-group',
      currentPrimary: 'node-a',
      coordinator: { connected: true, authority: false },
      controller: { state: 'standby' },
      inSyncReplicas: ['node-b'],
      laggingReplicas: [],
      syncState: 'ready',
      health,
    })
    const served = await serveRegistry(sirannon, { getReplicationStatus: status })
    const states: Array<[NodeHealth, string, string, string]> = [
      [health, 'ok', 'available', 'unavailable'],
      [
        { state: 'degraded', reason: 'lagging', canRead: true, canWrite: false },
        'degraded',
        'available',
        'unavailable',
      ],
      [
        { state: 'failing_over', reason: 'faulted', canRead: false, canWrite: false },
        'failing_over',
        'unavailable',
        'unavailable',
      ],
      [{ state: 'healthy', reason: 'in-sync', canRead: true, canWrite: true }, 'ok', 'available', 'available'],
    ]

    for (const [reported, readiness, reads, writes] of states) {
      health = reported
      const answer = await sendRaw(served.base, 'GET', '/health/ready')
      expect(answer.body, reported.state).toEqual({
        status: readiness,
        databases: [{ id: 'orders', readOnly: false, closed: false }],
        replication: {
          role: 'replica',
          writeForwarding: true,
          peers: 2,
          localSeq: '9223372036854775807',
          primaryTerm: '9007199254740993',
          replicationGroupId: 'orders-group',
          currentPrimary: 'node-a',
          coordinator: { connected: true, authority: false },
          controller: { state: 'standby' },
          inSyncReplicas: ['node-b'],
          laggingReplicas: [],
          syncState: 'ready',
          healthReason: reported.reason,
          readAvailability: reads,
          writeAvailability: writes,
        },
      })
    }
  })
})

describe('the listening port', () => {
  it('reports -1 before listening and after closing, and refuses a port that another server holds', async () => {
    const sirannon = trackedRegistry()
    const unstarted = createServer(sirannon, { port: 0 })
    expect(unstarted.listeningPort).toBe(-1)

    const first = await serveRegistry(sirannon)
    const second = createServer(sirannon, { port: first.port })

    await expect(second.listen()).rejects.toThrow(`Failed to listen on 127.0.0.1:${first.port}`)
    expect(second.listeningPort).toBe(-1)
    await second.close()
    expect(first.port).toBeGreaterThan(0)
    expect(first.server.listeningPort).toBe(first.port)
    expect((await sendRaw(first.base, 'GET', '/health')).status).toBe(200)

    await first.server.close()
    expect(first.server.listeningPort).toBe(-1)
    await expect(fetch(`${first.base}/health`)).rejects.toThrow()
  })
})

describe('cross-origin requests', () => {
  async function serveWithCors(cors: boolean | CorsOptions | undefined) {
    const sirannon = trackedRegistry()
    await sirannon.open('orders', join(dir, `orders-${served++}.db`))
    return serveRegistry(sirannon, { acceptSql: true, ...(cors === undefined ? {} : { cors }) })
  }

  it('answers a preflight with 204 and the allow headers, and marks each response with the origin it allows', async () => {
    const cases: Array<
      [label: string, cors: boolean | CorsOptions, origin: string, allowed: string | null, vary: string | null]
    > = [
      ['cors: true', true, FOREIGN_ORIGIN, '*', null],
      ['an empty options object', {}, FOREIGN_ORIGIN, '*', null],
      ['a single origin', { origin: ALLOWED_ORIGIN }, FOREIGN_ORIGIN, ALLOWED_ORIGIN, 'Origin'],
      ['a listed origin', { origin: [ALLOWED_ORIGIN, SECOND_ORIGIN] }, SECOND_ORIGIN, SECOND_ORIGIN, 'Origin'],
      ['an origin missing from the list', { origin: [ALLOWED_ORIGIN, SECOND_ORIGIN] }, FOREIGN_ORIGIN, null, null],
    ]

    for (const [label, cors, origin, allowed, vary] of cases) {
      const served = await serveWithCors(cors)
      const preflight = await fetch(`${served.base}/db/orders/query`, {
        method: 'OPTIONS',
        headers: { Origin: origin, 'Access-Control-Request-Method': 'POST' },
      })
      const routes = [
        await sendRaw(served.base, 'POST', '/db/orders/query', '{"sql":"SELECT 1 AS v"}', { Origin: origin }),
        await sendRaw(served.base, 'POST', '/db/orders/query', '{"sql":', { Origin: origin }),
        await sendRaw(served.base, 'GET', '/health', undefined, { Origin: origin }),
        await sendRaw(served.base, 'GET', '/health/ready', undefined, { Origin: origin }),
        await sendRaw(served.base, 'GET', '/capabilities', undefined, { Origin: origin }),
      ]

      expect(preflight.status, label).toBe(204)
      expect(preflight.headers.get('access-control-allow-origin'), label).toBe(allowed)
      expect(preflight.headers.get('vary'), label).toBe(vary)
      expect(preflight.headers.get('access-control-allow-methods'), label).toBe('GET, POST, OPTIONS')
      expect(preflight.headers.get('access-control-allow-headers'), label).toBe('Content-Type, Authorization')
      expect(preflight.headers.get('access-control-max-age'), label).toBe('86400')
      for (const answer of routes) {
        expect(answer.headers.get('access-control-allow-origin'), `${label} ${answer.status}`).toBe(allowed)
        expect(answer.headers.get('vary'), `${label} ${answer.status}`).toBe(vary)
      }
    }
  })

  it('sends the methods and headers that the options name, and no cross-origin header at all without the option', async () => {
    const custom = await serveWithCors({ origin: ALLOWED_ORIGIN, methods: ['POST'], headers: ['X-Tenant'] })
    const preflight = await fetch(`${custom.base}/db/orders/query`, {
      method: 'OPTIONS',
      headers: { Origin: ALLOWED_ORIGIN },
    })

    const plain = await serveWithCors(undefined)
    const answer = await sendRaw(plain.base, 'POST', '/db/orders/query', '{"sql":"SELECT 1"}', {
      Origin: ALLOWED_ORIGIN,
    })
    const options = await fetch(`${plain.base}/db/orders/query`, {
      method: 'OPTIONS',
      headers: { Origin: ALLOWED_ORIGIN },
    })

    expect(preflight.headers.get('access-control-allow-methods')).toBe('POST')
    expect(preflight.headers.get('access-control-allow-headers')).toBe('X-Tenant')
    expect(answer.status).toBe(200)
    expect(answer.headers.get('access-control-allow-origin')).toBeNull()
    expect([options.status, errorCodeOf(await options.json())]).toEqual([404, 'NOT_FOUND'])
  })
})
