import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { runCodegen } from '../../../codegen/run.js'
import type { LiveQuery } from '../../../core/live/types.js'
import type { DatabaseOperations, OperationRegistry } from '../../../core/operation-registry.js'
import type { ServerOptions } from '../../../core/types.js'
import { RawSocket, scratchDirectory, sendRaw, trackedRegistry, waitUntil } from '../lib/served-registry.js'
import { openOrders, releaseAccessServers, SEEDED_ORDERS, serveTyped, storedOrders } from './served-access.js'

interface Identity {
  tenant: string
  region: string
}

const select = () => ({ sql: 'SELECT id FROM orders' })
const insert = () => ({ sql: "INSERT INTO orders (customer, total, tenant) VALUES ('x', 1, 'acme')" })
const SYNC_TOKENS = 8

function baseline(): OperationRegistry<Identity> {
  return {
    orders: {
      reads: { byStatus: { args: ['status'], fromIdentity: { tenant: 'tenant' }, statement: select } },
      writes: { cancel: { args: ['id'], statements: insert } },
    },
  }
}

const sharedRead: DatabaseOperations<Identity> = { reads: { byStatus: { args: ['status'], statement: select } } }

let dir: string

beforeEach(() => {
  dir = scratchDirectory('registry-digest')
})

afterEach(releaseAccessServers)

async function capabilitiesOf(
  options: ServerOptions<Identity>,
): Promise<{ capabilities: string[]; registry?: { digest: string } }> {
  const served = await serveTyped<Identity>(trackedRegistry(), options)
  const answer = await sendRaw(served.base, 'GET', '/capabilities')
  return answer.body as { capabilities: string[]; registry?: { digest: string } }
}

async function digestOf(
  operations?: OperationRegistry<Identity>,
  shared?: DatabaseOperations<Identity>,
): Promise<string> {
  const digest = (await capabilitiesOf({ operations, sharedOperations: shared })).registry?.digest
  if (typeof digest !== 'string' || digest.length === 0) throw new Error('the server announced no digest')
  return digest
}

async function rowsOf(live: LiveQuery): Promise<readonly Record<string, unknown>[]> {
  const state = live.getState()
  return state.status === 'ready' ? state.rows : []
}

describe('the registry digest that a client checks', () => {
  it('announces query.named with a digest only when operations are registered, and query.sql only when SQL is accepted', async () => {
    const cases: Array<[label: string, options: ServerOptions<Identity>, tokens: string[], digest: boolean]> = [
      ['nothing', {}, [], false],
      ['SQL', { acceptSql: true }, ['query.sql'], false],
      ['operations', { operations: baseline() }, ['query.named'], true],
      ['an empty registry', { operations: {} }, ['query.named'], true],
      ['shared operations', { sharedOperations: sharedRead }, ['query.named'], true],
      ['operations and SQL', { operations: baseline(), acceptSql: true }, ['query.named', 'query.sql'], true],
    ]

    for (const [label, options, tokens, digest] of cases) {
      const announced = await capabilitiesOf(options)
      expect([...announced.capabilities].sort(), label).toEqual(tokens)
      expect(announced.registry !== undefined, label).toBe(digest)
    }
    const withSync = await capabilitiesOf({
      operations: baseline(),
      acceptDeviceSync: true,
      authenticate: () => ({ tenant: 'acme', region: 'eu' }),
    })
    expect(withSync.capabilities).toContain('query.named')
    expect(withSync.capabilities.filter(token => token.startsWith('sync.')).length).toBeGreaterThanOrEqual(SYNC_TOKENS)
  })

  it('changes the digest whenever the contract that a client calls changes, and keeps it when only statements, columns, or identity fields change', async () => {
    const original = await digestOf(baseline())
    const changed: Array<
      [label: string, registry: OperationRegistry<Identity>, shared?: DatabaseOperations<Identity>]
    > = [
      [
        'an added read',
        { orders: { ...baseline().orders, reads: { ...baseline().orders?.reads, all: { statement: select } } } },
      ],
      ['a removed write', { orders: { reads: baseline().orders?.reads } }],
      [
        'a renamed read',
        {
          orders: {
            ...baseline().orders,
            reads: { byState: { args: ['status'], fromIdentity: { tenant: 'tenant' }, statement: select } },
          },
        },
      ],
      ['a renamed database', { shipments: baseline().orders ?? {} }],
      [
        'an added argument',
        {
          orders: {
            ...baseline().orders,
            reads: { byStatus: { args: ['status', 'page'], fromIdentity: { tenant: 'tenant' }, statement: select } },
          },
        },
      ],
      [
        'a renamed argument',
        {
          orders: {
            ...baseline().orders,
            reads: { byStatus: { args: ['state'], fromIdentity: { tenant: 'tenant' }, statement: select } },
          },
        },
      ],
      [
        'a renamed identity argument',
        {
          orders: {
            ...baseline().orders,
            reads: { byStatus: { args: ['status'], fromIdentity: { owner: 'tenant' }, statement: select } },
          },
        },
      ],
      [
        'a read moved to the writes',
        {
          orders: {
            writes: {
              ...baseline().orders?.writes,
              byStatus: { args: ['status'], fromIdentity: { tenant: 'tenant' }, statements: insert },
            },
          },
        },
      ],
      [
        'a read moved to the shared set',
        { orders: { writes: baseline().orders?.writes } },
        { reads: { byStatus: { args: ['status'], fromIdentity: { tenant: 'tenant' }, statement: select } } },
      ],
      ['an added shared set', baseline(), sharedRead],
    ]
    const kept: Array<[label: string, registry: OperationRegistry<Identity>]> = [
      ['the same registry built again', baseline()],
      [
        'a different statement',
        {
          orders: {
            ...baseline().orders,
            reads: {
              byStatus: {
                args: ['status'],
                fromIdentity: { tenant: 'tenant' },
                statement: () => ({ sql: 'SELECT 2' }),
              },
            },
          },
        },
      ],
      [
        'declared columns',
        {
          orders: {
            ...baseline().orders,
            reads: {
              byStatus: { args: ['status'], fromIdentity: { tenant: 'tenant' }, columns: ['id'], statement: select },
            },
          },
        },
      ],
      [
        'another identity field behind the same argument',
        {
          orders: {
            ...baseline().orders,
            reads: { byStatus: { args: ['status'], fromIdentity: { tenant: 'region' }, statement: select } },
          },
        },
      ],
    ]

    const seen = new Set([original])
    for (const [label, registry, shared] of changed) {
      const digest = await digestOf(registry, shared)
      expect(seen.has(digest), label).toBe(false)
      seen.add(digest)
    }
    for (const [label, registry] of kept) {
      expect(await digestOf(registry), label).toBe(original)
    }
    expect(await digestOf({}, sharedRead)).not.toBe(await digestOf({ '': sharedRead }))
  })

  it('writes the digest that the server announces into generated references built from the same registry module', async () => {
    const modulePath = join(dir, 'operations.mjs')
    writeFileSync(
      modulePath,
      `export const operations = {
  orders: {
    reads: { everyOrder: { columns: ['id'], statement: () => ({ sql: 'SELECT id FROM orders' }) } },
    writes: { clear: { args: ['tenant'], statements: args => ({ sql: 'DELETE FROM orders WHERE tenant = ?', params: [args.tenant] }) } },
  },
}
export const sharedOperations = { reads: { ping: { statement: () => ({ sql: 'SELECT 1 AS one' }) } } }
`,
    )
    const outPath = join(dir, 'generated.ts')
    await runCodegen(['--registry', modulePath, '--out', outPath])
    const imported = (await import(pathToFileURL(modulePath).href)) as {
      operations: OperationRegistry<Identity>
      sharedOperations: DatabaseOperations<Identity>
    }

    const generated = /export const registryDigest = "([^"]+)"/.exec(readFileSync(outPath, 'utf8'))?.[1]
    const served = await digestOf(imported.operations, imported.sharedOperations)

    expect(generated).toBe(served)
  })

  it('refuses a live query that echoes another digest, and the published client recovers after the server restarts with a new registry', async () => {
    const sirannon = trackedRegistry()
    const { db, path } = await openOrders(sirannon, dir)
    const everyOrder = { statement: () => ({ sql: 'SELECT id, customer FROM orders ORDER BY id' }) }
    const first = await serveTyped<Identity>(sirannon, { operations: { orders: { reads: { everyOrder } } } })
    const announced = (await sendRaw(first.base, 'GET', '/capabilities')).body as { registry: { digest: string } }
    const raw = await RawSocket.open(`${first.wsBase}/db/orders`)

    const stale = await raw.request({
      id: 'stale',
      type: 'subscribe',
      name: 'everyOrder',
      registryDigest: 'f'.repeat(64),
    })
    const wrongType = await raw.request({ id: 'typed', type: 'subscribe', name: 'everyOrder', registryDigest: 5 })
    const current = await raw.request({
      id: 'current',
      type: 'subscribe',
      name: 'everyOrder',
      registryDigest: announced.registry.digest,
    })
    expect([stale.type, stale.error?.code]).toEqual(['error', 'REGISTRY_MISMATCH'])
    expect([wrongType.type, wrongType.error?.code]).toEqual(['error', 'INVALID_MESSAGE'])
    expect(current).toMatchObject({
      type: 'subscribed',
      rows: SEEDED_ORDERS.map(o => ({ id: o.id, customer: o.customer })),
    })
    raw.close()

    const client = new SirannonClient(first.base, { reconnectInterval: 20 })
    const orders = client.database('orders')
    const live = await orders.live('everyOrder')
    expect(await rowsOf(live)).toEqual(SEEDED_ORDERS.map(o => ({ id: o.id, customer: o.customer })))

    await first.server.close()
    await new Promise(resolve => setTimeout(resolve, 50))
    const second = await serveTyped<Identity>(sirannon, {
      port: first.port,
      operations: { orders: { reads: { everyOrder, newcomer: { statement: () => ({ sql: 'SELECT 1 AS one' }) } } } },
    })
    const renewed = (await sendRaw(second.base, 'GET', '/capabilities')).body as { registry: { digest: string } }
    expect(renewed.registry.digest).not.toBe(announced.registry.digest)

    await db.execute("INSERT INTO orders (customer, total, tenant) VALUES ('after-restart', 1, 'acme')")
    const expected = storedOrders(path).map(o => ({ id: o.id, customer: o.customer }))
    const reopened = await orders.live('everyOrder')
    expect(await rowsOf(reopened)).toEqual(expected)
    await waitUntil(
      () =>
        JSON.stringify(live.getState()) === JSON.stringify({ status: 'ready', rows: expected, revalidating: false }),
      'the first live query catching up',
    )

    await live.close()
    await reopened.close()
    client.close()
  })
})
