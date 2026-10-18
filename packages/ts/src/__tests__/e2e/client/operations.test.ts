import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { LiveQuery } from '../../../core/live/types.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { DatabaseOperations, OperationRegistry, QueryOptions, ServerExecutionTarget } from '../../../core/types.js'
import { sendRaw, waitUntil } from '../lib/served-registry.js'
import {
  DATABASE_ID,
  INSERT_ORDER,
  middlebox,
  releaseClientServers,
  restartServer,
  type StoredOrder,
  servedShop,
  storedOrders,
} from './harness.js'

const everyOrder = operationRef<Record<string, never>, StoredOrder>('everyOrder')
const ordersAbove = operationRef<{ minimum: number | bigint }, StoredOrder>('ordersAbove')
const ordersOfCaller = operationRef<{ customer?: string }, StoredOrder>('ordersOfCaller')
const spacedName = operationRef<Record<string, never>, { n: number }>('orders by count')
const addOrder = operationRef<{ customer: string; total: number }>('addOrder')
const addPair = operationRef<{ customer: string }>('addPair')

interface CallerIdentity {
  user: string
}

const READS: DatabaseOperations<CallerIdentity>['reads'] = {
  everyOrder: { statement: () => ({ sql: 'SELECT id, customer, total FROM orders ORDER BY id' }) },
  ordersAbove: {
    args: ['minimum'],
    statement: args => ({
      sql: 'SELECT id, customer, total FROM orders WHERE total > ? ORDER BY id',
      params: [args.minimum],
    }),
  },
  ordersOfCaller: {
    fromIdentity: { customer: 'user' },
    statement: args => ({
      sql: 'SELECT id, customer, total FROM orders WHERE customer = ? ORDER BY id',
      params: [args.customer],
    }),
  },
  'orders by count': { statement: () => ({ sql: 'SELECT count(*) AS n FROM orders' }) },
}

const WRITES: DatabaseOperations<CallerIdentity>['writes'] = {
  addOrder: {
    args: ['customer', 'total'],
    statements: args => ({ sql: INSERT_ORDER, params: [args.customer, args.total] }),
  },
  addPair: {
    args: ['customer'],
    statements: args => [
      { sql: INSERT_ORDER, params: [`${args.customer}-first`, 1] },
      { sql: INSERT_ORDER, params: [`${args.customer}-second`, 2] },
    ],
  },
}

const IDENTIFIED_OPERATIONS: OperationRegistry<CallerIdentity> = { [DATABASE_ID]: { reads: READS, writes: WRITES } }
const OPERATIONS = IDENTIFIED_OPERATIONS as unknown as OperationRegistry

afterEach(releaseClientServers)

function codeOf(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    () => 'resolved',
    (err: { code?: string }) => err.code ?? 'none',
  )
}

function readyRows<T>(live: LiveQuery<T>): readonly T[] | null {
  const state = live.getState()
  return state.status === 'ready' && !state.revalidating ? state.rows : null
}

async function capabilityDigest(base: string): Promise<string> {
  const answer = await sendRaw(base, 'GET', '/capabilities')
  return String((answer.body as { registry?: { digest?: string } }).registry?.digest)
}

describe('registered operations from the published client', () => {
  it('runs reads and writes by name over both transports and refuses what the registry does not allow', async () => {
    const shop = await servedShop({
      server: {
        acceptSql: false,
        operations: OPERATIONS,
        authenticate: ctx => ({ user: ctx.headers['x-user'] ?? 'nobody' }),
      },
    })

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(shop.served.base, {
        transport,
        autoReconnect: false,
        headers: { 'x-user': 'kofi' },
      })
      const db = client.database(DATABASE_ID)
      const added = await db.execute(addOrder, { customer: `${transport}-buyer`, total: 70 })
      const pair = await db.execute(addPair, { customer: transport })
      const all = await db.query(everyOrder, {})
      const above = await db.query(ordersAbove, { minimum: 2n ** 53n + 1n })
      const aboveSixty = await db.query(ordersAbove, { minimum: 60 })
      const mine = await db.query(ordersOfCaller, {})
      const counted = await db.query(spacedName, {})
      const refusals = await Promise.all([
        codeOf(db.query(operationRef('archivedOrders'), {})),
        codeOf(db.execute(operationRef('everyOrder'), {})),
        codeOf(db.query(ordersAbove, {} as { minimum: number })),
        codeOf(db.query(ordersOfCaller, { customer: 'ama' })),
        codeOf(db.execute(addOrder, { customer: null as unknown as string, total: 1 })),
      ])
      client.close()

      const stored = storedOrders(shop.path)
      expect(
        added.map(result => result.changes),
        transport,
      ).toEqual([1])
      expect(
        pair.map(result => result.changes),
        transport,
      ).toEqual([1, 1])
      expect(all, transport).toEqual(stored)
      expect(above, transport).toEqual([])
      expect(aboveSixty, transport).toEqual(stored.filter(order => order.total > 60))
      expect(mine, transport).toEqual(stored.filter(order => order.customer === 'kofi'))
      expect(counted, transport).toEqual([{ n: stored.length }])
      expect(refusals, transport).toEqual([
        'UNKNOWN_QUERY',
        'UNKNOWN_QUERY',
        'MISSING_ARGUMENT',
        'ARGUMENT_NOT_ALLOWED',
        'QUERY_ERROR',
      ])
    }
    expect(storedOrders(shop.path).map(order => order.customer)).toEqual([
      'ama',
      'kofi',
      'http-buyer',
      'http-first',
      'http-second',
      'websocket-buyer',
      'websocket-first',
      'websocket-second',
    ])
  })

  it('carries a per-call read concern to the server and raises the server refusal of a level it does not know', async () => {
    const seen: Array<QueryOptions | undefined> = []
    const shop = await servedShop({ server: { operations: OPERATIONS } })
    const recording: ServerExecutionTarget = {
      query: (sql, params, options) => {
        seen.push(options)
        return shop.database.query(sql, params, options)
      },
      execute: (sql, params, options) => shop.database.execute(sql, params, options),
      transaction: fn => shop.database.transaction(fn),
    }
    const proxied = await restartServer(shop, { operations: OPERATIONS, resolveExecutionTarget: () => recording })
    const levels = ['local', 'majority', 'linearizable'] as const
    const refused: string[] = []

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(proxied.base, { transport, autoReconnect: false })
      const db = client.database(DATABASE_ID)
      for (const level of levels) {
        expect(await db.query(everyOrder, {}, { readConcern: { level } }), level).toEqual(storedOrders(shop.path))
        await db.query('SELECT 1 AS one', [], { readConcern: { level } })
      }
      await db.query(everyOrder, {})
      refused.push(await codeOf(db.query(everyOrder, {}, { readConcern: { level: 'strongest' } as never })))
      refused.push(await codeOf(db.query('SELECT 1', [], { readConcern: { level: 'strongest' } as never })))
      client.close()
    }

    const perTransport = [
      ...levels.flatMap(level => [{ readConcern: { level } }, { readConcern: { level } }]),
      undefined,
    ]
    expect(seen).toEqual([...perTransport, ...perTransport])
    expect(refused).toEqual(['INVALID_REQUEST', 'INVALID_REQUEST', 'INVALID_MESSAGE', 'INVALID_MESSAGE'])
  })
})

describe('live queries from the published client', () => {
  it('holds the rows the file holds as writes change them and stops changing once closed', async () => {
    const shop = await servedShop({ server: { acceptSql: false, operations: OPERATIONS } })
    const client = new SirannonClient(shop.served.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)
    const live = await db.live(everyOrder, {})
    const kinds: string[] = []
    live.subscribe(update => kinds.push(update.kind))
    const initial = readyRows(live)

    await shop.database.execute(INSERT_ORDER, ['esi', 30])
    await waitUntil(() => readyRows(live)?.length === 3, 'the inserted row')
    await shop.database.execute('UPDATE orders SET total = 99 WHERE customer = ?', ['ama'])
    await waitUntil(() => readyRows(live)?.[0]?.total === 99, 'the updated row')
    await shop.database.execute('DELETE FROM orders WHERE customer = ?', ['kofi'])
    await waitUntil(() => readyRows(live)?.length === 2, 'the deleted row')
    const beforeClose = readyRows(live)
    await live.close()
    await shop.database.execute(INSERT_ORDER, ['after-close', 1])
    const sentinel = await db.live(everyOrder, {})
    await waitUntil(() => readyRows(sentinel)?.length === 3, 'a second live query reading the later row')
    client.close()

    expect(initial).toEqual([
      { id: 1, customer: 'ama', total: 10 },
      { id: 2, customer: 'kofi', total: 20 },
    ])
    expect(beforeClose).toEqual(storedOrders(shop.path).filter(order => order.customer !== 'after-close'))
    expect(readyRows(live)).toEqual(beforeClose)
    expect(kinds).toEqual(['ops', 'ops', 'ops'])
  })

  it('marks its rows as revalidating while the server is down and reads them again once it returns', async () => {
    const shop = await servedShop({ server: { acceptSql: false, operations: OPERATIONS } })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: 25 })
    const live = await client.database(DATABASE_ID).live(everyOrder, {})

    await shop.served.server.close()
    await waitUntil(() => {
      const state = live.getState()
      return state.status === 'ready' && state.revalidating
    }, 'the revalidating state')
    const heldWhileDown = live.getState()
    await shop.database.execute(INSERT_ORDER, ['while-down', 5])
    const restarted = await restartServer(shop, { acceptSql: false, operations: OPERATIONS })
    box.pointAt(restarted.port)
    await waitUntil(() => readyRows(live)?.length === 3, 'the rows read again after the restart')
    client.close()

    expect(heldWhileDown).toEqual({
      status: 'ready',
      revalidating: true,
      rows: [
        { id: 1, customer: 'ama', total: 10 },
        { id: 2, customer: 'kofi', total: 20 },
      ],
    })
    expect(readyRows(live)).toEqual(storedOrders(shop.path))
    expect(box.messages('client', 'subscribe').map(frame => frame.name)).toEqual(['everyOrder', 'everyOrder'])
  })

  it('subscribes once more with the new registry digest after a deploy changes the registry', async () => {
    const shop = await servedShop({ server: { acceptSql: false, operations: OPERATIONS } })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: 25 })
    const live = await client.database(DATABASE_ID).live(everyOrder, {})
    const firstDigest = await capabilityDigest(shop.served.base)
    const redeployed: OperationRegistry<CallerIdentity> = {
      [DATABASE_ID]: {
        reads: { ...READS, newestOrder: { statement: () => ({ sql: 'SELECT max(id) AS id FROM orders' }) } },
        writes: WRITES,
      },
    }

    await shop.served.server.close()
    await shop.database.execute(INSERT_ORDER, ['during-deploy', 5])
    const restarted = await restartServer(shop, {
      acceptSql: false,
      operations: redeployed as unknown as OperationRegistry,
    })
    box.pointAt(restarted.port)
    const secondDigest = await capabilityDigest(restarted.base)
    await waitUntil(() => readyRows(live)?.length === 3, 'the rows after the redeploy')
    client.close()

    expect(secondDigest).not.toBe(firstDigest)
    expect(box.messages('client', 'subscribe').map(frame => frame.registryDigest)).toEqual([
      firstDigest,
      firstDigest,
      secondDigest,
    ])
    expect(box.messages('server', 'error').map(frame => (frame.error as { code: string }).code)).toEqual([
      'REGISTRY_MISMATCH',
    ])
    expect(readyRows(live)).toEqual(storedOrders(shop.path))
  })

  it('refuses a live query over HTTP or for an unknown name, and fails one sent an operation outside its rows', async () => {
    const shop = await servedShop({ server: { acceptSql: false, operations: OPERATIONS } })
    const box = await middlebox(shop.served.port)
    const overHttp = new SirannonClient(shop.served.base, { transport: 'http' })
    const client = new SirannonClient(box.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)

    const httpCode = await codeOf(overHttp.database(DATABASE_ID).live(everyOrder, {}))
    const unknownCode = await codeOf(db.live('archivedOrders'))
    const outOfRange = await db.live(everyOrder, {})
    const empty = await db.live(everyOrder, {})
    const [outOfRangeId, emptyId] = box
      .messages('server', 'subscribed')
      .filter(frame => Array.isArray(frame.rows))
      .map(frame => String(frame.id))
    box.inject(JSON.stringify({ type: 'live', id: outOfRangeId, ops: [{ op: 'delete', index: 7 }] }))
    box.inject(JSON.stringify({ type: 'live', id: emptyId }))
    await waitUntil(
      () => outOfRange.getState().status === 'error' && empty.getState().status === 'error',
      'both live queries failing',
    )
    const states = [outOfRange.getState(), empty.getState()]
    const stillServes = await db.query(everyOrder, {})
    overHttp.close()
    client.close()

    expect([httpCode, unknownCode]).toEqual(['TRANSPORT_ERROR', 'UNKNOWN_QUERY'])
    expect(
      states.map(state => (state.status === 'error' ? (state.error as { code?: string }).code : state.status)),
    ).toEqual(['INVALID_RESPONSE', 'INVALID_RESPONSE'])
    expect(stillServes).toEqual(storedOrders(shop.path))
  })
})
