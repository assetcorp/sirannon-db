import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { CDCMetrics, OperationRegistry } from '../../../core/types.js'
import { postJson } from '../lib/served-registry.js'
import {
  cdcEpochOf,
  changeLog,
  highestSeq,
  lastOfEachTransaction,
  pause,
  QUIET_PERIOD_MS,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  stampsFromLog,
  WireSocket,
  waitFor,
  writeFile,
} from './harness.js'

const ORDERS_SCHEMA = [
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, status TEXT NOT NULL, total INTEGER NOT NULL)',
]
const INSERT_ORDER = 'INSERT INTO orders (customer, status, total) VALUES (?, ?, ?)'
const CLOSING_SUBSCRIBERS = 12
const UNSTAMPED_DELETE_INDEX = 4

const operations: OperationRegistry = {
  shop: {
    reads: {
      openOrders: { statement: () => ({ sql: "SELECT id, customer FROM orders WHERE status = 'open' ORDER BY id" }) },
    },
  },
}

afterEach(releaseChangeStreams)

function shop(cdcEvents?: CDCMetrics[]): Promise<ServedDatabase> {
  return serveDatabase({
    label: 'subscriptions',
    databaseId: 'shop',
    schema: ORDERS_SCHEMA,
    registry: cdcEvents === undefined ? undefined : { metrics: { onCDCEvent: event => cdcEvents.push(event) } },
    server: { operations },
  })
}

function order(id: number, customer: string, status: string, total: number): Record<string, unknown> {
  return { id, customer, status, total }
}

async function insertOrder(served: ServedDatabase, customer: string): Promise<void> {
  const answer = await postJson(served.served.base, '/db/shop/execute', {
    sql: INSERT_ORDER,
    params: [customer, 'open', 1],
  })
  expect(answer.status, answer.text).toBe(200)
}

describe('subscriptions to a table over the WebSocket protocol', () => {
  it('delivers each insert, update, and delete written after it subscribes, from every writer, as the change log records it', async () => {
    const served = await shop()
    await served.db.execute(INSERT_ORDER, ['history', 'open', 5])
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const subscribed = await ws.request({ id: 'feed', type: 'subscribe', table: 'orders' })
    const baseline = highestSeq(served.path)
    const startedAt = Date.now()

    const httpInsert = await postJson(served.served.base, '/db/shop/execute', {
      sql: INSERT_ORDER,
      params: ['ama', 'open', 10],
    })
    const writer = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const wsUpdate = await writer.request({
      id: 'pay',
      type: 'execute',
      sql: "UPDATE orders SET status = 'paid' WHERE id = 2",
    })
    const client = new SirannonClient(served.served.base, { transport: 'websocket', autoReconnect: false })
    await client
      .database('shop')
      .transaction([
        { sql: INSERT_ORDER, params: ['kofi', 'open', 20] },
        { sql: 'UPDATE orders SET total = 25 WHERE id = 3' },
      ])
    client.close()
    writeFile(served.path, 'DELETE FROM orders WHERE id = 1')
    await ws.waitForEvents('feed', UNSTAMPED_DELETE_INDEX + 1)
    const httpBatch = await postJson(served.served.base, '/db/shop/batch', {
      sql: INSERT_ORDER,
      paramsBatch: [
        ['esi', 'open', 30],
        ['kwame', 'open', 40],
      ],
    })
    const events = await ws.waitForEvents('feed', 7)
    const finishedAt = Date.now()
    const log = changeLog(served.path, 'orders').filter(row => row.seq > baseline)
    const ends = lastOfEachTransaction(log)
    const contents = [
      { type: 'insert', row: order(2, 'ama', 'open', 10) },
      { type: 'update', row: order(2, 'ama', 'paid', 10), oldRow: order(2, 'ama', 'open', 10) },
      { type: 'insert', row: order(3, 'kofi', 'open', 20) },
      { type: 'update', row: order(3, 'kofi', 'open', 25), oldRow: order(3, 'kofi', 'open', 20) },
      { type: 'delete', row: {}, oldRow: order(1, 'history', 'open', 5) },
      { type: 'insert', row: order(4, 'esi', 'open', 30) },
      { type: 'insert', row: order(5, 'kwame', 'open', 40) },
    ]

    expect([httpInsert.status, wsUpdate.type, httpBatch.status]).toEqual([200, 'result', 200])
    expect(subscribed).toEqual({
      type: 'subscribed',
      id: 'feed',
      seq: baseline.toString(),
      epoch: cdcEpochOf(served.path),
    })
    expect(log).toHaveLength(contents.length)
    expect(events).toEqual(
      contents.map((content, index) => {
        const logged = log[index]
        if (logged === undefined) throw new Error(`the change log lacks entry ${index}`)
        const stamps =
          index === UNSTAMPED_DELETE_INDEX
            ? { seq: logged.seq.toString(), rowId: logged.row_id, txEnd: true }
            : stampsFromLog(logged, ends)
        return { table: 'orders', timestamp: expect.any(Number), ...content, ...stamps }
      }),
    )
    const [beforeDelete, unstampedDelete, afterDelete] = log.slice(
      UNSTAMPED_DELETE_INDEX - 1,
      UNSTAMPED_DELETE_INDEX + 2,
    )
    expect(unstampedDelete?.node_id).toBe(beforeDelete?.node_id)
    expect(unstampedDelete?.tx_id).toMatch(/^[0-9a-f]{32}$/)
    expect([beforeDelete?.tx_id, afterDelete?.tx_id]).not.toContain(unstampedDelete?.tx_id)
    for (const event of events) {
      expect(event.timestamp).toBeGreaterThanOrEqual(startedAt - 1)
      expect(event.timestamp).toBeLessThanOrEqual(finishedAt + 1)
    }
  })

  it('stops after an unsubscribe, takes the same identifier again, and refuses to unsubscribe one it does not hold', async () => {
    const served = await shop()
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const neighbour = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await ws.request({ id: 'feed', type: 'subscribe', table: 'orders' })
    await neighbour.request({ id: 'other', type: 'subscribe', table: 'orders' })

    const early = await ws.request({ id: 'never-held', type: 'unsubscribe' })
    await insertOrder(served, 'kept')
    await ws.waitForEvents('feed', 1)
    const unsubscribed = await ws.request({ id: 'feed', type: 'unsubscribe' })
    const again = await ws.request({ id: 'feed', type: 'unsubscribe' })
    const foreign = await ws.request({ id: 'other', type: 'unsubscribe' })
    await insertOrder(served, 'unseen')
    await neighbour.waitForEvents('other', 2)
    const resubscribed = await ws.request({ id: 'feed', type: 'subscribe', table: 'orders' })
    await insertOrder(served, 'after')
    const events = await ws.waitForEvents('feed', 2)

    expect([early.type, early.error?.code]).toEqual(['error', 'SUBSCRIPTION_NOT_FOUND'])
    expect(unsubscribed).toEqual({ type: 'unsubscribed', id: 'feed' })
    expect([again.error?.code, foreign.error?.code]).toEqual(['SUBSCRIPTION_NOT_FOUND', 'SUBSCRIPTION_NOT_FOUND'])
    expect(resubscribed.type).toBe('subscribed')
    expect(events.map(event => event.row.customer)).toEqual(['kept', 'after'])
    expect(neighbour.events('other').map(event => event.row.customer)).toEqual(['kept', 'unseen', 'after'])
  })

  it('refuses a second subscription under an identifier in use, whether sent later or in the same burst, and delivers each change once', async () => {
    const served = await shop()
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await ws.request({ id: 'later', type: 'subscribe', table: 'orders' })

    const laterRefusals = [
      await ws.request({ id: 'later', type: 'subscribe', table: 'orders' }),
      await ws.request({ id: 'later', type: 'subscribe', table: 'orders', sinceSeq: '0' }),
      await ws.request({ id: 'later', type: 'subscribe', table: 'orders', filter: { status: 'open' } }),
      await ws.request({ id: 'later', type: 'subscribe', name: 'openOrders' }),
    ]
    for (const variant of [{}, { sinceSeq: '0' }, { name: 'openOrders', table: undefined }]) {
      const id = `burst-${Object.keys(variant).join('-') || 'plain'}`
      ws.send({ id, type: 'subscribe', table: 'orders', ...variant })
      ws.send({ id, type: 'subscribe', table: 'orders', ...variant })
    }
    await waitFor(() => ws.messages.filter(message => message.id?.startsWith('burst-')).length === 6, 'six replies')
    await insertOrder(served, 'once')
    await ws.waitForEvents('later', 1)
    const survivorsGone = await Promise.all(
      ['burst-plain', 'burst-sinceSeq'].map(id => ws.request({ id, type: 'unsubscribe' }, ['unsubscribed', 'error'])),
    )
    await insertOrder(served, 'after the survivors left')
    await ws.waitForEvents('later', 2)

    expect(survivorsGone.map(reply => reply.type)).toEqual(['unsubscribed', 'unsubscribed'])
    for (const refusal of laterRefusals) {
      expect([refusal.type, refusal.error?.code]).toEqual(['error', 'DUPLICATE_SUBSCRIPTION'])
    }
    for (const id of ['burst-plain', 'burst-sinceSeq', 'burst-name-table']) {
      expect(ws.of(id, 'subscribed'), id).toHaveLength(1)
      expect(
        ws.of(id, 'error').map(message => message.error?.code),
        id,
      ).toEqual(['DUPLICATE_SUBSCRIPTION'])
    }
    expect(ws.events('later').map(event => event.row.customer)).toEqual(['once', 'after the survivors left'])
    expect(ws.events('burst-plain').map(event => event.row.customer)).toEqual(['once'])
    expect(ws.events('burst-sinceSeq').map(event => event.row.customer)).toEqual(['once'])
    expect(
      ws.of('burst-name-table', 'live').map(message => message.rows ?? message.revalidating ?? message.ops),
    ).toEqual([
      true,
      [{ id: 1, customer: 'once' }],
      [{ op: 'insert', index: 1, row: { id: 2, customer: 'after the survivors left' } }],
    ])
  })

  it('releases every subscription whose socket closes while it is still opening, so no change is counted for it', async () => {
    const cdcEvents: CDCMetrics[] = []
    const served = await shop(cdcEvents)
    await served.db.execute(INSERT_ORDER, ['history', 'open', 1])
    const variants = [{}, { sinceSeq: '0' }, { filter: { status: 'open' } }, { sinceSeq: '0', filter: { total: 1 } }]

    const closing = await Promise.all(
      Array.from({ length: CLOSING_SUBSCRIBERS }, () => WireSocket.open(`${served.served.wsBase}/db/shop`)),
    )
    for (const [index, socket] of closing.entries()) {
      socket.send({ id: 'brief', type: 'subscribe', table: 'orders', ...variants[index % variants.length] })
      socket.close()
    }
    await Promise.all(closing.map(socket => socket.closed))
    await pause(QUIET_PERIOD_MS)
    const watcher = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await watcher.request({ id: 'watch', type: 'subscribe', table: 'orders' })
    const counted = cdcEvents.length
    for (const customer of ['first', 'second', 'third']) await insertOrder(served, customer)
    await watcher.waitForEvents('watch', 3)

    expect(cdcEvents.slice(counted)).toEqual(
      ['first', 'second', 'third'].map(() => ({
        databaseId: 'shop',
        table: 'orders',
        operation: 'insert',
        subscriberCount: 1,
      })),
    )
  })
})
