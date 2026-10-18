import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { SirannonError } from '../../../core/errors.js'
import type { ChangeEvent } from '../../../core/types.js'
import { waitUntil } from '../lib/served-registry.js'
import {
  changeLogSeqs,
  DATABASE_ID,
  INSERT_ORDER,
  middlebox,
  pause,
  pruneChangeLog,
  releaseClientServers,
  servedShop,
  serverProxy,
  storedOrders,
} from './harness.js'

const RECONNECT_INTERVAL_MS = 25

afterEach(releaseClientServers)

function summary(event: ChangeEvent): [string, unknown, unknown, unknown] {
  return [event.type, event.row.customer, event.row.total, event.oldRow?.customer]
}

function subscribedSeq(message: Record<string, unknown> | undefined): bigint {
  if (typeof message?.seq !== 'string') throw new Error('The server confirmed no subscription sequence')
  return BigInt(message.seq)
}

describe('table subscriptions through the published client', () => {
  it('delivers the inserts, updates, and deletes that match each subscription and nothing after it ends', async () => {
    const shop = await servedShop({ customers: [] })
    const client = new SirannonClient(shop.served.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)
    const everything: ChangeEvent[] = []
    const amaOnly: ChangeEvent[] = []
    const sentinel: ChangeEvent[] = []
    const all = await db.on('orders').subscribe(event => {
      everything.push(event)
    })
    const ama = await db
      .on('orders')
      .filter({ customer: 'ama' })
      .subscribe(event => {
        amaOnly.push(event)
      })
    const amaAtSeven: ChangeEvent[] = []
    const both = await db
      .on('orders')
      .filter({ customer: 'ama' })
      .filter({ total: 7 })
      .subscribe(event => {
        amaAtSeven.push(event)
      })

    await shop.database.execute(INSERT_ORDER, ['ama', 5])
    await shop.database.execute(INSERT_ORDER, ['kofi', 6])
    await shop.database.execute('UPDATE orders SET total = 7 WHERE id = 1')
    await shop.database.execute("UPDATE orders SET customer = 'ama' WHERE id = 2")
    await shop.database.execute("UPDATE orders SET customer = 'esi' WHERE id = 1")
    await shop.database.execute('DELETE FROM orders WHERE id = 2')
    await waitUntil(() => everything.length === 6 && amaOnly.length === 5, 'every change of the first six writes')
    all.unsubscribe()
    both.unsubscribe()
    await shop.database.execute(INSERT_ORDER, ['ama', 8])
    await waitUntil(() => amaOnly.length === 6, 'the insert after the first unsubscribe')
    ama.unsubscribe()
    const last = await db.on('orders').subscribe(event => {
      sentinel.push(event)
    })
    await shop.database.execute(INSERT_ORDER, ['ama', 9])
    await waitUntil(() => sentinel.length === 1, 'the insert after both unsubscribed')
    last.unsubscribe()
    client.close()

    expect(everything.map(summary)).toEqual([
      ['insert', 'ama', 5, undefined],
      ['insert', 'kofi', 6, undefined],
      ['update', 'ama', 7, 'ama'],
      ['update', 'ama', 6, 'kofi'],
      ['update', 'esi', 7, 'ama'],
      ['delete', undefined, undefined, 'ama'],
    ])
    expect(amaOnly.map(event => [event.type, event.oldRow?.customer])).toEqual([
      ['insert', undefined],
      ['update', 'ama'],
      ['insert', undefined],
      ['delete', 'ama'],
      ['delete', 'ama'],
      ['insert', undefined],
    ])
    expect(amaAtSeven.map(event => [event.type, event.row.id ?? event.oldRow?.id])).toEqual([
      ['insert', 1],
      ['delete', 1],
    ])
    expect(everything.map(event => event.seq)).toEqual(changeLogSeqs(shop.path, 'orders').slice(0, 6))
    expect(everything.every(event => event.table === 'orders' && Number.isFinite(event.timestamp))).toBe(true)
    expect(storedOrders(shop.path).map(order => order.customer)).toEqual(['esi', 'ama', 'ama'])
  })

  it('passes a throw or a rejection from a callback to onError and keeps delivering to every subscription', async () => {
    const shop = await servedShop({ customers: [] })
    const client = new SirannonClient(shop.served.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)
    const thrown: Error[] = []
    const rejected: Error[] = []
    const calls = { throwing: 0, rejecting: 0, silent: 0, plain: 0 }
    await db.on('orders').subscribe(
      () => {
        calls.throwing += 1
        throw new Error(`refused change ${calls.throwing}`)
      },
      { onError: err => thrown.push(err) },
    )
    await db.on('orders').subscribe(
      async () => {
        calls.rejecting += 1
        throw new Error(`rejected change ${calls.rejecting}`)
      },
      {
        onError: err => {
          rejected.push(err)
          throw new Error('onError itself failed')
        },
      },
    )
    await db.on('orders').subscribe(() => {
      calls.silent += 1
      throw new Error('nobody listens')
    })
    await db.on('orders').subscribe(() => {
      calls.plain += 1
    })

    for (const customer of ['ama', 'kofi', 'esi']) await shop.database.execute(INSERT_ORDER, [customer, 1])
    await waitUntil(() => calls.plain === 3 && rejected.length === 3, 'three deliveries to every subscription')
    client.close()

    expect(calls).toEqual({ throwing: 3, rejecting: 3, silent: 3, plain: 3 })
    expect(thrown.map(err => err.message)).toEqual(['refused change 1', 'refused change 2', 'refused change 3'])
    expect(rejected.map(err => err.message)).toEqual(['rejected change 1', 'rejected change 2', 'rejected change 3'])
  })

  it('resumes after a dropped connection from the baseline it was given, then from the last change it saw', async () => {
    const shop = await servedShop({ customers: [] })
    const box = await middlebox(shop.served.port)
    const proxy = await serverProxy(box.port)
    const client = new SirannonClient(`http://127.0.0.1:${proxy.port}`, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const received: ChangeEvent[] = []
    await client
      .database(DATABASE_ID)
      .on('orders')
      .subscribe(event => {
        received.push(event)
      })
    const baseline = subscribedSeq(box.messages('server', 'subscribed')[0])

    box.refuseNextConnections(Number.POSITIVE_INFINITY)
    proxy.killAllConnections()
    await pause(100)
    for (const customer of ['outage-1-a', 'outage-1-b', 'outage-1-c']) {
      await shop.database.execute(INSERT_ORDER, [customer, 1])
    }
    box.refuseNextConnections(0)
    await waitUntil(() => received.length === 3, 'the changes written during the first outage')
    const lastSeen = received[2]?.seq

    box.refuseNextConnections(Number.POSITIVE_INFINITY)
    proxy.killAllConnections()
    await pause(100)
    for (const customer of ['outage-2-a', 'outage-2-b']) await shop.database.execute(INSERT_ORDER, [customer, 2])
    box.refuseNextConnections(0)
    await waitUntil(() => received.length === 5, 'the changes written during the second outage')
    await shop.database.execute(INSERT_ORDER, ['after', 3])
    await waitUntil(() => received.length === 6, 'the change after both outages')
    client.close()

    const subscribes = box.messages('client', 'subscribe')
    expect(subscribes.map(frame => frame.sinceSeq)).toEqual([undefined, baseline.toString(), lastSeen?.toString()])
    expect(new Set(subscribes.map(frame => frame.id)).size).toBe(1)
    expect(received.map(event => event.seq)).toEqual(changeLogSeqs(shop.path, 'orders'))
    expect(received.map(event => event.row.customer)).toEqual(storedOrders(shop.path).map(order => order.customer))
  })

  it('calls onReset and carries on live when the change log no longer holds what followed its cursor', async () => {
    const shop = await servedShop({ customers: [] })
    const box = await middlebox(shop.served.port)
    const proxy = await serverProxy(box.port)
    const client = new SirannonClient(`http://127.0.0.1:${proxy.port}`, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const received: ChangeEvent[] = []
    let resets = 0
    await client
      .database(DATABASE_ID)
      .on('orders')
      .subscribe(
        event => {
          received.push(event)
        },
        {
          onReset: () => {
            resets += 1
          },
        },
      )
    await shop.database.execute(INSERT_ORDER, ['before', 1])
    await waitUntil(() => received.length === 1, 'the change before the outage')
    const cursor = received[0]?.seq ?? -1n

    box.refuseNextConnections(Number.POSITIVE_INFINITY)
    proxy.killAllConnections()
    await pause(100)
    for (const customer of ['lost-1', 'lost-2', 'lost-3', 'lost-4'])
      await shop.database.execute(INSERT_ORDER, [customer, 2])
    const pruned = pruneChangeLog(shop.path, cursor + 2n)
    box.refuseNextConnections(0)
    await waitUntil(() => resets === 1, 'the reset after the change log was pruned')
    await shop.database.execute(INSERT_ORDER, ['after', 3])
    await waitUntil(() => received.length === 2, 'the live change after the reset')
    client.close()

    const resumed = box.messages('client', 'subscribe')[1]
    const confirmed = box.messages('server', 'subscribed')[1]
    expect(pruned).toBe(3)
    expect(resumed?.sinceSeq).toBe(cursor.toString())
    expect(confirmed?.resync).toBe(true)
    expect(resets).toBe(1)
    expect(received.map(event => event.row.customer)).toEqual(['before', 'after'])
    expect(received[1]?.seq).toBe(changeLogSeqs(shop.path, 'orders').at(-1))
  })

  it('drops a subscription the server refuses to restore and never asks for it again', async () => {
    let refuseKofi = false
    const shop = await servedShop({
      customers: [],
      registry: {
        hooks: {
          onBeforeSubscribe: ctx => {
            if (refuseKofi && ctx.filter?.customer === 'kofi') {
              throw new SirannonError('Subscriptions to kofi are closed', 'HOOK_DENIED')
            }
          },
        },
      },
    })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const db = client.database(DATABASE_ID)
    const ama: ChangeEvent[] = []
    const kofi: ChangeEvent[] = []
    await db
      .on('orders')
      .filter({ customer: 'ama' })
      .subscribe(event => {
        ama.push(event)
      })
    await db
      .on('orders')
      .filter({ customer: 'kofi' })
      .subscribe(event => {
        kofi.push(event)
      })
    const [amaId, kofiId] = box.messages('client', 'subscribe').map(frame => frame.id)

    refuseKofi = true
    box.killAllConnections()
    await waitUntil(() => box.messages('server', 'error').length === 1, 'the refused restore')
    await waitUntil(() => box.messages('server', 'subscribed').length === 3, 'the restored subscription')
    box.killAllConnections()
    await waitUntil(() => box.messages('server', 'subscribed').length === 4, 'the second restore')
    await shop.database.execute(INSERT_ORDER, ['kofi', 1])
    await shop.database.execute(INSERT_ORDER, ['ama', 2])
    await waitUntil(() => ama.length === 1, 'the change for the restored subscription')
    await pause(150)
    client.close()

    expect(box.messages('client', 'subscribe').map(frame => frame.id)).toEqual([amaId, kofiId, amaId, kofiId, amaId])
    expect(box.messages('server', 'error')[0]).toMatchObject({ id: kofiId, error: { code: 'HOOK_DENIED' } })
    expect(kofi).toEqual([])
    expect(ama.map(event => event.row.customer)).toEqual(['ama'])
  })

  it('keeps a subscription whose restore loses the connection and restores it on the next connection', async () => {
    const shop = await servedShop({ customers: [] })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const received: ChangeEvent[] = []
    await client
      .database(DATABASE_ID)
      .on('orders')
      .subscribe(event => {
        received.push(event)
      })
    let drops = 1
    box.dropConnectionWhenClientSends(message => {
      if (message.type !== 'subscribe' || message.sinceSeq === undefined || drops === 0) return false
      drops -= 1
      return true
    })

    box.killAllConnections()
    await pause(500)
    await shop.database.execute(INSERT_ORDER, ['after-blip', 1])
    await waitUntil(() => received.length === 1, 'the change after the restore was cut off')
    client.close()

    expect(received.map(event => event.row.customer)).toEqual(['after-blip'])
  })

  it('recovers every change after the server closes a connection whose outbound buffer overflowed', async () => {
    const shop = await servedShop({
      customers: [],
      server: { maxBodyBytes: 65_536, maxWebSocketBackpressureBytes: 65_536 },
    })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const received: ChangeEvent[] = []
    await client
      .database(DATABASE_ID)
      .on('orders')
      .subscribe(event => {
        received.push(event)
      })
    const padding = 'p'.repeat(5_000)

    box.stallServerFrames()
    for (let start = 0; start < 600; start += 100) {
      await shop.database.executeBatch(
        INSERT_ORDER,
        Array.from({ length: 100 }, (_, index) => [`${padding}-${start + index}`, start + index]),
      )
    }
    await pause(500)
    box.releaseServerFrames()
    await waitUntil(() => received.length === 600, 'all 600 changes after the overflow')
    client.close()

    expect(box.upgrades).toBeGreaterThanOrEqual(2)
    expect(received.map(event => event.seq)).toEqual(changeLogSeqs(shop.path, 'orders'))
    expect(received.map(event => event.row.total)).toEqual(Array.from({ length: 600 }, (_, index) => index))
  })

  it('passes a change it cannot decode to onError, ignores frames for nothing it holds, and delivers what follows', async () => {
    const shop = await servedShop({ customers: [] })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { autoReconnect: false })
    const received: ChangeEvent[] = []
    const errors: Error[] = []
    await client
      .database(DATABASE_ID)
      .on('orders')
      .subscribe(
        event => {
          received.push(event)
        },
        { onError: err => errors.push(err) },
      )
    const subscriptionId = String(box.messages('server', 'subscribed')[0]?.id)
    const event = { type: 'insert', table: 'orders', row: { id: 1, customer: 'forged', total: 1 }, timestamp: 1 }

    for (const frame of [
      'not json at all',
      '5',
      '[]',
      JSON.stringify({ type: 'result', id: 'nobody-asked', data: { rows: [] } }),
      JSON.stringify({ type: 'change', id: 'unknown-subscription', event: { ...event, seq: '1' } }),
      JSON.stringify({ type: 'unknown-type', id: subscriptionId }),
      JSON.stringify({ type: 'changes', id: subscriptionId, events: 'not a list' }),
      JSON.stringify({ type: 'change', id: subscriptionId, event: { ...event, seq: 'not-a-number' } }),
      JSON.stringify({
        type: 'change',
        id: subscriptionId,
        event: { ...event, row: { id: { __sirannon_int: '12x' } }, seq: '1' },
      }),
      JSON.stringify({ type: 'change', id: subscriptionId }),
    ]) {
      expect(box.inject(frame), frame).toBe(1)
    }
    await waitUntil(() => errors.length === 3, 'three undecodable changes')
    await shop.database.execute(INSERT_ORDER, ['real', 9])
    await waitUntil(() => received.length === 1, 'the real change after the forged frames')
    const answered = await client.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    client.close()

    expect(received.map(summary)).toEqual([['insert', 'real', 9, undefined]])
    expect(errors).toHaveLength(3)
    expect(answered).toEqual([{ n: 1 }])
    expect(box.upgrades).toBe(1)
  })

  it('keeps the client running when a frame is JSON null or an error frame carries no error', async () => {
    const shop = await servedShop({ customers: [] })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)
    await db.on('orders').subscribe(() => {})

    box.inject('null')
    box.inject(JSON.stringify({ type: 'error', id: 'c_1_1' }))
    const answered = await db.query<{ n: number }>('SELECT count(*) AS n FROM orders')
    client.close()

    expect(answered).toEqual([{ n: 0 }])
  })
})
