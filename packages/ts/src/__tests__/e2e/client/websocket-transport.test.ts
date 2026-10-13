import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { ChangeEvent, ServerExecutionTarget } from '../../../core/types.js'
import { waitUntil } from '../lib/served-registry.js'
import {
  DATABASE_ID,
  type Gate,
  gate,
  INSERT_ORDER,
  middlebox,
  pause,
  releaseClientServers,
  restartServer,
  type ServedShop,
  servedShop,
  storedCustomers,
  storedOrders,
} from './harness.js'

const REQUEST_ID = /^c_(\d+)_(\d+)$/

afterEach(releaseClientServers)

function codeOf(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    () => 'resolved',
    (err: { code?: string }) => err.code ?? 'none',
  )
}

function heldReads(shop: ServedShop, held: () => Gate | null): ServerExecutionTarget {
  return {
    query: async (sql, params, options) => {
      await held()?.opened
      return shop.database.query(sql, params, options)
    },
    execute: (sql, params, options) => shop.database.execute(sql, params, options),
    transaction: fn => shop.database.transaction(fn),
    executeTransaction: statements => shop.database.executeTransaction(statements),
  }
}

describe('the published client over WebSocket', () => {
  it('sends every request over one connection with an id of its own and rolls back a transaction that fails', async () => {
    const shop = await servedShop()
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)

    await db.execute(INSERT_ORDER, ['esi', 30])
    await db.batch(INSERT_ORDER, [
      ['kwame', 40],
      ['abena', 50],
    ])
    const failed = await codeOf(
      db.transaction([
        { sql: INSERT_ORDER, params: ['ghost', 60] },
        { sql: INSERT_ORDER, params: [null, 70] },
      ]),
    )
    const committed = await db.transaction([
      { sql: 'UPDATE orders SET total = total + 1 WHERE customer = ?', params: ['ama'] },
      { sql: 'DELETE FROM orders WHERE customer = ?', params: ['kofi'] },
    ])
    const rows = await db.query('SELECT id, customer, total FROM orders ORDER BY id')
    client.close()

    const ids = box.messages('client').map(frame => String(frame.id))
    expect(failed).toBe('QUERY_ERROR')
    expect(committed.map(result => result.changes)).toEqual([1, 1])
    expect(rows).toEqual(storedOrders(shop.path))
    expect(storedCustomers(shop.path)).toEqual(['ama', 'esi', 'kwame', 'abena'])
    expect(box.upgrades).toBe(1)
    expect(ids.every(id => REQUEST_ID.test(id))).toBe(true)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.map(id => Number(REQUEST_ID.exec(id)?.[1]))).toEqual(ids.map((_, index) => index + 1))
  })

  it('fails a request the server holds past requestTimeout with TIMEOUT, and waits for it when the timeout is zero', async () => {
    let held: Gate | null = null
    const shop = await servedShop()
    const served = await restartServer(shop, { resolveExecutionTarget: () => heldReads(shop, () => held) })
    const impatient = new SirannonClient(served.base, { autoReconnect: false, requestTimeout: 150 })
    const patient = new SirannonClient(served.base, { autoReconnect: false, requestTimeout: 0 })

    held = gate()
    const started = Date.now()
    const timedOut = await codeOf(impatient.database(DATABASE_ID).query('SELECT count(*) AS n FROM orders'))
    const elapsed = Date.now() - started
    const waiting = patient.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    await pause(400)
    const firstGate = held
    held = null
    firstGate.open()
    const waited = await waiting
    const afterTimeout = await impatient.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    impatient.close()
    patient.close()

    expect(timedOut).toBe('TIMEOUT')
    expect(elapsed).toBeGreaterThanOrEqual(140)
    expect(elapsed).toBeLessThan(2_000)
    expect(waited).toEqual([{ n: 2 }])
    expect(afterTimeout).toEqual([{ n: 2 }])
  })

  it('fails what is in flight when the connection drops at any stage, and reconnects only for the next request', async () => {
    const shop = await servedShop()
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { reconnectInterval: 20 })
    const db = client.database(DATABASE_ID)

    box.refuseNextConnections(1)
    const duringHandshake = await codeOf(db.query('SELECT 1'))
    box.dropConnectionWhenClientSends(message => message.type === 'execute')
    const duringRequest = await codeOf(db.execute(INSERT_ORDER, ['cut-off', 1]))
    box.dropConnectionWhenClientSends(null)
    await pause(200)
    const upgradesWhileIdle = box.upgrades
    const recovered = await db.execute(INSERT_ORDER, ['after-drop', 2])
    await shop.served.server.close()
    const serverGone = await codeOf(db.query('SELECT 1'))
    const restarted = await restartServer(shop)
    box.pointAt(restarted.port)
    const afterRestart = await db.query<{ n: number }>('SELECT count(*) AS n FROM orders')
    client.close()

    expect(duringHandshake).toBe('CONNECTION_ERROR')
    expect(duringRequest).toBe('CONNECTION_ERROR')
    expect(upgradesWhileIdle).toBe(1)
    expect(recovered.changes).toBe(1)
    expect(serverGone).toBe('CONNECTION_ERROR')
    expect(afterRestart).toEqual([{ n: 3 }])
    expect(storedCustomers(shop.path)).toEqual(['ama', 'kofi', 'after-drop'])
    expect(box.upgrades).toBe(3)
  })

  it('rejects requests still in flight with TRANSPORT_ERROR when the client closes and fails every call after', async () => {
    const held = gate()
    const shop = await servedShop()
    const served = await restartServer(shop, { resolveExecutionTarget: () => heldReads(shop, () => held) })
    const client = new SirannonClient(served.base, { autoReconnect: false })
    const db = client.database(DATABASE_ID)
    await db.execute(INSERT_ORDER, ['before-close', 1])

    const pending = [codeOf(db.query('SELECT 1')), codeOf(db.query('SELECT 2'))]
    await pause(100)
    client.close()
    held.open()
    const outcomes = await Promise.all(pending)
    const afterClose = await codeOf(db.execute(INSERT_ORDER, ['after-close', 2]))

    expect(outcomes).toEqual(['TRANSPORT_ERROR', 'TRANSPORT_ERROR'])
    expect(afterClose).toBe('TRANSPORT_ERROR')
    expect(() => client.database(DATABASE_ID)).toThrow(/closed/)
    expect(storedCustomers(shop.path)).toEqual(['ama', 'kofi', 'before-close'])
  })

  it('answers hundreds of concurrent requests and dozens of filtered subscriptions through one client', async () => {
    const shop = await servedShop({ customers: [] })
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { autoReconnect: false })
    const overHttp = new SirannonClient(shop.served.base, { transport: 'http' })
    const db = client.database(DATABASE_ID)
    const buckets = 40
    const seen: ChangeEvent[][] = Array.from({ length: buckets }, () => [])
    const subscriptions = await Promise.all(
      seen.map((events, bucket) =>
        db
          .on('orders')
          .filter({ total: bucket })
          .subscribe(event => {
            events.push(event)
          }),
      ),
    )
    const rows = Array.from({ length: 200 }, (_, index) => [`ws-${index}`, index % buckets] as const)
    const httpRows = Array.from({ length: 100 }, (_, index) => [`http-${index}`, buckets + index] as const)

    const counts = await Promise.all([
      ...rows.map(row => db.execute(INSERT_ORDER, [...row]).then(() => -1)),
      ...httpRows.map(row =>
        overHttp
          .database(DATABASE_ID)
          .execute(INSERT_ORDER, [...row])
          .then(() => -1),
      ),
      ...Array.from({ length: 100 }, () =>
        db.query<{ n: number }>('SELECT count(*) AS n FROM orders').then(answer => answer[0]?.n ?? -2),
      ),
    ])
    await waitUntil(
      () => seen.reduce((total, events) => total + events.length, 0) === rows.length,
      'every filtered change',
    )
    for (const subscription of subscriptions) subscription.unsubscribe()
    client.close()
    overHttp.close()

    const reads = counts.filter(count => count !== -1)
    expect(storedCustomers(shop.path).sort()).toEqual([...rows, ...httpRows].map(([customer]) => customer).sort())
    expect(reads.every(count => count >= 0 && count <= rows.length + httpRows.length)).toBe(true)
    expect(seen.map(events => events.map(event => event.row.customer).sort())).toEqual(
      seen.map((_, bucket) =>
        rows
          .filter(([, total]) => total === bucket)
          .map(([customer]) => customer)
          .sort(),
      ),
    )
    expect(box.upgrades).toBe(1)
  })
})
