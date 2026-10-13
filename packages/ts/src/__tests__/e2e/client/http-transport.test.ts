import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { RequestDeniedError } from '../../../core/errors.js'
import type { ServerExecutionTarget } from '../../../core/types.js'
import { errorCodeOf, errorMessageOf, postJson } from '../lib/served-registry.js'
import {
  DATABASE_ID,
  droppingListener,
  gate,
  INSERT_ORDER,
  pause,
  recordingProxy,
  releaseClientServers,
  requestTally,
  restartServer,
  type StoredOrder,
  servedShop,
  storedCustomers,
  storedOrders,
} from './harness.js'

const TENANT_HEADER = 'x-tenant'

afterEach(releaseClientServers)

function caught(promise: Promise<unknown>): Promise<{ name: string; code?: string; message: string }> {
  return promise.then(
    () => ({ name: 'resolved', message: '' }),
    (err: Error & { code?: string }) => ({ name: err.name, code: err.code, message: err.message }),
  )
}

describe('the published client over HTTP', () => {
  it('sends every statement shape with the caller headers and returns what the file holds', async () => {
    const tally = requestTally()
    const shop = await servedShop({ server: { authenticate: tally.authenticate } })
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, {
      transport: 'http',
      headers: { [TENANT_HEADER]: 'north', authorization: 'Bearer clerk' },
    })
    const db = client.database(DATABASE_ID)

    const positional = await db.query<StoredOrder>('SELECT id, customer, total FROM orders WHERE total > ?', [15])
    const named = await db.query<StoredOrder>('SELECT id, customer, total FROM orders WHERE customer = :who', {
      who: 'ama',
    })
    const nothing = await db.query('SELECT id FROM orders WHERE total > ?', [1_000])
    const inserted = await db.execute(INSERT_ORDER, ['esi', 30])
    const updated = await db.execute('UPDATE orders SET total = total + :bump WHERE total >= :floor', {
      bump: 5,
      floor: 20,
    })
    const committed = await db.transaction([
      { sql: INSERT_ORDER, params: ['kwame', 40] },
      { sql: 'DELETE FROM orders WHERE customer = ?', params: ['ama'] },
    ])
    const batched = await db.batch(INSERT_ORDER, [
      ['abena', 50],
      ['yaw', 60],
    ])
    const everything = await db.query<StoredOrder>('SELECT id, customer, total FROM orders ORDER BY id')
    client.close()

    expect(positional).toEqual([{ id: 2, customer: 'kofi', total: 20 }])
    expect(named).toEqual([{ id: 1, customer: 'ama', total: 10 }])
    expect(nothing).toEqual([])
    expect(inserted).toEqual({ changes: 1, lastInsertRowId: 3 })
    expect(updated.changes).toBe(2)
    expect(committed.map(result => result.changes)).toEqual([1, 1])
    expect(batched).toEqual([
      { changes: 1, lastInsertRowId: 5 },
      { changes: 1, lastInsertRowId: 6 },
    ])
    expect(everything).toEqual(storedOrders(shop.path))
    expect(storedOrders(shop.path)).toEqual([
      { id: 2, customer: 'kofi', total: 25 },
      { id: 3, customer: 'esi', total: 35 },
      { id: 4, customer: 'kwame', total: 40 },
      { id: 5, customer: 'abena', total: 50 },
      { id: 6, customer: 'yaw', total: 60 },
    ])
    expect(proxy.requests.length).toBeGreaterThan(0)
    for (const request of proxy.requests) {
      expect([request.headers[TENANT_HEADER], request.headers.authorization], request.path).toEqual([
        'north',
        'Bearer clerk',
      ])
    }
    expect(tally.requests.map(ctx => ctx.path)).toEqual([
      ...Array(3).fill(`/db/${DATABASE_ID}/query`),
      ...Array(2).fill(`/db/${DATABASE_ID}/execute`),
      `/db/${DATABASE_ID}/transaction`,
      `/db/${DATABASE_ID}/batch`,
      `/db/${DATABASE_ID}/query`,
    ])
    expect(tally.requests.every(ctx => ctx.headers.authorization === 'Bearer clerk')).toBe(true)
  })

  it('raises each refusal with the code and message the server answers and leaves the file as it was', async () => {
    const shop = await servedShop({
      server: {
        maxBodyBytes: 4_096,
        authenticate: ctx => {
          if (ctx.headers[TENANT_HEADER] === 'closed') {
            throw new RequestDeniedError(403, 'TENANT_CLOSED', 'The closed tenant may not read')
          }
          return undefined
        },
      },
    })
    const client = new SirannonClient(shop.served.base, { transport: 'http' })
    const closedTenant = new SirannonClient(shop.served.base, {
      transport: 'http',
      headers: { [TENANT_HEADER]: 'closed' },
    })
    const before = storedOrders(shop.path)
    const refusals: Array<[label: string, call: () => Promise<unknown>, route: string, body: unknown]> = [
      [
        'a statement SQLite cannot parse',
        () => client.database(DATABASE_ID).query('SELEC id FROM orders'),
        `/db/${DATABASE_ID}/query`,
        { sql: 'SELEC id FROM orders' },
      ],
      [
        'a transaction whose second statement breaks NOT NULL',
        () =>
          client.database(DATABASE_ID).transaction([
            { sql: INSERT_ORDER, params: ['ghost', 1] },
            { sql: INSERT_ORDER, params: [null, 2] },
          ]),
        `/db/${DATABASE_ID}/transaction`,
        {
          statements: [
            { sql: INSERT_ORDER, params: ['ghost', 1] },
            { sql: INSERT_ORDER, params: [null, 2] },
          ],
        },
      ],
      [
        'a database the server never opened',
        () => client.database('refunds').query('SELECT 1'),
        '/db/refunds/query',
        { sql: 'SELECT 1' },
      ],
      [
        'a read of an internal table',
        () => client.database(DATABASE_ID).query('SELECT * FROM _sirannon_changes'),
        `/db/${DATABASE_ID}/query`,
        { sql: 'SELECT * FROM _sirannon_changes' },
      ],
      ['a tenant the authenticate hook refuses', () => closedTenant.database(DATABASE_ID).query('SELECT 1'), '', null],
    ]

    for (const [label, call, route, body] of refusals) {
      const failure = await caught(call())
      expect(failure.name, label).toBe('RemoteError')
      if (route === '') {
        expect([failure.code, failure.message], label).toEqual(['TENANT_CLOSED', 'The closed tenant may not read'])
        continue
      }
      const raw = await postJson(shop.served.base, route, body)
      expect([failure.code, failure.message], label).toEqual([errorCodeOf(raw.body), errorMessageOf(raw.body)])
    }
    const oversized = await caught(client.database(DATABASE_ID).execute(INSERT_ORDER, ['x'.repeat(8_192), 1]))
    client.close()
    closedTenant.close()

    expect([oversized.name, oversized.code]).toEqual(['RemoteError', 'PAYLOAD_TOO_LARGE'])
    expect(storedOrders(shop.path)).toEqual(before)
  })

  it('refuses subscriptions and live queries with TRANSPORT_ERROR and sends no request for them', async () => {
    const shop = await servedShop({
      server: {
        operations: {
          [DATABASE_ID]: { reads: { everyOrder: { statement: () => ({ sql: 'SELECT id FROM orders' }) } } },
        },
      },
    })
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const db = client.database(DATABASE_ID)

    const subscription = await caught(db.on('orders').subscribe(() => {}))
    const filtered = await caught(
      db
        .on('orders')
        .filter({ customer: 'ama' })
        .subscribe(() => {}),
    )
    const live = await caught(db.live('everyOrder'))
    client.close()

    for (const failure of [subscription, filtered, live]) {
      expect([failure.name, failure.code]).toEqual(['RemoteError', 'TRANSPORT_ERROR'])
    }
    expect(proxy.requests).toEqual([])
  })

  it('fails every call made after a close and serves a handle opened again for the same database', async () => {
    const shop = await servedShop()
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const first = client.database(DATABASE_ID)
    await first.query('SELECT 1 AS one')
    const sentBeforeClose = proxy.requests.length

    first.close()
    const afterHandleClose = await caught(first.execute(INSERT_ORDER, ['after-close', 1]))
    const reopened = await client.database(DATABASE_ID).query<{ n: number }>('SELECT count(*) AS n FROM orders')
    const sentAfterReopen = proxy.requests.length
    client.close()

    expect([afterHandleClose.name, afterHandleClose.code]).toEqual(['RemoteError', 'TRANSPORT_ERROR'])
    expect(reopened).toEqual([{ n: storedOrders(shop.path).length }])
    expect(() => client.database(DATABASE_ID)).toThrow(/closed/)
    expect(storedCustomers(shop.path)).not.toContain('after-close')
    expect(sentAfterReopen).toBeGreaterThan(sentBeforeClose)
    expect(proxy.requests.filter(request => request.text.includes('after-close'))).toEqual([])
  })

  it('rejects a request still in flight with TRANSPORT_ERROR when the client closes', async () => {
    const held = gate()
    const shop = await servedShop()
    const stalling: ServerExecutionTarget = {
      query: async (sql, params, options) => {
        await held.opened
        return shop.database.query(sql, params, options)
      },
      execute: (sql, params, options) => shop.database.execute(sql, params, options),
      transaction: fn => shop.database.transaction(fn),
    }
    const served = await restartServer(shop, { resolveExecutionTarget: () => stalling })
    const client = new SirannonClient(served.base, { transport: 'http' })
    const pending = caught(client.database(DATABASE_ID).query('SELECT count(*) AS n FROM orders'))

    await pause(100)
    client.close()
    held.open()

    expect(await pending).toMatchObject({ name: 'RemoteError', code: 'TRANSPORT_ERROR' })
  })

  it('raises CONNECTION_ERROR or INVALID_RESPONSE from a server that drops, cuts short, or answers with anything but JSON', async () => {
    const shop = await servedShop()
    const proxy = await recordingProxy(shop.served.port)
    const refused = await droppingListener()
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const db = client.database(DATABASE_ID)
    await db.query('SELECT 1 AS warm')
    const hostile: Array<[label: string, answer: Parameters<typeof proxy.answerWith>[0], code: string]> = [
      ['a dropped connection', () => ({ kind: 'drop' }), 'CONNECTION_ERROR'],
      [
        'a body cut short',
        () => ({ kind: 'truncate', status: 200, declaredLength: 400, text: '{"rows":[{"id":1' }),
        'INVALID_RESPONSE',
      ],
      [
        'a gateway page',
        () => ({ kind: 'reply', status: 502, text: '<html>Bad Gateway</html>', contentType: 'text/html' }),
        'INVALID_RESPONSE',
      ],
      ['plain text with status 200', () => ({ kind: 'reply', status: 200, text: 'rows: none' }), 'INVALID_RESPONSE'],
    ]

    for (const [label, answer, code] of hostile) {
      proxy.answerWith(request => (request.method === 'POST' ? answer(request) : undefined))
      const failure = await caught(db.query('SELECT id FROM orders'))
      expect([failure.name, failure.code], label).toEqual(['RemoteError', code])
    }
    proxy.answerWith(request =>
      request.method === 'POST' ? { kind: 'reply', status: 500, text: '{"error":{}}' } : undefined,
    )
    const codeless = await caught(db.query('SELECT id FROM orders'))
    proxy.answerWith(request =>
      request.method === 'POST'
        ? { kind: 'reply', status: 200, text: '{"rows":[{"total":{"__sirannon_int":"12x"}}]}' }
        : undefined,
    )
    const forgedEnvelope = await caught(db.query('SELECT total FROM orders'))
    proxy.answerWith(() => undefined)
    const recovered = await db.query<{ n: number }>('SELECT count(*) AS n FROM orders')
    const unreachable = await caught(
      new SirannonClient(refused.base, { transport: 'http' }).database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1]),
    )
    client.close()

    expect([codeless.name, codeless.message]).toEqual(['RemoteError', expect.stringContaining('500')])
    expect(forgedEnvelope.name).not.toBe('resolved')
    expect(recovered).toEqual([{ n: storedOrders(shop.path).length }])
    expect([unreachable.name, unreachable.code]).toEqual(['RemoteError', 'CONNECTION_ERROR'])
    expect(storedCustomers(shop.path)).toEqual(['ama', 'kofi'])
  })

  it('raises INVALID_RESPONSE for a JSON answer that is not an object', async () => {
    const shop = await servedShop()
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const db = client.database(DATABASE_ID)
    await db.query('SELECT 1 AS warm')

    for (const text of ['null', '5', '"rows"', '[]']) {
      proxy.answerWith(request => (request.method === 'POST' ? { kind: 'reply', status: 200, text } : undefined))
      for (const call of [
        () => db.query('SELECT id FROM orders'),
        () => db.execute(INSERT_ORDER, ['lost', 1]),
        () => db.transaction([{ sql: INSERT_ORDER, params: ['lost', 1] }]),
      ]) {
        expect(await caught(call()), text).toMatchObject({ name: 'RemoteError', code: 'INVALID_RESPONSE' })
      }
    }
    client.close()
  })
})
