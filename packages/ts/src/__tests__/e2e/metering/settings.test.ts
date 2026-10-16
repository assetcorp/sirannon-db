import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { LiveQuery } from '../../../core/live/types.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions } from '../../../core/types.js'
import { type BetterSqlite3Options, betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { stockBetterSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
  waitUntil,
} from '../lib/served-registry.js'
import {
  type Caller,
  clientCaller,
  httpCaller,
  METERED_OPERATIONS,
  ORDERS_OF_CUSTOMER,
  PLACE_ORDER,
} from './metered-callers.js'
import { INSERT_ORDER, Meter, ORDERS_SCHEMA, SCAN_BY_CUSTOMER, TOPOLOGIES } from './metered-registry.js'

interface StoredOrder {
  id: number
  customer: string
  total: number
}

const ordersOfCustomer = operationRef<{ customer: string }, StoredOrder>(ORDERS_OF_CUSTOMER)

let dir: string
let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry

function servedLazily(driverOptions: BetterSqlite3Options, options: DatabaseOptions = {}): Sirannon {
  return trackedRegistry({
    driver: betterSqlite3(driverOptions),
    metrics: { onQueryComplete: meter.record },
    lifecycle: { autoOpen: { resolver: id => ({ path: join(dir, `${id}.db`), options }) } },
  })
}

function readyRows<T>(live: LiveQuery<T>): readonly T[] | null {
  const state = live.getState()
  return state.status === 'ready' && !state.revalidating ? state.rows : null
}

async function everyRoute(call: Caller): Promise<boolean[]> {
  const answers = [
    await call.query(SCAN_BY_CUSTOMER, ['acme-customer-1']),
    await call.execute(INSERT_ORDER, ['acme-customer-1', 3]),
    await call.transaction([
      { sql: INSERT_ORDER, params: ['acme-customer-2', 4] },
      { sql: 'UPDATE orders SET total = 5 WHERE id = ?', params: [1] },
    ]),
    await call.transaction([
      { sql: INSERT_ORDER, params: ['acme-customer-3', 4] },
      { sql: INSERT_ORDER, params: ['acme-customer-3', null] },
    ]),
    await call.batch(INSERT_ORDER, [
      ['acme-batch', 1],
      ['acme-batch', 2],
    ]),
    await call.load(INSERT_ORDER, [
      ['acme-load', 1],
      ['acme-load', null],
    ]),
    await call.namedQuery(ORDERS_OF_CUSTOMER, { customer: 'acme-customer-1' }),
    await call.namedExecute(PLACE_ORDER, { customer: 'acme-named', total: 6 }),
    await call.query('SELECT missing_column FROM orders'),
  ]
  return answers.map(answer => answer.ok)
}

describe('metering a served registry under bad row count settings', () => {
  beforeEach(() => {
    dir = scratchDirectory('metered-settings')
    meter = new Meter()
  })

  afterEach(releaseServedRegistries)

  it.each([
    ['the stock better-sqlite3 build, which has no row counters', () => stockBetterSqliteBindingPath()],
    ['a binding path that names a missing file', () => join(dir, 'missing.node')],
    [
      'a binding path that names a file that is no native module',
      () => {
        const notAModule = join(dir, 'not-a-module.node')
        writeFileSync(notAModule, 'these bytes are not a native module')
        return notAModule
      },
    ],
  ])('refuses every route with ROW_COUNTS_UNSUPPORTED and reports nothing when rowCounts meets %s', async (_, bindingPath) => {
    sirannon = servedLazily({ rowCounts: true, sqliteBindingPath: bindingPath() })
    served = await serveRegistry(sirannon, { acceptSql: true, sharedOperations: METERED_OPERATIONS })

    const answers = [
      await postJson(served.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: ['x'] }),
      await postJson(served.base, '/db/acme/execute', { sql: INSERT_ORDER, params: ['x', 1] }),
      await postJson(served.base, '/db/acme/batch', { sql: INSERT_ORDER, paramsBatch: [['x', 1]] }),
      await postJson(served.base, `/db/acme/query/${ORDERS_OF_CUSTOMER}`, { args: { customer: 'x' } }),
    ]
    const ws = await RawSocket.open(`${served.wsBase}/db/acme`)
    const refusal = await ws.next()
    const closed = await ws.closed

    for (const answer of answers)
      expect([answer.status, errorCodeOf(answer.body)]).toEqual([500, 'ROW_COUNTS_UNSUPPORTED'])
    expect(refusal.error?.code).toBe('ROW_COUNTS_UNSUPPORTED')
    expect(closed.code).toBe(1011)
    expect(meter.reports).toEqual([])
  })

  describe.each(TOPOLOGIES)('with rowCounts off and $label', topology => {
    beforeEach(async () => {
      sirannon = servedLazily({}, topology.options)
      const db = await sirannon.resolve('acme')
      if (db === undefined) throw new Error('The resolver opened no database for acme')
      for (const statement of ORDERS_SCHEMA) await db.execute(statement)
      await db.bulkLoad(
        INSERT_ORDER,
        Array.from({ length: 50 }, (_, index) => [`acme-customer-${index}`, index]),
      )
      served = await serveRegistry(sirannon, { acceptSql: true, sharedOperations: METERED_OPERATIONS })
      meter.clear()
    })

    it('leaves the counts out of every report on every route, including failures and live reads, and never reports zero', async () => {
      const outcomes = [
        await everyRoute(httpCaller(served.base, 'acme')),
        await everyRoute(clientCaller(served.base, 'acme', 'websocket')),
      ]
      const client = new SirannonClient(served.base, { transport: 'websocket', autoReconnect: false })
      const live = await client.database('acme').live(ordersOfCustomer, { customer: 'acme-live' })
      await waitUntil(() => readyRows(live)?.length === 0, 'the first live read')
      await client.database('acme').execute(INSERT_ORDER, ['acme-live', 1])
      await waitUntil(() => readyRows(live)?.length === 1, 'the live probe')
      await live.close()
      client.close()

      const expectedOutcomes = [true, true, true, false, true, false, true, true, false]
      expect(outcomes).toEqual([expectedOutcomes, expectedOutcomes])
      expect(meter.reports.some(metrics => metrics.source === 'live')).toBe(true)
      expect(meter.reports.some(metrics => metrics.error === true)).toBe(true)
      for (const metrics of meter.reports) {
        expect(metrics).not.toHaveProperty('rowsRead')
        expect(metrics).not.toHaveProperty('rowsWritten')
      }
    })
  })
})
