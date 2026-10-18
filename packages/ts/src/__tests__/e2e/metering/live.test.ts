import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { LiveQuery } from '../../../core/live/types.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { QueryMetrics } from '../../../core/types.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  waitUntil,
} from '../lib/served-registry.js'
import { METERED_OPERATIONS, ORDERS_OF_CUSTOMER } from './metered-callers.js'
import {
  type Billed,
  billedAs,
  type Counts,
  INSERT_ORDER,
  indexesOn,
  Meter,
  meteredRegistry,
  openOrders,
  ROWID_LOOKUP,
  rowInserts,
  rowsIn,
  SCAN_BY_CUSTOMER,
  SET_TOTAL_BY_ID,
  SIRANNON_SQLITE_BINDING,
  scansWholeTable,
  type Tenant,
  TOPOLOGIES,
  wholeTableScan,
} from './metered-registry.js'

interface StoredOrder {
  id: number
  customer: string
  total: number
}

const SEEDED_ORDERS = 200
const WATCHED_CUSTOMER = 'acme-customer-4'
const OTHER_CUSTOMER = 'acme-customer-5'
const ORDERS_IN_ONE_TRANSACTION = 3
const ordersOfCustomer = operationRef<{ customer: string }, StoredOrder>(ORDERS_OF_CUSTOMER)

let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry
let tenant: Tenant
let client: SirannonClient

function liveReports(): QueryMetrics[] {
  return meter.of('acme').filter(metrics => metrics.source === 'live')
}

function otherReports(): QueryMetrics[] {
  return meter.of('acme').filter(metrics => metrics.source !== 'live')
}

function billedOf(reports: QueryMetrics[]): Billed[] {
  return reports.map(metrics => ({
    sql: metrics.sql,
    error: metrics.error === true,
    rowsRead: metrics.rowsRead ?? Number.NaN,
    rowsWritten: metrics.rowsWritten ?? Number.NaN,
  }))
}

function probeOf(candidateRows: number): Counts {
  return { rowsRead: candidateRows, rowsWritten: 0 }
}

function readyRows<T>(live: LiveQuery<T>): readonly T[] | null {
  const state = live.getState()
  return state.status === 'ready' && !state.revalidating ? state.rows : null
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering a live query served over WebSocket', () => {
  afterEach(async () => {
    client.close()
    await releaseServedRegistries()
  })

  describe.each(TOPOLOGIES)('with $label', topology => {
    beforeEach(async () => {
      const dir = scratchDirectory('metered-live')
      meter = new Meter()
      sirannon = meteredRegistry(meter)
      tenant = await openOrders(sirannon, dir, 'acme', SEEDED_ORDERS, topology.options)
      served = await serveRegistry(sirannon, { acceptSql: true, sharedOperations: METERED_OPERATIONS })
      client = new SirannonClient(served.base, { transport: 'websocket', autoReconnect: false })
      meter.clear()
    })

    it('bills the first read and every re-read for the whole scan and each probe for the changed rows it examined, all tagged live', async () => {
      expect(await scansWholeTable(tenant.path, SCAN_BY_CUSTOMER, [WATCHED_CUSTOMER], 'orders')).toBe(true)
      const firstRead = await wholeTableScan(tenant.path, 'orders')
      const inserted = await rowInserts(tenant.path, 'orders')
      const indexesHoldingTotal = await indexesOn(tenant.path, 'orders')
      const repriced = { rowsRead: ROWID_LOOKUP.rowsRead, rowsWritten: 1 + indexesHoldingTotal }
      const db = client.database('acme')

      const live = await db.live(ordersOfCustomer, { customer: WATCHED_CUSTOMER })
      await waitUntil(() => readyRows(live)?.length === 1, 'the first read')
      await waitUntil(() => liveReports().length === 1, 'the first read reporting')

      const placed = await db.execute(INSERT_ORDER, [WATCHED_CUSTOMER, 6])
      await waitUntil(() => readyRows(live)?.length === 2, 'the inserted order')
      await waitUntil(() => liveReports().length === 2, 'the insert probe reporting')

      await db.execute(SET_TOTAL_BY_ID, [8, placed.lastInsertRowId])
      await waitUntil(() => readyRows(live)?.some(order => Number(order.total) === 8) === true, 'the repriced order')
      await waitUntil(() => liveReports().length === 3, 'the update probe reporting')

      await db.transaction(
        Array.from({ length: ORDERS_IN_ONE_TRANSACTION }, (_, index) => ({
          sql: INSERT_ORDER,
          params: [WATCHED_CUSTOMER, index],
        })),
      )
      await waitUntil(() => readyRows(live)?.length === 2 + ORDERS_IN_ONE_TRANSACTION, 'the orders read again')
      await waitUntil(() => liveReports().length === 5, 'the probe and the re-read reporting')
      const rowsAtReread = await rowsIn(tenant.path, 'orders')

      await db.execute(INSERT_ORDER, [OTHER_CUSTOMER, 1])
      await waitUntil(() => liveReports().length === 6, 'the probe of an order outside the result reporting')
      await live.close()

      expect(rowsAtReread).toBe(SEEDED_ORDERS + 1 + ORDERS_IN_ONE_TRANSACTION)
      expect(billedOf(liveReports())).toEqual([
        billedAs(SCAN_BY_CUSTOMER, false, firstRead),
        billedAs(SCAN_BY_CUSTOMER, false, probeOf(1)),
        billedAs(SCAN_BY_CUSTOMER, false, probeOf(2)),
        billedAs(SCAN_BY_CUSTOMER, false, probeOf(ORDERS_IN_ONE_TRANSACTION)),
        billedAs(SCAN_BY_CUSTOMER, false, { rowsRead: rowsAtReread, rowsWritten: 0 }),
        billedAs(SCAN_BY_CUSTOMER, false, probeOf(1)),
      ])
      expect(billedOf(otherReports())).toEqual([
        billedAs(INSERT_ORDER, false, inserted),
        billedAs(SET_TOTAL_BY_ID, false, repriced),
        ...Array.from({ length: ORDERS_IN_ONE_TRANSACTION }, () => billedAs(INSERT_ORDER, false, inserted)),
        billedAs(INSERT_ORDER, false, inserted),
      ])
      for (const report of otherReports()) expect(report).not.toHaveProperty('source')
    })
  })
})
