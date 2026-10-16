import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import { crashesAfterSqlite, meteringFaultDriver } from '../../helpers/fault-drivers/metering-fault-driver.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import { addLabels, openBackedUp, restoreSettled, triggerAndSettle } from '../backups-over-server/served-backups.js'
import {
  errorCodeOf,
  postJson,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
} from '../lib/served-registry.js'
import {
  billedAs,
  INSERT_ORDER,
  Meter,
  meteredRegistry,
  openOrders,
  rowInserts,
  rowsIn,
  SCAN_BY_CUSTOMER,
  SIRANNON_SQLITE_BINDING,
  scansWholeTable,
  type Tenant,
  TOPOLOGIES,
  wholeTableScan,
} from './metered-registry.js'

const SEEDED_ORDERS = 150
const CRASHING_SETS = 40
const LABELS_BY_NOTE = 'SELECT label FROM orders WHERE note = ?'
const INSERT_LABEL = 'INSERT INTO orders (label, note) VALUES (?, ?)'
const OPERATOR = { authenticate: () => ({ operator: true }) }

let dir: string
let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering across restarts', () => {
  beforeEach(() => {
    dir = scratchDirectory('metered-restarts')
    meter = new Meter()
  })

  afterEach(releaseServedRegistries)

  describe.each(TOPOLOGIES)('with $label', topology => {
    it('bills every request exactly when the database closes and opens again between requests', async () => {
      sirannon = meteredRegistry(meter)
      const tenant = await openOrders(sirannon, dir, 'acme', SEEDED_ORDERS, topology.options)
      served = await serveRegistry(sirannon, { acceptSql: true })
      expect(await scansWholeTable(tenant.path, SCAN_BY_CUSTOMER, ['x'], 'orders')).toBe(true)
      const inserted = await rowInserts(tenant.path, 'orders')
      meter.clear()

      const before = await postJson(served.base, '/db/acme/execute', { sql: INSERT_ORDER, params: ['acme-before', 1] })
      await sirannon.close('acme')
      const whileClosed = await postJson(served.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: ['x'] })
      await sirannon.open('acme', tenant.path, topology.options)
      const scanned = await wholeTableScan(tenant.path, 'orders')
      const after = await postJson(served.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: ['x'] })

      expect([before.status, whileClosed.status, after.status]).toEqual([200, 404, 200])
      expect(scanned.rowsRead).toBe(SEEDED_ORDERS + 1)
      expect(meter.billed('acme')).toEqual([
        billedAs(INSERT_ORDER, false, inserted),
        billedAs(SCAN_BY_CUSTOMER, false, scanned),
      ])
    })
  })

  it('reports no counts for a batch whose writer worker dies inside it, commits none of it, and bills every later statement exactly', async () => {
    sirannon = meteredRegistry(meter, {
      driver: meteringFaultDriver({ sqliteBindingPath: SIRANNON_SQLITE_BINDING ?? '' }),
    })
    const tenant: Tenant = await openOrders(sirannon, dir, 'acme', SEEDED_ORDERS, {
      writerWorker: true,
      readPoolSize: 1,
    })
    served = await serveRegistry(sirannon, { acceptSql: true })
    const crashingInsert = crashesAfterSqlite(INSERT_ORDER)
    const inserted = await rowInserts(tenant.path, 'orders')
    meter.clear()

    const crashed = await postJson(served.base, '/db/acme/batch', {
      sql: crashingInsert,
      paramsBatch: Array.from({ length: CRASHING_SETS }, (_, index) => [`acme-crash-${index}`, index]),
    })
    const scanned = await wholeTableScan(tenant.path, 'orders')
    const later = await postJson(served.base, '/db/acme/execute', { sql: INSERT_ORDER, params: ['acme-later', 2] })
    const scan = await postJson(served.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: ['x'] })

    expect([crashed.status, errorCodeOf(crashed.body)]).toEqual([500, 'WRITER_WORKER_EXIT'])
    expect([later.status, scan.status]).toEqual([200, 200])
    expect(scanned.rowsRead).toBe(SEEDED_ORDERS)
    expect(await rowsIn(tenant.path, 'orders')).toBe(SEEDED_ORDERS + 1)
    const [crashReport, ...rest] = meter.of('acme')
    expect(crashReport).toMatchObject({ sql: crashingInsert, error: true })
    expect(crashReport).not.toHaveProperty('rowsRead')
    expect(crashReport).not.toHaveProperty('rowsWritten')
    expect(meter.billed('acme').slice(1)).toEqual([
      billedAs(INSERT_ORDER, false, inserted),
      billedAs(SCAN_BY_CUSTOMER, false, { rowsRead: SEEDED_ORDERS + 1, rowsWritten: 0 }),
    ])
    expect(rest).toHaveLength(2)
  })

  it('reports nothing while a backup restore rebuilds the database, and bills the restored rows exactly afterwards', async () => {
    sirannon = meteredRegistry(meter)
    const orders = await openBackedUp(sirannon, dir, 'orders')
    served = await serveRegistry(sirannon, { ...OPERATOR, acceptSql: true, acceptBackupRestore: true })
    await addLabels(orders.db, ['first', 'second', 'third'])
    await triggerAndSettle(served.base, orders, 'orders')
    await addLabels(orders.db, ['after-backup'])
    expect(await scansWholeTable(orders.path, LABELS_BY_NOTE, ['x'], 'orders')).toBe(true)
    meter.clear()

    const accepted = await postJson(served.base, '/db/orders/backup/restore', {})
    const settled = await restoreSettled(served.base, 'orders')
    const reportedDuringRestore = meter.of('orders').length
    const restoredRows = await rowsIn(orders.path, 'orders')
    const scan = await postJson(served.base, '/db/orders/query', { sql: LABELS_BY_NOTE, params: ['x'] })
    const insert = await postJson(served.base, '/db/orders/execute', { sql: INSERT_LABEL, params: ['restored', ''] })

    expect(accepted.status).toBe(202)
    expect(settled).toMatchObject({ state: 'done' })
    expect(settled.reopenError).toBeUndefined()
    expect(reportedDuringRestore).toBe(0)
    expect(restoredRows).toBeGreaterThanOrEqual(3)
    expect([scan.status, insert.status]).toEqual([200, 200])
    expect(meter.billed('orders')).toEqual([
      billedAs(LABELS_BY_NOTE, false, { rowsRead: restoredRows, rowsWritten: 0 }),
      billedAs(INSERT_LABEL, false, await rowInserts(orders.path, 'orders')),
    ])
  })
})
