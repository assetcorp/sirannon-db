import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import {
  heldAfterSqlite,
  heldStatementReached,
  meteringFaultDriver,
  releaseHeldStatement,
} from '../../helpers/fault-drivers/metering-fault-driver.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  connectRaw,
  httpRequestText,
  postJson,
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  waitUntil,
} from '../lib/served-registry.js'
import {
  type Billed,
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
  wholeTableScan,
} from './metered-registry.js'

const SEEDED_ORDERS = 300
const REPRICE_NOBODY = 'UPDATE orders SET total = total + 1 WHERE customer = ?'
const QUEUED_WRITES = 3

let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry
let tenant: Tenant
let releaseFile: string

function bySql(left: Billed, right: Billed): number {
  return left.sql.localeCompare(right.sql)
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering requests whose client goes away', () => {
  beforeEach(async () => {
    const dir = scratchDirectory('metered-disconnects')
    releaseFile = join(dir, 'release')
    meter = new Meter()
    sirannon = meteredRegistry(meter, {
      driver: meteringFaultDriver({ sqliteBindingPath: SIRANNON_SQLITE_BINDING ?? '' }),
    })
    tenant = await openOrders(sirannon, dir, 'acme', SEEDED_ORDERS, { writerWorker: true, readPoolSize: 1 })
    served = await serveRegistry(sirannon, { acceptSql: true })
    meter.clear()
  })

  afterEach(async () => {
    releaseHeldStatement(releaseFile)
    await releaseServedRegistries()
  })

  it('bills a write for every row and index entry it wrote when its HTTP client hangs up while SQLite holds it', async () => {
    const heldInsert = heldAfterSqlite(INSERT_ORDER, releaseFile)
    const expected = await rowInserts(tenant.path, 'orders')
    const client = await connectRaw(served.port)

    client.socket.write(
      httpRequestText('POST', '/db/acme/execute', JSON.stringify({ sql: heldInsert, params: ['acme-gone', 7] })),
    )
    await waitUntil(() => heldStatementReached(releaseFile), 'the held write reaching SQLite')
    client.socket.destroy()
    await client.closed
    releaseHeldStatement(releaseFile)
    await waitUntil(() => meter.reports.length === 1, 'the held write reporting')

    expect(client.received()).toBe('')
    expect(await rowsIn(tenant.path, 'orders')).toBe(SEEDED_ORDERS + 1)
    expect(meter.billed('acme')).toEqual([billedAs(heldInsert, false, expected)])
  })

  it('bills a long scan for every row it read when its HTTP client hangs up before the scan returns', async () => {
    const heldScan = heldAfterSqlite(REPRICE_NOBODY, releaseFile)
    expect(await scansWholeTable(tenant.path, REPRICE_NOBODY, ['nobody'], 'orders')).toBe(true)
    const expected = await wholeTableScan(tenant.path, 'orders')
    const client = await connectRaw(served.port)

    client.socket.write(
      httpRequestText(
        'POST',
        '/db/acme/transaction',
        JSON.stringify({ statements: [{ sql: heldScan, params: ['nobody'] }] }),
      ),
    )
    await waitUntil(() => heldStatementReached(releaseFile), 'the held scan reaching SQLite')
    client.socket.destroy()
    await client.closed
    releaseHeldStatement(releaseFile)
    await waitUntil(() => meter.reports.length === 1, 'the held scan reporting')

    expect(client.received()).toBe('')
    expect(meter.billed('acme')).toEqual([billedAs(heldScan, false, expected)])
  })

  it('bills every statement that a WebSocket sent before it closed, exactly once, including the ones still queued behind a held write', async () => {
    const heldInsert = heldAfterSqlite(INSERT_ORDER, releaseFile)
    expect(await scansWholeTable(tenant.path, SCAN_BY_CUSTOMER, ['acme-customer-4'], 'orders')).toBe(true)
    const inserted = await rowInserts(tenant.path, 'orders')
    const ws = await RawSocket.open(`${served.wsBase}/db/acme`)

    ws.sendText(JSON.stringify({ id: 'held', type: 'execute', sql: heldInsert, params: ['acme-held', 1] }))
    for (let index = 0; index < QUEUED_WRITES; index++) {
      ws.sendText(
        JSON.stringify({ id: `queued-${index}`, type: 'execute', sql: INSERT_ORDER, params: [`q-${index}`, 2] }),
      )
    }
    await waitUntil(() => heldStatementReached(releaseFile), 'the held write reaching SQLite')
    const scanned = await wholeTableScan(tenant.path, 'orders')
    ws.sendText(JSON.stringify({ id: 'scan', type: 'query', sql: SCAN_BY_CUSTOMER, params: ['acme-customer-4'] }))
    await waitUntil(() => meter.reports.length === 1, 'the scan beside the held write reporting')
    expect(scanned.rowsRead).toBe(SEEDED_ORDERS + 1)
    ws.close()
    await ws.closed
    releaseHeldStatement(releaseFile)
    await waitUntil(() => meter.reports.length === QUEUED_WRITES + 2, 'every sent statement reporting')

    expect(await rowsIn(tenant.path, 'orders')).toBe(SEEDED_ORDERS + QUEUED_WRITES + 1)
    expect(meter.billed('acme').sort(bySql)).toEqual(
      [
        billedAs(SCAN_BY_CUSTOMER, false, scanned),
        billedAs(heldInsert, false, inserted),
        ...Array.from({ length: QUEUED_WRITES }, () => billedAs(INSERT_ORDER, false, inserted)),
      ].sort(bySql),
    )
  })

  it('bills a write that a closing server lets finish, nothing for an upload it cuts, and the next server exactly', async () => {
    const heldInsert = heldAfterSqlite(INSERT_ORDER, releaseFile)
    const inserted = await rowInserts(tenant.path, 'orders')
    const finishing = postJson(served.base, '/db/acme/execute', { sql: heldInsert, params: ['acme-finishing', 3] })
    await waitUntil(() => heldStatementReached(releaseFile), 'the held write reaching SQLite')
    const uploader = await connectRaw(served.port)
    const upload = httpRequestText(
      'POST',
      '/db/acme/execute',
      JSON.stringify({ sql: INSERT_ORDER, params: ['acme-cut-upload', 5] }),
    )
    uploader.socket.write(upload.slice(0, upload.length - 10))

    const closing = served.server.close()
    await uploader.closed
    releaseHeldStatement(releaseFile)
    const finished = await finishing
    await closing
    const reopened = await serveRegistry(sirannon, { acceptSql: true })
    const scannedAfter = await wholeTableScan(tenant.path, 'orders')
    const scan = await postJson(reopened.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: ['acme-customer-9'] })
    const insert = await postJson(reopened.base, '/db/acme/execute', { sql: INSERT_ORDER, params: ['acme-after', 4] })

    expect(uploader.received()).not.toMatch(/HTTP\/1\.1 2\d\d/)
    expect([finished.status, scan.status, insert.status]).toEqual([200, 200, 200])
    expect(scannedAfter.rowsRead).toBe(SEEDED_ORDERS + 1)
    expect(await rowsIn(tenant.path, 'orders')).toBe(SEEDED_ORDERS + 2)
    expect(meter.billed('acme')).toEqual([
      billedAs(heldInsert, false, inserted),
      billedAs(SCAN_BY_CUSTOMER, false, scannedAfter),
      billedAs(INSERT_ORDER, false, inserted),
    ])
  })
})
