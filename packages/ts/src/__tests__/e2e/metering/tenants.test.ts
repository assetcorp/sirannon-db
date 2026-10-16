import { afterEach, describe, expect, it } from 'vitest'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import { releaseServedRegistries, scratchDirectory, serveRegistry } from '../lib/served-registry.js'
import { type Answer, type Caller, clientCaller, httpCaller, webSocketCaller } from './metered-callers.js'
import {
  type Billed,
  billedAs,
  Meter,
  meteredRegistry,
  openOrders,
  rowInserts,
  SCAN_BY_CUSTOMER,
  SIRANNON_SQLITE_BINDING,
  scansWholeTable,
  type Tenant,
  TOPOLOGIES,
  wholeTableScan,
} from './metered-registry.js'

const TENANTS = 12
const SCANS_PER_TENANT = 15
const WRITES_PER_TENANT = 15
const CREATE_EVENTS = 'CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL)'
const INSERT_EVENT = 'INSERT INTO events (kind) VALUES (?)'

function bySql(left: Billed, right: Billed): number {
  return left.sql.localeCompare(right.sql) || left.rowsRead - right.rowsRead
}

async function callerFor(index: number, base: string, wsBase: string, id: string): Promise<Caller> {
  switch (index % 4) {
    case 0:
      return httpCaller(base, id)
    case 1:
      return webSocketCaller(wsBase, id)
    case 2:
      return clientCaller(base, id, 'http')
    default:
      return clientCaller(base, id, 'websocket')
  }
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering many tenants at once', () => {
  afterEach(releaseServedRegistries)

  it('bills every tenant exactly for its own statements when every tenant sends its requests at the same moment', async () => {
    const dir = scratchDirectory('metered-tenants')
    const meter = new Meter()
    const sirannon = meteredRegistry(meter)
    const tenants: Tenant[] = []
    for (let index = 0; index < TENANTS; index++) {
      const topology = TOPOLOGIES[index % TOPOLOGIES.length]
      const tenant = await openOrders(sirannon, dir, `tenant-${index}`, 40 + index * 17, topology?.options)
      await tenant.db.execute(CREATE_EVENTS)
      tenants.push(tenant)
    }
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const expected = new Map<string, Billed[]>()
    for (const tenant of tenants) {
      expect(await scansWholeTable(tenant.path, SCAN_BY_CUSTOMER, ['nobody'], 'orders')).toBe(true)
      const scanned = await wholeTableScan(tenant.path, 'orders')
      const inserted = await rowInserts(tenant.path, 'events')
      expected.set(
        tenant.id,
        [
          ...Array.from({ length: SCANS_PER_TENANT }, () => billedAs(SCAN_BY_CUSTOMER, false, scanned)),
          ...Array.from({ length: WRITES_PER_TENANT }, () => billedAs(INSERT_EVENT, false, inserted)),
        ].sort(bySql),
      )
    }
    const callers = await Promise.all(
      tenants.map((tenant, index) => callerFor(index, served.base, served.wsBase, tenant.id)),
    )
    meter.clear()

    const answers: Answer[] = await Promise.all(
      tenants.flatMap((tenant, index) => {
        const call = callers[index] as Caller
        return Array.from({ length: SCANS_PER_TENANT + WRITES_PER_TENANT }, (_, step) =>
          step % 2 === 0
            ? call.query(SCAN_BY_CUSTOMER, [`${tenant.id}-nobody`])
            : call.execute(INSERT_EVENT, [`${tenant.id}-${step}`]),
        )
      }),
    )
    for (const call of callers) call.close()

    expect(answers.filter(answer => !answer.ok)).toEqual([])
    expect(meter.reports).toHaveLength(TENANTS * (SCANS_PER_TENANT + WRITES_PER_TENANT))
    for (const tenant of tenants) {
      expect(meter.billed(tenant.id).sort(bySql), tenant.id).toEqual(expected.get(tenant.id))
    }
  })
})
