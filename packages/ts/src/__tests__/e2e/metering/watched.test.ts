import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions } from '../../../core/types.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  postJson,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
} from '../lib/served-registry.js'
import {
  billedAs,
  type Counts,
  indexesOn,
  Meter,
  meteredRegistry,
  plusCounts,
  ROWID_LOOKUP,
  SIRANNON_SQLITE_BINDING,
  TOPOLOGIES,
} from './metered-registry.js'
import { readInSeparateProcess } from './oracle.js'

const CUSTOMERS = 20
const INVOICES_PER_CUSTOMER = 5
const LEDGER_SCHEMA = [
  'CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
  'CREATE TABLE invoices (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers (id) ON DELETE CASCADE, amount INTEGER NOT NULL)',
  'CREATE INDEX invoices_by_customer ON invoices (customer_id)',
  'CREATE TABLE audit (id INTEGER PRIMARY KEY, invoice_id INTEGER NOT NULL)',
  'CREATE TRIGGER invoices_audited AFTER INSERT ON invoices BEGIN INSERT INTO audit (invoice_id) VALUES (NEW.id); END',
]
const WATCHED_TABLES = ['customers', 'invoices', 'audit']
const INSERT_CUSTOMER = 'INSERT INTO customers (name) VALUES (?)'
const INSERT_INVOICE = 'INSERT INTO invoices (customer_id, amount) VALUES (?, ?)'
const SET_AMOUNT_BY_ID = 'UPDATE invoices SET amount = ? WHERE id = ?'
const DELETE_CUSTOMER_BY_ID = 'DELETE FROM customers WHERE id = ?'
const CANCELLED_CUSTOMER = 7
const BILLED_CUSTOMER = 3

let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry
const paths = new Map<string, string>()

async function openLedger(dir: string, id: string, options: DatabaseOptions, watched: boolean): Promise<void> {
  const path = join(dir, `${id}.db`)
  paths.set(id, path)
  const db = await sirannon.open(id, path, options)
  for (const statement of LEDGER_SCHEMA) await db.execute(statement)
  if (watched) for (const table of WATCHED_TABLES) await db.watch(table)
  await db.bulkLoad(
    INSERT_CUSTOMER,
    Array.from({ length: CUSTOMERS }, (_, index) => [`customer-${index + 1}`]),
  )
  await db.bulkLoad(
    INSERT_INVOICE,
    Array.from({ length: CUSTOMERS * INVOICES_PER_CUSTOMER }, (_, index) => [(index % CUSTOMERS) + 1, index]),
  )
}

async function invoicesOf(path: string, customer: number): Promise<number> {
  const [row] = await readInSeparateProcess<{ n: bigint }>(
    path,
    'SELECT count(*) AS n FROM invoices WHERE customer_id = ?',
    [customer],
  )
  return Number(row?.n ?? -1n)
}

async function changesLogged(path: string): Promise<number> {
  const [row] = await readInSeparateProcess<{ n: bigint }>(path, 'SELECT count(*) AS n FROM _sirannon_changes')
  return Number(row?.n ?? -1n)
}

async function exercise(id: string): Promise<number[]> {
  const statuses: number[] = []
  const send = async (route: string, sql: string, params: unknown[]): Promise<void> => {
    statuses.push((await postJson(served.base, `/db/${id}/${route}`, { sql, params })).status)
  }
  await send('execute', INSERT_INVOICE, [BILLED_CUSTOMER, 500])
  await send('execute', SET_AMOUNT_BY_ID, [650, 1])
  await send('execute', DELETE_CUSTOMER_BY_ID, [CANCELLED_CUSTOMER])
  await send('execute', INSERT_INVOICE, [CUSTOMERS + 100, 1])
  return statuses
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering writes on watched tables', () => {
  afterEach(releaseServedRegistries)

  describe.each(TOPOLOGIES)('with $label', topology => {
    beforeEach(async () => {
      const dir = scratchDirectory('metered-watched')
      meter = new Meter()
      sirannon = meteredRegistry(meter)
      await openLedger(dir, 'watched', topology.options, true)
      await openLedger(dir, 'plain', topology.options, false)
      served = await serveRegistry(sirannon, { acceptSql: true })
      meter.clear()
    })

    it("bills a watched table's writes without the change log, and counts the caller's own trigger and cascade", async () => {
      const plainPath = paths.get('plain') ?? ''
      const watchedPath = paths.get('watched') ?? ''
      const invoiceEntries = 1 + (await indexesOn(plainPath, 'invoices'))
      const auditEntries = 1 + (await indexesOn(plainPath, 'audit'))
      const cascaded = await invoicesOf(plainPath, CANCELLED_CUSTOMER)
      const childPlan = await readInSeparateProcess<{ detail: string }>(
        plainPath,
        'EXPLAIN QUERY PLAN SELECT id FROM invoices WHERE customer_id = ?',
        [CANCELLED_CUSTOMER],
      )
      const loggedBefore = await changesLogged(watchedPath)

      const watchedStatuses = await exercise('watched')
      const plainStatuses = await exercise('plain')

      expect(cascaded).toBe(INVOICES_PER_CUSTOMER)
      expect(childPlan.map(step => step.detail)).toEqual([
        'SEARCH invoices USING COVERING INDEX invoices_by_customer (customer_id=?)',
      ])
      expect(watchedStatuses).toEqual([200, 200, 200, 400])
      expect(plainStatuses).toEqual(watchedStatuses)
      expect(await changesLogged(watchedPath)).toBeGreaterThan(loggedBefore)
      expect(await invoicesOf(watchedPath, CANCELLED_CUSTOMER)).toBe(0)
      const parentCheck = ROWID_LOOKUP
      const invoiceAndAudit: Counts = { rowsRead: 0, rowsWritten: invoiceEntries + auditEntries }
      const customerDeleted: Counts = { rowsRead: ROWID_LOOKUP.rowsRead, rowsWritten: 1 }
      const childRange = { rowsRead: 1 + cascaded, rowsWritten: 0 }
      const childrenDeleted: Counts = { rowsRead: cascaded, rowsWritten: cascaded }
      const childParentChecks: Counts = { rowsRead: cascaded, rowsWritten: 0 }
      const expected = [
        billedAs(INSERT_INVOICE, false, plusCounts(parentCheck, invoiceAndAudit)),
        billedAs(SET_AMOUNT_BY_ID, false, { rowsRead: ROWID_LOOKUP.rowsRead, rowsWritten: 1 }),
        billedAs(
          DELETE_CUSTOMER_BY_ID,
          false,
          plusCounts(customerDeleted, childRange, childRange, childrenDeleted, childParentChecks),
        ),
        billedAs(INSERT_INVOICE, true, plusCounts(parentCheck, invoiceAndAudit)),
      ]
      expect({ watched: meter.billed('watched'), plain: meter.billed('plain') }).toEqual({
        watched: expected,
        plain: expected,
      })
    })
  })
})
