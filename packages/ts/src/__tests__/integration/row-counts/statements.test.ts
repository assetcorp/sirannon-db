import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { QueryMetrics } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { heldSql, releaseHeldWrite, sleepingDriver } from '../../helpers/fault-drivers/sleeping-driver.js'
import { builtSirannonSqliteBindingPath, SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'

const SEEDED_ORDERS = 1000
const DISTINCT_TOTALS = 100
const ORDERS_PER_TOTAL = SEEDED_ORDERS / DISTINCT_TOTALS
const LAST_CUSTOMER = `customer-${SEEDED_ORDERS - 1}`
const CONCURRENT_READS = 40
const CONCURRENT_WRITES = 12
const HELD_WRITE_DEADLINE_MS = 500

const SCAN_BY_CUSTOMER = 'SELECT * FROM orders WHERE customer = ?'
const ORDER_BY_ID = 'SELECT * FROM orders WHERE id = ?'
const ORDERS_UP_TO = 'SELECT id FROM orders WHERE id <= ?'
const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const BLANK_TOTAL_BY_CUSTOMER = 'UPDATE orders SET total = NULL WHERE customer = ?'

const binding = builtSirannonSqliteBindingPath()

interface Topology {
  label: string
  writerWorker: boolean
  readPoolSize: number
}

const TOPOLOGIES: Topology[] = [
  { label: 'writes and reads on the calling thread', writerWorker: false, readPoolSize: 2 },
  { label: 'writes on the writer worker', writerWorker: true, readPoolSize: 2 },
  { label: 'every statement on the writer worker', writerWorker: true, readPoolSize: 0 },
]

let dir: string
let sirannon: Sirannon
let reported: QueryMetrics[]

function reportsFor(sql: string): QueryMetrics[] {
  return reported.filter(metrics => metrics.sql === sql)
}

function onlyReportFor(sql: string): QueryMetrics {
  const matching = reportsFor(sql)
  expect(matching).toHaveLength(1)
  return matching[0]
}

async function openOrders(topology: Topology, name = 'tenant'): Promise<Database> {
  const db = await sirannon.open(name, join(dir, `${name}.db`), {
    writerWorker: topology.writerWorker,
    readPoolSize: topology.readPoolSize,
  })
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)')
  await db.execute('CREATE INDEX orders_by_total ON orders (total)')
  await db.bulkLoad(
    INSERT_ORDER,
    Array.from({ length: SEEDED_ORDERS }, (_, index) => [`customer-${index}`, index % DISTINCT_TOTALS]),
  )
  return db
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe.skipIf(binding === null)('rows read and rows written in onQueryComplete', () => {
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-row-counts-'))
    reported = []
    sirannon = new Sirannon({
      driver: betterSqlite3({ rowCounts: true, sqliteBindingPath: binding ?? undefined }),
      metrics: { onQueryComplete: metrics => reported.push(metrics) },
    })
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  describe.each(TOPOLOGIES)('with $label', topology => {
    it('bills a scan for every row it reads and a primary-key lookup for one row', async () => {
      const db = await openOrders(topology)
      expect(onlyReportFor(INSERT_ORDER)).toMatchObject({
        changes: SEEDED_ORDERS,
        rowsRead: 0,
        rowsWritten: SEEDED_ORDERS * 2,
      })

      expect(await db.query(SCAN_BY_CUSTOMER, [LAST_CUSTOMER])).toHaveLength(1)
      expect(await db.queryOne(ORDER_BY_ID, [7])).toMatchObject({ id: 7 })

      expect(onlyReportFor(SCAN_BY_CUSTOMER)).toMatchObject({
        rowsReturned: 1,
        rowsRead: SEEDED_ORDERS,
        rowsWritten: 0,
        error: false,
      })
      expect(onlyReportFor(ORDER_BY_ID)).toMatchObject({ rowsReturned: 1, rowsRead: 1, rowsWritten: 0 })
    })

    it('counts each table row and each index entry that a write inserts, updates, or deletes', async () => {
      const db = await openOrders(topology)
      const inserted = await db.execute(INSERT_ORDER, ['new-customer', 5])
      await db.execute('UPDATE orders SET customer = ? WHERE id = ?', ['renamed', inserted.lastInsertRowId])
      await db.execute('UPDATE orders SET total = ? WHERE id = ?', [6, inserted.lastInsertRowId])
      await db.execute('DELETE FROM orders WHERE id = ?', [inserted.lastInsertRowId])

      expect(inserted).toEqual({ changes: 1, lastInsertRowId: SEEDED_ORDERS + 1 })
      expect(reportsFor(INSERT_ORDER).at(-1)).toMatchObject({ changes: 1, rowsRead: 0, rowsWritten: 2 })
      expect(onlyReportFor('UPDATE orders SET customer = ? WHERE id = ?')).toMatchObject({
        rowsRead: 1,
        rowsWritten: 1,
      })
      expect(onlyReportFor('UPDATE orders SET total = ? WHERE id = ?')).toMatchObject({ rowsRead: 1, rowsWritten: 2 })
      expect(onlyReportFor('DELETE FROM orders WHERE id = ?')).toMatchObject({ rowsRead: 1, rowsWritten: 1 })
    })

    it('bills a statement that fails for the rows it read before the failure', async () => {
      const db = await openOrders(topology)
      await expect(db.execute(BLANK_TOTAL_BY_CUSTOMER, [LAST_CUSTOMER])).rejects.toThrow(/NOT NULL/)

      expect(onlyReportFor(BLANK_TOTAL_BY_CUSTOMER)).toMatchObject({
        error: true,
        rowsRead: SEEDED_ORDERS,
        rowsWritten: 0,
      })
      expect(onlyReportFor(BLANK_TOTAL_BY_CUSTOMER).changes).toBeUndefined()
    })

    it('reports each concurrent read with its own counts while the reads share one cached statement', async () => {
      const db = await openOrders(topology)
      const limits = Array.from({ length: CONCURRENT_READS }, (_, index) => 1 + ((index * 97) % (SEEDED_ORDERS - 1)))
      await Promise.all(limits.map(limit => db.query(ORDERS_UP_TO, [limit])))

      const reads = reportsFor(ORDERS_UP_TO)
      expect(reads).toHaveLength(CONCURRENT_READS)
      expect(reads.map(read => read.rowsReturned).sort((a, b) => (a ?? 0) - (b ?? 0))).toEqual(
        [...limits].sort((a, b) => a - b),
      )
      for (const read of reads) expect(read.rowsRead).toBe((read.rowsReturned ?? 0) + 1)
    })

    it('reports only the attempt that took effect when a failed write forces a grouped commit to execute again', async () => {
      const db = await openOrders(topology)
      const writes = Array.from({ length: CONCURRENT_WRITES }, (_, index) =>
        index === CONCURRENT_WRITES / 2
          ? db.execute(INSERT_ORDER, ['rejected', null])
          : db.execute(INSERT_ORDER, [`grouped-${index}`, index]),
      )
      const settled = await Promise.allSettled(writes)

      expect(settled.filter(outcome => outcome.status === 'rejected')).toHaveLength(1)
      const groupedReports = reportsFor(INSERT_ORDER).slice(1)
      expect(groupedReports).toHaveLength(CONCURRENT_WRITES)
      expect(groupedReports.filter(report => report.error)).toEqual([
        expect.objectContaining({ rowsRead: 0, rowsWritten: 0 }),
      ])
      for (const report of groupedReports.filter(candidate => !candidate.error)) {
        expect(report).toMatchObject({ changes: 1, rowsRead: 0, rowsWritten: 2 })
      }
      expect(await db.queryOne<{ n: number }>('SELECT count(*) AS n FROM orders')).toEqual({
        n: SEEDED_ORDERS + CONCURRENT_WRITES - 1,
      })
    })

    it('reports each statement of a transaction, including those of a transaction that fails', async () => {
      const db = await openOrders(topology)
      await db.executeTransaction([
        { sql: INSERT_ORDER, params: ['first', 1] },
        { sql: 'UPDATE orders SET customer = ? WHERE total = ?', params: ['bulk', 3] },
      ])
      expect(reportsFor(INSERT_ORDER).at(-1)).toMatchObject({ changes: 1, rowsRead: 0, rowsWritten: 2 })
      expect(onlyReportFor('UPDATE orders SET customer = ? WHERE total = ?')).toMatchObject({
        changes: ORDERS_PER_TOTAL,
        rowsRead: ORDERS_PER_TOTAL + 1,
        rowsWritten: ORDERS_PER_TOTAL,
      })

      reported = []
      await expect(
        db.executeTransaction([
          { sql: INSERT_ORDER, params: ['rolled-back', 1] },
          { sql: BLANK_TOTAL_BY_CUSTOMER, params: [LAST_CUSTOMER] },
          { sql: 'DELETE FROM orders WHERE id = ?', params: [1] },
        ]),
      ).rejects.toThrow(/NOT NULL/)
      expect(reported.map(({ sql, error, rowsRead, rowsWritten }) => ({ sql, error, rowsRead, rowsWritten }))).toEqual([
        { sql: INSERT_ORDER, error: true, rowsRead: 0, rowsWritten: 2 },
        { sql: BLANK_TOTAL_BY_CUSTOMER, error: true, rowsRead: SEEDED_ORDERS, rowsWritten: 0 },
        { sql: 'DELETE FROM orders WHERE id = ?', error: true, rowsRead: 0, rowsWritten: 0 },
      ])
    })

    it('reports the statements that a transaction callback executes', async () => {
      const db = await openOrders(topology)
      const found = await db.transaction(async tx => {
        await tx.execute(INSERT_ORDER, ['inside', 9])
        return tx.query(SCAN_BY_CUSTOMER, ['inside'])
      })

      expect(found).toHaveLength(1)
      expect(reportsFor(INSERT_ORDER).at(-1)).toMatchObject({ changes: 1, rowsRead: 0, rowsWritten: 2 })
      expect(onlyReportFor(SCAN_BY_CUSTOMER)).toMatchObject({ rowsReturned: 1, rowsRead: SEEDED_ORDERS + 1 })
    })

    it('totals every parameter set of a batch', async () => {
      const db = await openOrders(topology)
      const results = await db.executeBatch('UPDATE orders SET customer = ? WHERE total = ?', [
        ['batch-a', 1],
        ['batch-b', 2],
      ])

      expect(results).toEqual([
        { changes: ORDERS_PER_TOTAL, lastInsertRowId: SEEDED_ORDERS },
        { changes: ORDERS_PER_TOTAL, lastInsertRowId: SEEDED_ORDERS },
      ])
      expect(onlyReportFor('UPDATE orders SET customer = ? WHERE total = ?')).toMatchObject({
        changes: ORDERS_PER_TOTAL * 2,
        rowsRead: (ORDERS_PER_TOTAL + 1) * 2,
        rowsWritten: ORDERS_PER_TOTAL * 2,
      })
    })

    it('bills a batch that fails partway for every parameter set it executed', async () => {
      const db = await openOrders(topology)
      await expect(db.executeBatch(BLANK_TOTAL_BY_CUSTOMER, [['no-such-customer'], [LAST_CUSTOMER]])).rejects.toThrow(
        /NOT NULL/,
      )

      expect(onlyReportFor(BLANK_TOTAL_BY_CUSTOMER)).toMatchObject({
        error: true,
        rowsRead: SEEDED_ORDERS * 2,
        rowsWritten: 0,
      })
    })

    it("leaves Sirannon's change capture out of a watched table's counts and keeps the caller's triggers in", async () => {
      const db = await openOrders(topology)
      await db.execute('CREATE TABLE audit (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL)')
      await db.execute(
        'CREATE TRIGGER orders_audit AFTER INSERT ON orders BEGIN INSERT INTO audit (order_id) VALUES (NEW.id); END',
      )
      await db.execute(INSERT_ORDER, ['before-watch', 1])
      await db.watch('orders')
      await db.execute(INSERT_ORDER, ['after-watch', 1])

      const [beforeWatch, afterWatch] = reportsFor(INSERT_ORDER).slice(1)
      expect(beforeWatch).toMatchObject({ rowsRead: 0, rowsWritten: 3 })
      expect(afterWatch).toMatchObject({ rowsRead: 0, rowsWritten: 3 })
      expect(await db.queryOne('SELECT count(*) AS n FROM audit')).toEqual({ n: 2 })
    })

    it('bills an update that changes no key column of a watched table the same as before the watch', async () => {
      const db = await openOrders(topology)
      await db.execute('CREATE UNIQUE INDEX orders_by_customer ON orders (customer)')
      const updates: Array<[string, unknown[]]> = [
        ['UPDATE orders SET total = ? WHERE id = ?', [7, 3]],
        ['UPDATE orders SET total = total + ? WHERE customer = ?', [1, LAST_CUSTOMER]],
        ['UPDATE OR REPLACE orders SET total = ? WHERE id <= ?', [9, 4]],
      ]
      for (const [sql, params] of updates) await db.execute(sql, params)
      await db.watch('orders')
      for (const [sql, params] of updates) await db.execute(sql, params)

      for (const [sql] of updates) {
        const [beforeWatch, afterWatch] = reportsFor(sql)
        expect(afterWatch.rowsRead, sql).toBe(beforeWatch.rowsRead)
        expect(afterWatch.rowsWritten, sql).toBe(beforeWatch.rowsWritten)
      }
    })

    it('counts the rows that a foreign-key cascade deletes', async () => {
      const db = await openOrders(topology)
      await db.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY)')
      await db.execute(
        'CREATE TABLE invoices (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers (id) ON DELETE CASCADE)',
      )
      await db.execute('INSERT INTO customers (id) VALUES (1), (2)')
      await db.execute('INSERT INTO invoices (customer_id) VALUES (1), (1), (2)')
      await db.execute('DELETE FROM customers WHERE id = ?', [1])

      expect(onlyReportFor('DELETE FROM customers WHERE id = ?')).toMatchObject({ changes: 1, rowsWritten: 3 })
    })
  })

  it("reports every read of a live query with source 'live' and the live query's SQL", async () => {
    const db = await openOrders(TOPOLOGIES[0])
    const liveSql = 'SELECT id, customer FROM orders WHERE total = 5'
    const live = await db.live(liveSql)
    const firstRead = reportsFor(liveSql)
    expect(firstRead).toEqual([
      expect.objectContaining({ source: 'live', rowsReturned: ORDERS_PER_TOTAL, error: false }),
    ])
    expect(firstRead[0].rowsRead).toBeGreaterThanOrEqual(ORDERS_PER_TOTAL)

    const changed = new Promise<void>(resolve => {
      const stop = live.subscribe(() => {
        stop()
        resolve()
      })
    })
    await db.execute(INSERT_ORDER, ['live-customer', 5])
    await changed

    const probes = reportsFor(liveSql).slice(1)
    expect(probes.length).toBeGreaterThan(0)
    for (const probe of probes) expect(probe).toMatchObject({ source: 'live', error: false })
    expect(reported.filter(metrics => metrics.sql !== liveSql).every(metrics => metrics.source === undefined)).toBe(
      true,
    )
    await live.close()
  })
})

describe.skipIf(binding === null)('a write that outlives the writer worker deadline', () => {
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-unknown-row-counts-'))
    reported = []
  })

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true })
  })

  it('reports once, after the worker answers, with the rows that SQLite wrote', async () => {
    const registry = new Sirannon({
      driver: sleepingDriver({ sqliteBindingPath: binding ?? undefined }),
      metrics: { onQueryComplete: metrics => reported.push(metrics) },
    })
    const release = join(dir, 'release-held-write')
    try {
      const db = await registry.open('tenant', join(dir, 'tenant.db'), {
        writerWorker: { writeTimeoutMs: HELD_WRITE_DEADLINE_MS },
      })
      await db.execute('CREATE TABLE marker (n INTEGER NOT NULL)')
      const heldInsert = heldSql('INSERT INTO marker (n) VALUES (1)', release)

      await expect(db.execute(heldInsert)).rejects.toMatchObject({ code: 'WRITER_WORKER_TIMEOUT' })
      expect(reportsFor(heldInsert)).toEqual([])
      releaseHeldWrite(release)
      await vi.waitFor(() => expect(reportsFor(heldInsert)).toHaveLength(1), { timeout: 10_000, interval: 20 })
      const late = onlyReportFor(heldInsert)
      expect([late.error, late.rowsRead, late.rowsWritten]).toEqual([true, 0, 1])
      expect(late.durationMs).toBeGreaterThanOrEqual(HELD_WRITE_DEADLINE_MS)
      expect(await db.query('SELECT n FROM marker')).toEqual([{ n: 1 }])
    } finally {
      releaseHeldWrite(release)
      await registry.shutdown().catch(() => {})
    }
  }, 30_000)
})
