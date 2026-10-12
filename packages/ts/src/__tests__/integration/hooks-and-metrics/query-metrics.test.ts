import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { AfterQueryHookContext, CDCMetrics, QueryMetrics, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { pausedSql, pausingDriver } from '../../helpers/fault-drivers/pausing-driver.js'
import {
  elapse,
  readFile,
  type ScratchDirectory,
  scratchDirectory,
  settle,
  waitUntil,
  watchUnhandledRejections,
  writeFile,
} from './harness.js'

const SEEDED_ORDERS = 40
const DISTINCT_TOTALS = 4
const SEED_SCRIPT = `
  CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL);
  WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM seq WHERE i < ${SEEDED_ORDERS - 1})
  INSERT INTO orders (customer, total) SELECT 'c-' || i, i % ${DISTINCT_TOTALS} FROM seq;
  CREATE TABLE payments (id INTEGER PRIMARY KEY, status TEXT NOT NULL);
  CREATE TABLE ledger (id INTEGER PRIMARY KEY, note TEXT NOT NULL);
`
const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const ORDERS_WITH_TOTAL = 'SELECT id FROM orders WHERE total = ?'
const ORDER_BY_ID = 'SELECT customer FROM orders WHERE id = ?'
const RAISE_TOTAL = 'UPDATE orders SET total = total + 10 WHERE total = ?'
const DELETE_WITH_TOTAL = 'DELETE FROM orders WHERE total = ?'
const RENAME_WITH_TOTAL = 'UPDATE orders SET customer = ? WHERE total = ?'
const PAUSE_MS = 60

const TOPOLOGIES = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

interface Step {
  label: string
  fails: boolean
  expected: () => QueryMetrics[]
  run: () => Promise<unknown>
}

let scratch: ScratchDirectory
let ordersPath: string
const registries: Sirannon[] = []

function registry(options: Omit<SirannonOptions, 'driver'>, driver = betterSqlite3()): Sirannon {
  const sirannon = new Sirannon({ driver, ...options })
  registries.push(sirannon)
  return sirannon
}

function countInFile(where: string): number {
  return readFile<{ n: number }>(ordersPath, `SELECT count(*) AS n FROM orders WHERE ${where}`)[0].n
}

function succeeded(sql: string, outcome: { rowsReturned: number } | { changes: number }): QueryMetrics {
  return { databaseId: 'orders', sql, ...outcome, durationMs: expect.any(Number), error: false }
}

function failed(sql: string): QueryMetrics {
  return { databaseId: 'orders', sql, durationMs: expect.any(Number), error: true }
}

beforeEach(() => {
  scratch = scratchDirectory('sirannon-query-metrics-')
  ordersPath = scratch.path('orders.db')
  writeFile(ordersPath, SEED_SCRIPT)
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
  scratch.remove()
})

describe.each(TOPOLOGIES)('onQueryComplete with $label', topology => {
  it('reports each statement once, with its duration, its outcome, and the rows that it returned or changed', async () => {
    const reports: QueryMetrics[] = []
    const sirannon = registry({ metrics: { onQueryComplete: metrics => reports.push(metrics) } })
    const db = await sirannon.open('orders', ordersPath, { writerWorker: topology.writerWorker })
    await db.watch('orders')
    const steps: Step[] = [
      {
        label: 'a read',
        fails: false,
        expected: () => [succeeded(ORDERS_WITH_TOTAL, { rowsReturned: countInFile('total = 2') })],
        run: () => db.query(ORDERS_WITH_TOTAL, [2]),
      },
      {
        label: 'a single-row read that finds its row',
        fails: false,
        expected: () => [succeeded(ORDER_BY_ID, { rowsReturned: 1 })],
        run: () => db.queryOne(ORDER_BY_ID, [7]),
      },
      {
        label: 'a single-row read that finds nothing',
        fails: false,
        expected: () => [succeeded(ORDER_BY_ID, { rowsReturned: 0 })],
        run: () => db.queryOne(ORDER_BY_ID, [SEEDED_ORDERS + 1_000]),
      },
      {
        label: 'a read of a missing table',
        fails: true,
        expected: () => [failed('SELECT id FROM missing_orders')],
        run: () => db.query('SELECT id FROM missing_orders'),
      },
      {
        label: 'a write',
        fails: false,
        expected: () => [succeeded(RAISE_TOTAL, { changes: countInFile('total = 3') })],
        run: () => db.execute(RAISE_TOTAL, [3]),
      },
      {
        label: 'a write that breaks NOT NULL',
        fails: true,
        expected: () => [failed(INSERT_ORDER)],
        run: () => db.execute(INSERT_ORDER, ['nobody', null]),
      },
      {
        label: 'a batch',
        fails: false,
        expected: () => [succeeded(DELETE_WITH_TOTAL, { changes: countInFile('total IN (0, 1)') })],
        run: () => db.executeBatch(DELETE_WITH_TOTAL, [[0], [1]]),
      },
      {
        label: 'a bulk load',
        fails: false,
        expected: () => [succeeded(INSERT_ORDER, { changes: 25 })],
        run: () =>
          db.bulkLoad(
            INSERT_ORDER,
            Array.from({ length: 25 }, (_, index) => [`loaded-${index}`, 5]),
          ),
      },
      {
        label: 'a transaction',
        fails: false,
        expected: () => [
          succeeded(INSERT_ORDER, { changes: 1 }),
          succeeded(RENAME_WITH_TOTAL, { changes: countInFile('total = 13') }),
        ],
        run: () =>
          db.executeTransaction([
            { sql: INSERT_ORDER, params: ['tx', 4] },
            { sql: RENAME_WITH_TOTAL, params: ['renamed', 13] },
          ]),
      },
      {
        label: 'a transaction whose second statement breaks NOT NULL',
        fails: true,
        expected: () => [failed(`${INSERT_ORDER} -- rolled back`), failed(`${INSERT_ORDER} -- null total`)],
        run: () =>
          db.executeTransaction([
            { sql: `${INSERT_ORDER} -- rolled back`, params: ['rolled-back', 4] },
            { sql: `${INSERT_ORDER} -- null total`, params: ['nobody', null] },
          ]),
      },
      {
        label: 'a transaction callback that catches one failed statement',
        fails: false,
        expected: () => [
          succeeded(ORDERS_WITH_TOTAL, { rowsReturned: countInFile('total = 2') }),
          succeeded(DELETE_WITH_TOTAL, { changes: countInFile('total = 2') }),
          succeeded(INSERT_ORDER, { changes: 2 }),
          failed(`${INSERT_ORDER} -- caught`),
        ],
        run: () =>
          db.transaction(async tx => {
            await tx.query(ORDERS_WITH_TOTAL, [2])
            await tx.execute(DELETE_WITH_TOTAL, [2])
            await tx.executeBatch(INSERT_ORDER, [
              ['callback-1', 1],
              ['callback-2', 2],
            ])
            await tx.execute(`${INSERT_ORDER} -- caught`, ['nobody', null]).catch(() => undefined)
          }),
      },
    ]

    let expectedReports = 0
    for (const { label, fails, expected, run } of steps) {
      const wanted = expected()
      expectedReports += wanted.length
      const first = reports.length
      const start = performance.now()
      const outcome = await settle(run())
      const elapsedMs = performance.now() - start
      const emitted = reports.slice(first)
      expect(outcome.rejected, label).toBe(fails)
      expect(emitted, label).toStrictEqual(wanted)
      for (const metrics of emitted) {
        expect(metrics.durationMs, label).toBeGreaterThanOrEqual(0)
        expect(metrics.durationMs, label).toBeLessThanOrEqual(elapsedMs)
      }
    }
    await elapse(120)

    expect(reports).toHaveLength(expectedReports)
    expect(readFile(ordersPath, 'SELECT total, count(*) AS n FROM orders GROUP BY total ORDER BY total')).toEqual([
      { total: 1, n: 1 },
      { total: 2, n: 1 },
      { total: 4, n: 1 },
      { total: 5, n: 25 },
      { total: 13, n: 10 },
    ])
    expect(countInFile("total = 13 AND customer = 'renamed'")).toBe(10)
  })
})

describe('the duration of a slow statement', () => {
  it('covers the time that SQLite spent executing it, in onQueryComplete and in the after-query hook alike', async () => {
    const reports: QueryMetrics[] = []
    const afterCalls: AfterQueryHookContext[] = []
    const sirannon = registry(
      {
        hooks: { onAfterQuery: ctx => afterCalls.push(ctx) },
        metrics: { onQueryComplete: metrics => reports.push(metrics) },
      },
      pausingDriver(),
    )
    const db = await sirannon.open('orders', ordersPath)
    const slowStatements: Array<[string, string, () => Promise<unknown>]> = [
      ['a read', pausedSql(ORDERS_WITH_TOTAL, PAUSE_MS), () => db.query(pausedSql(ORDERS_WITH_TOTAL, PAUSE_MS), [1])],
      ['a write', pausedSql(RAISE_TOTAL, PAUSE_MS), () => db.execute(pausedSql(RAISE_TOTAL, PAUSE_MS), [1])],
      [
        'a write that breaks NOT NULL',
        pausedSql(INSERT_ORDER, PAUSE_MS),
        () => db.execute(pausedSql(INSERT_ORDER, PAUSE_MS), ['nobody', null]),
      ],
    ]

    for (const [label, sql, run] of slowStatements) {
      const start = performance.now()
      await settle(run())
      const elapsedMs = performance.now() - start
      const reported = reports.filter(metrics => metrics.sql === sql)
      const hooked = afterCalls.filter(ctx => ctx.sql === sql)
      expect(reported, label).toHaveLength(1)
      expect(hooked, label).toHaveLength(1)
      for (const durationMs of [reported[0].durationMs, hooked[0].durationMs]) {
        expect(durationMs, label).toBeGreaterThanOrEqual(PAUSE_MS - 1)
        expect(durationMs, label).toBeLessThanOrEqual(elapsedMs)
      }
    }
    expect(countInFile('total = 11')).toBe(SEEDED_ORDERS / DISTINCT_TOTALS)
  })
})

describe('a metrics callback that fails', () => {
  it('leaves every statement and its result or error untouched, with no unhandled rejection', async () => {
    const unhandled = watchUnhandledRejections()
    try {
      const callbacks: Array<[string, () => unknown]> = [
        [
          'throws an Error',
          () => {
            throw new Error('the histogram is offline')
          },
        ],
        [
          'throws a string',
          () => {
            throw 'the histogram is offline'
          },
        ],
        [
          'returns a rejected promise',
          async () => {
            throw new Error('the async histogram is offline')
          },
        ],
        [
          'returns a promise that rejects later',
          () =>
            elapse(20).then(() => {
              throw new Error('the histogram timed out')
            }),
        ],
      ]
      for (const [index, [label, callback]] of callbacks.entries()) {
        ordersPath = scratch.path(`orders-${index}.db`)
        writeFile(ordersPath, SEED_SCRIPT)
        const cdcSeen: string[] = []
        const sirannon = registry({ metrics: { onQueryComplete: callback, onCDCEvent: callback } })
        const db = await sirannon.open('orders', ordersPath, { cdcPollInterval: 10 })
        await db.watch('orders')
        db.on('orders').subscribe(event => cdcSeen.push(String(event.row.customer)))

        const rows = await db.query(ORDERS_WITH_TOTAL, [1])
        const missing = await settle(db.query('SELECT id FROM missing_orders'))
        const written = await db.execute(INSERT_ORDER, [label, 7])
        const transaction = await db.executeTransaction([
          { sql: INSERT_ORDER, params: [`${label} in a transaction`, 7] },
        ])
        const fromCallback = await db.transaction(tx => tx.query(ORDERS_WITH_TOTAL, [7]))
        await waitUntil(() => cdcSeen.length >= 2)
        await elapse(80)
        await sirannon.shutdown()

        expect(rows, label).toEqual(readFile(ordersPath, ORDERS_WITH_TOTAL, [1]))
        expect(missing.rejected, label).toBe(true)
        expect((missing.value as Error).message, label).toMatch(/no such table: missing_orders/)
        expect(written.changes, label).toBe(1)
        expect(
          transaction.map(result => result.changes),
          label,
        ).toEqual([1])
        expect(fromCallback, label).toEqual(readFile(ordersPath, ORDERS_WITH_TOTAL, [7]))
        expect(fromCallback, label).toHaveLength(2)
        expect(cdcSeen, label).toEqual([label, `${label} in a transaction`])
      }

      expect(unhandled.reasons).toEqual([])
    } finally {
      unhandled.stop()
    }
  })
})

describe('onCDCEvent', () => {
  it('reports each change once, with the number of subscribers whose filter the old or the new row matches', async () => {
    const events: CDCMetrics[] = []
    let deliveries = 0
    const sirannon = registry({
      metrics: {
        onCDCEvent: metrics => {
          events.push(metrics)
          throw new Error('the change counter is offline')
        },
      },
    })
    const db = await sirannon.open('orders', ordersPath, { cdcPollInterval: 10 })
    await db.watch('payments')
    const filters: Array<string | undefined> = [undefined, 'paid', 'void']
    const subscriptions = filters.map(status => {
      const builder = status === undefined ? db.on('payments') : db.on('payments').filter({ status })
      return builder.subscribe(() => {
        deliveries++
        if (status === undefined) throw new Error('the unfiltered subscriber crashed')
      })
    })
    const reached = (before: string | undefined, after: string | undefined): number =>
      filters.filter(status => status === undefined || status === before || status === after).length
    const expected = [
      { operation: 'insert', subscriberCount: reached(undefined, 'paid') },
      { operation: 'insert', subscriberCount: reached(undefined, 'open') },
      { operation: 'insert', subscriberCount: reached(undefined, 'open') },
      { operation: 'update', subscriberCount: reached('open', 'void') },
      { operation: 'update', subscriberCount: reached('paid', 'void') },
      { operation: 'delete', subscriberCount: reached('void', undefined) },
      { operation: 'update', subscriberCount: reached('void', 'paid') },
      { operation: 'update', subscriberCount: reached('open', 'paid') },
    ]

    await db.execute("INSERT INTO payments (id, status) VALUES (1, 'paid')")
    await db.execute("INSERT INTO payments (id, status) VALUES (2, 'open'), (3, 'open')")
    await db.execute("UPDATE payments SET status = 'void' WHERE id = 2")
    await db.execute("UPDATE payments SET status = 'void' WHERE id = 1")
    await db.execute('DELETE FROM payments WHERE id = 2')
    await db.execute("INSERT INTO ledger (note) VALUES ('an unwatched table')")
    await db.execute("UPDATE payments SET status = 'paid' WHERE id IN (1, 3)")
    await waitUntil(() => events.length >= expected.length)
    subscriptions[0].unsubscribe()
    await db.execute("INSERT INTO payments (id, status) VALUES (4, 'open')")
    await waitUntil(() => events.length >= expected.length + 1)
    await elapse(80)

    expect(events).toEqual(
      [...expected, { operation: 'insert', subscriberCount: 0 }].map(event => ({
        databaseId: 'orders',
        table: 'payments',
        ...event,
      })),
    )
    expect(deliveries).toBe(expected.reduce((total, event) => total + event.subscriberCount, 0))
    expect(readFile(ordersPath, 'SELECT id, status FROM payments ORDER BY id')).toEqual([
      { id: 1, status: 'paid' },
      { id: 3, status: 'paid' },
      { id: 4, status: 'open' },
    ])
  })
})
