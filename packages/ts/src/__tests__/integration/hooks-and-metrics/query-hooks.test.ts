import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { HookDeniedError } from '../../../core/errors.js'
import type { HookDispose } from '../../../core/hooks/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { AfterQueryHookContext, QueryHookContext, QueryMetrics, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  elapse,
  readFile,
  type ScratchDirectory,
  scratchDirectory,
  settle,
  tablesIn,
  watchUnhandledRejections,
  writeFile,
} from './harness.js'

const SEEDED_ORDERS = [
  { id: 1, customer: 'ada', total: 120 },
  { id: 2, customer: 'grace', total: 75 },
]
const SEED_SCRIPT = `
  CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL);
  INSERT INTO orders (id, customer, total) VALUES (1, 'ada', 120), (2, 'grace', 75);
`
const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const ALL_ORDERS = 'SELECT id, customer, total FROM orders ORDER BY id'

const TOPOLOGIES = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

const driver = betterSqlite3()

let scratch: ScratchDirectory
let ordersPath: string
const registries: Sirannon[] = []

function registry(options: Omit<SirannonOptions, 'driver'> = {}): Sirannon {
  const sirannon = new Sirannon({ driver, ...options })
  registries.push(sirannon)
  return sirannon
}

beforeEach(() => {
  scratch = scratchDirectory('sirannon-query-hooks-')
  ordersPath = scratch.path('orders.db')
  writeFile(ordersPath, SEED_SCRIPT)
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
  scratch.remove()
})

describe('a before-query hook that throws', () => {
  it('refuses the statement on every path with whatever the hook threw, before SQLite executes any of it', async () => {
    const thrownValues: Array<[string, unknown]> = [
      ['a HookDeniedError', new HookDeniedError('beforeQuery', 'orders are frozen')],
      ['an Error', new Error('the policy service refused it')],
      ['a string', 'refused by policy'],
      ['a number', 403],
      ['null', null],
      ['undefined', undefined],
      ['a plain object', { code: 'POLICY' }],
    ]
    const paths: Array<[string, (db: Database) => Promise<unknown>]> = [
      ['query', db => db.query('SELECT * FROM missing_orders -- refused')],
      ['queryOne', db => db.queryOne('SELECT * FROM missing_orders -- refused')],
      ['execute', db => db.execute('DROP TABLE orders -- refused')],
      [
        'executeBatch',
        db =>
          db.executeBatch(`${INSERT_ORDER} -- refused`, [
            ['eve', 1],
            ['mallory', 2],
          ]),
      ],
      ['bulkLoad', db => db.bulkLoad(`${INSERT_ORDER} -- refused`, [['eve', 1]])],
      [
        'a transaction that Sirannon groups with other writes',
        db =>
          db.executeTransaction([{ sql: INSERT_ORDER, params: ['eve', 1] }, { sql: 'DELETE FROM orders -- refused' }]),
      ],
      [
        'a transaction that executes alone',
        db =>
          db.executeTransaction([
            { sql: 'CREATE TABLE audit (id INTEGER PRIMARY KEY)' },
            { sql: 'DELETE FROM orders -- refused' },
          ]),
      ],
    ]
    let refusal: unknown
    const afterCalls: AfterQueryHookContext[] = []
    const reports: QueryMetrics[] = []
    const sirannon = registry({
      hooks: {
        onBeforeQuery: ctx => {
          if (ctx.sql.includes('refused')) throw refusal
        },
        onAfterQuery: ctx => {
          afterCalls.push(ctx)
        },
      },
      metrics: { onQueryComplete: metrics => reports.push(metrics) },
    })
    const db = await sirannon.open('orders', ordersPath)

    for (const [thrownLabel, thrown] of thrownValues) {
      refusal = thrown
      for (const [pathLabel, run] of paths) {
        const outcome = await settle(run(db))
        expect(outcome.rejected, `${pathLabel} with ${thrownLabel}`).toBe(true)
        expect(outcome.value, `${pathLabel} with ${thrownLabel}`).toBe(thrown)
      }
    }

    expect(afterCalls).toEqual([])
    expect(reports).toEqual([])
    expect(tablesIn(ordersPath)).toEqual(['orders'])
    expect(readFile(ordersPath, ALL_ORDERS)).toEqual(SEEDED_ORDERS)
  })
})

describe('query hooks on many databases', () => {
  it('calls registry hooks before database hooks in the order of registration, with the statement, its parameters, and its concerns', async () => {
    const calls: Array<{ hook: string; ctx: QueryHookContext | AfterQueryHookContext }> = []
    const record = (hook: string) => (ctx: QueryHookContext | AfterQueryHookContext) => {
      calls.push({ hook, ctx })
    }
    const ledgerPath = scratch.path('ledger.db')
    writeFile(ledgerPath, 'CREATE TABLE entries (id INTEGER PRIMARY KEY); INSERT INTO entries VALUES (1), (2), (3);')
    const sirannon = registry({
      hooks: {
        onBeforeQuery: [record('options-first'), record('options-second')],
        onAfterQuery: record('options-after'),
      },
    })
    sirannon.onBeforeQuery(record('registry'))
    const orders = await sirannon.open('orders', ordersPath)
    const ledger = await sirannon.open('ledger', ledgerPath)
    const twice = record('orders-database')
    orders.onBeforeQuery(twice)
    orders.onBeforeQuery(twice)
    orders.onAfterQuery(record('orders-database-after'))
    ledger.onBeforeQuery(record('ledger-database'))
    sirannon.onAfterQuery(record('registry-after'))
    await orders.watch('orders')
    calls.length = 0

    const insertSql = INSERT_ORDER
    const readSql = 'SELECT customer FROM orders WHERE total > :minimum ORDER BY id'
    const countSql = 'SELECT count(*) AS n FROM entries'
    const writeConcern = { level: 'local' as const }
    const readConcern = { level: 'local' as const }
    await orders.execute(insertSql, ['linus', 30], { writeConcern })
    const read = await orders.query(readSql, { minimum: 50 }, { readConcern })
    const counted = await ledger.queryOne(countSql)
    await elapse(150)

    const beforeInsert = { databaseId: 'orders', sql: insertSql, params: ['linus', 30], writeConcern }
    const beforeRead = { databaseId: 'orders', sql: readSql, params: { minimum: 50 }, readConcern }
    const beforeCount = { databaseId: 'ledger', sql: countSql }
    const after = (ctx: object) => {
      const { writeConcern: _write, readConcern: _read, ...rest } = ctx as Record<string, unknown>
      return { ...rest, durationMs: expect.any(Number) }
    }
    expect(calls.map(({ hook, ctx }) => [hook, ctx])).toEqual([
      ['options-first', beforeInsert],
      ['options-second', beforeInsert],
      ['registry', beforeInsert],
      ['orders-database', beforeInsert],
      ['orders-database', beforeInsert],
      ['options-after', after(beforeInsert)],
      ['registry-after', after(beforeInsert)],
      ['orders-database-after', after(beforeInsert)],
      ['options-first', beforeRead],
      ['options-second', beforeRead],
      ['registry', beforeRead],
      ['orders-database', beforeRead],
      ['orders-database', beforeRead],
      ['options-after', after(beforeRead)],
      ['registry-after', after(beforeRead)],
      ['orders-database-after', after(beforeRead)],
      ['options-first', beforeCount],
      ['options-second', beforeCount],
      ['registry', beforeCount],
      ['ledger-database', beforeCount],
      ['options-after', after(beforeCount)],
      ['registry-after', after(beforeCount)],
    ])
    for (const { hook, ctx } of calls.filter(call => call.hook.endsWith('after'))) {
      expect('error' in ctx, hook).toBe(false)
      expect((ctx as AfterQueryHookContext).durationMs, hook).toBeGreaterThanOrEqual(0)
    }
    expect(read).toEqual(readFile(ordersPath, 'SELECT customer FROM orders WHERE total > 50 ORDER BY id'))
    expect(counted).toEqual({ n: 3 })
    expect(readFile(ordersPath, 'SELECT customer, total FROM orders WHERE id = 3')).toEqual([
      { customer: 'linus', total: 30 },
    ])
  })
})

describe('hook disposers', () => {
  it('remove only the registration that returned them, and a second call changes nothing', async () => {
    const seen: string[] = []
    const audit = (ctx: QueryHookContext) => {
      seen.push(ctx.sql)
    }
    const sirannon = registry()
    const disposeFirstRegistryCopy = sirannon.onBeforeQuery(audit)
    sirannon.onBeforeQuery(audit)
    const db = await sirannon.open('orders', ordersPath)
    const disposeFirstDatabaseCopy = db.onAfterQuery(audit)
    db.onAfterQuery(audit)

    await db.query('SELECT 1 AS one')
    disposeFirstRegistryCopy()
    disposeFirstDatabaseCopy()
    await db.query('SELECT 2 AS two')
    disposeFirstRegistryCopy()
    disposeFirstRegistryCopy()
    disposeFirstDatabaseCopy()
    await db.query('SELECT 3 AS three')

    expect(seen).toEqual([
      'SELECT 1 AS one',
      'SELECT 1 AS one',
      'SELECT 1 AS one',
      'SELECT 1 AS one',
      'SELECT 2 AS two',
      'SELECT 2 AS two',
      'SELECT 3 AS three',
      'SELECT 3 AS three',
    ])
  })

  it('take effect from the next statement when a running hook registers or disposes hooks', async () => {
    const order: string[] = []
    const sirannon = registry()
    const db = await sirannon.open('orders', ordersPath)
    let disposeDoomed: HookDispose = () => {}
    let disposeOneShot: HookDispose = () => {}
    let disposeAfterOnce: HookDispose = () => {}
    let rearranged = false
    sirannon.onBeforeQuery(() => {
      order.push('rearranger')
      if (rearranged) return
      rearranged = true
      disposeDoomed()
      sirannon.onBeforeQuery(() => {
        order.push('late')
      })
    })
    disposeDoomed = sirannon.onBeforeQuery(() => {
      order.push('doomed')
    })
    disposeOneShot = db.onBeforeQuery(() => {
      order.push('one-shot')
      disposeOneShot()
    })
    disposeAfterOnce = db.onAfterQuery(() => {
      order.push('after-once')
      disposeAfterOnce()
    })
    db.onAfterQuery(() => {
      order.push('after-always')
    })

    await db.query('SELECT 1')
    order.push('next statement')
    await db.execute(INSERT_ORDER, ['linus', 30])

    expect(order).toEqual([
      'rearranger',
      'doomed',
      'one-shot',
      'after-once',
      'after-always',
      'next statement',
      'rearranger',
      'late',
      'after-always',
    ])
    expect(readFile(ordersPath, 'SELECT customer FROM orders WHERE id = 3')).toEqual([{ customer: 'linus' }])
  })
})

describe('a before-query hook that returns a promise', () => {
  it('fails the statement before SQLite executes it, and leaves no unhandled rejection', async () => {
    const unhandled = watchUnhandledRejections()
    try {
      const afterCalls: AfterQueryHookContext[] = []
      const sirannon = registry({ hooks: { onAfterQuery: ctx => afterCalls.push(ctx) } })
      const db = await sirannon.open('orders', ordersPath)
      const promisingHooks: Array<[string, () => Promise<void>]> = [
        ['resolves', async () => {}],
        [
          'rejects at once',
          async () => {
            throw new Error('the policy store is offline')
          },
        ],
        [
          'rejects later',
          () =>
            elapse(20).then(() => {
              throw new Error('the policy store timed out')
            }),
        ],
      ]

      for (const [label, hook] of promisingHooks) {
        const dispose = db.onBeforeQuery(hook)
        const outcome = await settle(db.execute(INSERT_ORDER, [label, 1]))
        dispose()
        expect(outcome.rejected, label).toBe(true)
        expect(outcome.value, label).toBeInstanceOf(Error)
        expect((outcome.value as Error).message, label).toMatch(/returned a Promise/)
      }
      await elapse(80)

      expect(afterCalls).toEqual([])
      expect(unhandled.reasons).toEqual([])
      expect(readFile(ordersPath, ALL_ORDERS)).toEqual(SEEDED_ORDERS)
    } finally {
      unhandled.stop()
    }
  })
})

describe.each(TOPOLOGIES)('after-query hooks with $label', topology => {
  it('receive the error that the caller receives on every path, and no error when the statement succeeds', async () => {
    const afterCalls: AfterQueryHookContext[] = []
    const sirannon = registry({ hooks: { onAfterQuery: ctx => afterCalls.push(ctx) } })
    const db = await sirannon.open('orders', ordersPath, { writerWorker: topology.writerWorker })
    const calls: Array<{ label: string; statements: string[]; run: () => Promise<unknown>; fails: boolean }> = [
      { label: 'a read', statements: [ALL_ORDERS], run: () => db.query(ALL_ORDERS), fails: false },
      {
        label: 'a read of a missing table',
        statements: ['SELECT id FROM missing_orders'],
        run: () => db.query('SELECT id FROM missing_orders'),
        fails: true,
      },
      {
        label: 'a single-row read',
        statements: ['SELECT customer FROM orders WHERE id = ?'],
        run: () => db.queryOne('SELECT customer FROM orders WHERE id = ?', [1]),
        fails: false,
      },
      {
        label: 'a single-row read with a syntax error',
        statements: ['SELEC customer FROM orders'],
        run: () => db.queryOne('SELEC customer FROM orders'),
        fails: true,
      },
      {
        label: 'a write',
        statements: [INSERT_ORDER],
        run: () => db.execute(INSERT_ORDER, ['linus', 30]),
        fails: false,
      },
      {
        label: 'a write that breaks NOT NULL',
        statements: [`${INSERT_ORDER} -- null total`],
        run: () => db.execute(`${INSERT_ORDER} -- null total`, ['nobody', null]),
        fails: true,
      },
      {
        label: 'a batch',
        statements: [`${INSERT_ORDER} -- batch`],
        run: () =>
          db.executeBatch(`${INSERT_ORDER} -- batch`, [
            ['ken', 5],
            ['barbara', 6],
          ]),
        fails: false,
      },
      {
        label: 'a batch whose second parameter set breaks NOT NULL',
        statements: [`${INSERT_ORDER} -- failing batch`],
        run: () =>
          db.executeBatch(`${INSERT_ORDER} -- failing batch`, [
            ['dennis', 7],
            ['nobody', null],
          ]),
        fails: true,
      },
      {
        label: 'a bulk load',
        statements: [`${INSERT_ORDER} -- load`],
        run: () => db.bulkLoad(`${INSERT_ORDER} -- load`, [['margaret', 8]]),
        fails: false,
      },
      {
        label: 'a bulk load whose second row breaks NOT NULL',
        statements: [`${INSERT_ORDER} -- failing load`],
        run: () =>
          db.bulkLoad(`${INSERT_ORDER} -- failing load`, [
            ['alan', 9],
            ['nobody', null],
          ]),
        fails: true,
      },
      {
        label: 'a grouped transaction',
        statements: [`${INSERT_ORDER} -- grouped`, "UPDATE orders SET total = total + 1 WHERE customer = 'ada'"],
        run: () =>
          db.executeTransaction([
            { sql: `${INSERT_ORDER} -- grouped`, params: ['edsger', 10] },
            { sql: "UPDATE orders SET total = total + 1 WHERE customer = 'ada'" },
          ]),
        fails: false,
      },
      {
        label: 'a grouped transaction whose second statement breaks NOT NULL',
        statements: [`${INSERT_ORDER} -- failing grouped`, `${INSERT_ORDER} -- failing grouped null`],
        run: () =>
          db.executeTransaction([
            { sql: `${INSERT_ORDER} -- failing grouped`, params: ['tony', 11] },
            { sql: `${INSERT_ORDER} -- failing grouped null`, params: ['nobody', null] },
          ]),
        fails: true,
      },
      {
        label: 'a transaction that executes alone',
        statements: ['CREATE TABLE audit (note TEXT NOT NULL)', 'INSERT INTO audit (note) VALUES (?)'],
        run: () =>
          db.executeTransaction([
            { sql: 'CREATE TABLE audit (note TEXT NOT NULL)' },
            { sql: 'INSERT INTO audit (note) VALUES (?)', params: ['opened'] },
          ]),
        fails: false,
      },
      {
        label: 'a transaction that executes alone and breaks NOT NULL',
        statements: ['CREATE TABLE audit_two (note TEXT NOT NULL)', 'INSERT INTO audit_two (note) VALUES (?)'],
        run: () =>
          db.executeTransaction([
            { sql: 'CREATE TABLE audit_two (note TEXT NOT NULL)' },
            { sql: 'INSERT INTO audit_two (note) VALUES (?)', params: [null] },
          ]),
        fails: true,
      },
    ]

    for (const call of calls) {
      const firstContext = afterCalls.length
      const outcome = await settle(call.run())
      const contexts = afterCalls.slice(firstContext)
      expect(outcome.rejected, call.label).toBe(call.fails)
      expect(
        contexts.map(ctx => ctx.sql),
        call.label,
      ).toEqual(call.statements)
      for (const ctx of contexts) {
        if (call.fails) expect(ctx.error, call.label).toBe(outcome.value)
        else expect('error' in ctx, call.label).toBe(false)
      }
    }

    expect(readFile(ordersPath, 'SELECT customer, total FROM orders ORDER BY id')).toEqual([
      { customer: 'ada', total: 121 },
      { customer: 'grace', total: 75 },
      { customer: 'linus', total: 30 },
      { customer: 'ken', total: 5 },
      { customer: 'barbara', total: 6 },
      { customer: 'margaret', total: 8 },
      { customer: 'edsger', total: 10 },
    ])
    expect(tablesIn(ordersPath)).toEqual(['audit', 'orders'])
    expect(readFile(ordersPath, 'SELECT note FROM audit')).toEqual([{ note: 'opened' }])
  })
})

describe('an after-query hook that fails', () => {
  it('leaves the statement, its result or error, and every later after-query hook untouched, with no unhandled rejection', async () => {
    const unhandled = watchUnhandledRejections()
    try {
      const seen: string[] = []
      const sirannon = registry({
        hooks: {
          onAfterQuery: [
            () => {
              throw new Error('the audit sink is offline')
            },
            () => {
              throw 'the audit sink is offline'
            },
            () => {
              throw undefined
            },
          ],
        },
      })
      sirannon.onAfterQuery(async () => {
        throw new Error('the async audit sink is offline')
      })
      sirannon.onAfterQuery(() =>
        elapse(20).then(() => {
          throw new Error('the audit sink timed out')
        }),
      )
      const db = await sirannon.open('orders', ordersPath)
      db.onAfterQuery(() => {
        throw new RangeError('the tenant audit sink is offline')
      })
      db.onAfterQuery(ctx => {
        seen.push(ctx.sql)
      })

      const rows = await db.query(ALL_ORDERS)
      const failed = await settle(db.query('SELECT id FROM missing_orders'))
      const written = await db.execute(INSERT_ORDER, ['linus', 30])
      const transaction = await settle(
        db.executeTransaction([
          { sql: `${INSERT_ORDER} -- first`, params: ['ken', 5] },
          { sql: `${INSERT_ORDER} -- second`, params: ['nobody', null] },
        ]),
      )
      await elapse(80)

      expect(rows).toEqual(SEEDED_ORDERS)
      expect(failed.rejected).toBe(true)
      expect((failed.value as Error).message).toMatch(/no such table: missing_orders/)
      expect(written).toEqual({ changes: 1, lastInsertRowId: 3 })
      expect(transaction.rejected).toBe(true)
      expect((transaction.value as Error).message).toMatch(/NOT NULL/)
      expect(seen).toEqual([
        ALL_ORDERS,
        'SELECT id FROM missing_orders',
        INSERT_ORDER,
        `${INSERT_ORDER} -- first`,
        `${INSERT_ORDER} -- second`,
      ])
      expect(readFile(ordersPath, ALL_ORDERS)).toEqual([...SEEDED_ORDERS, { id: 3, customer: 'linus', total: 30 }])
      expect(unhandled.reasons).toEqual([])
    } finally {
      unhandled.stop()
    }
  })
})
