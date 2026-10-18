import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { HookDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { AfterQueryHookContext, QueryMetrics } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { readFile, type ScratchDirectory, scratchDirectory, settle, writeFile } from './harness.js'

const CALLS = 240
const SECOND_ROW_OFFSET = 100_000
const REFUSED_MARKER = '-- refused'
const SEED_SCRIPT = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL);'

const TOPOLOGIES = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

type Expectation = 'succeeds' | 'fails' | 'refused'

interface Call {
  statements: string[]
  expectation: Expectation
  insertedIds: number[]
  run(db: Database): Promise<unknown>
}

function callAt(index: number): Call {
  const tag = `-- call ${index}`
  const refused = index % 11 === 5
  const marker = refused ? ` ${REFUSED_MARKER}` : ''
  switch (index % 4) {
    case 0: {
      const fails = index % 7 === 0
      const sql = `INSERT INTO orders (id, customer, total) VALUES (?, ?, ?) ${tag}${marker}`
      return {
        statements: [sql],
        expectation: refused ? 'refused' : fails ? 'fails' : 'succeeds',
        insertedIds: refused || fails ? [] : [index],
        run: db => db.execute(sql, [index, `customer-${index}`, fails ? null : index]),
      }
    }
    case 1: {
      const sql = `SELECT count(*) AS n FROM orders WHERE total >= ? ${tag}${marker}`
      return {
        statements: [sql],
        expectation: refused ? 'refused' : 'succeeds',
        insertedIds: [],
        run: db => db.query(sql, [index]),
      }
    }
    case 2: {
      const fails = index % 9 === 2
      const first = `INSERT INTO orders (id, customer, total) VALUES (?, ?, ?) ${tag} first`
      const second = `INSERT INTO orders (id, customer, total) VALUES (?, ?, ?) ${tag} second${marker}`
      return {
        statements: [first, second],
        expectation: refused ? 'refused' : fails ? 'fails' : 'succeeds',
        insertedIds: refused || fails ? [] : [index, index + SECOND_ROW_OFFSET],
        run: db =>
          db.executeTransaction([
            { sql: first, params: [index, `customer-${index}`, index] },
            { sql: second, params: [index + SECOND_ROW_OFFSET, `customer-${index}`, fails ? null : index] },
          ]),
      }
    }
    default: {
      const fails = index % 5 === 3
      const sql = `SELECT total FROM ${fails ? 'missing_orders' : 'orders'} WHERE id = ? ${tag}${marker}`
      return {
        statements: [sql],
        expectation: refused ? 'refused' : fails ? 'fails' : 'succeeds',
        insertedIds: [],
        run: db => db.queryOne(sql, [index - 3]),
      }
    }
  }
}

let scratch: ScratchDirectory
let ordersPath: string
let sirannon: Sirannon

beforeEach(() => {
  scratch = scratchDirectory('sirannon-concurrent-hooks-')
  ordersPath = scratch.path('orders.db')
  writeFile(ordersPath, SEED_SCRIPT)
})

afterEach(async () => {
  await sirannon.shutdown().catch(() => {})
  scratch.remove()
})

describe.each(TOPOLOGIES)('many statements at once with $label', topology => {
  it('fire each hook once per statement and report each statement once, with the outcome that its caller received', async () => {
    const before: string[] = []
    const after: AfterQueryHookContext[] = []
    const reports: QueryMetrics[] = []
    sirannon = new Sirannon({
      driver: betterSqlite3(),
      hooks: {
        onBeforeQuery: ctx => {
          before.push(ctx.sql)
          if (ctx.sql.includes(REFUSED_MARKER)) throw new HookDeniedError('beforeQuery', 'the tenant is read-only')
        },
      },
      metrics: { onQueryComplete: metrics => reports.push(metrics) },
    })
    const db = await sirannon.open('orders', ordersPath, { writerWorker: topology.writerWorker })
    db.onAfterQuery(ctx => {
      after.push(ctx)
    })
    const calls = Array.from({ length: CALLS }, (_, index) => callAt(index))

    const outcomes = await Promise.all(calls.map(call => settle(call.run(db))))

    const outcomeOf = new Map<string, { expectation: Expectation; value: unknown }>()
    for (const [index, call] of calls.entries()) {
      const outcome = outcomes[index]
      expect(outcome.rejected, call.statements[0]).toBe(call.expectation !== 'succeeds')
      if (call.expectation === 'refused') expect(outcome.value, call.statements[0]).toBeInstanceOf(HookDeniedError)
      for (const sql of call.statements) outcomeOf.set(sql, { expectation: call.expectation, value: outcome.value })
    }
    const allStatements = calls.flatMap(call => call.statements)
    const executedStatements = calls.filter(call => call.expectation !== 'refused').flatMap(call => call.statements)
    const sorted = (values: string[]) => [...values].sort()

    expect(sorted(before)).toEqual(sorted(allStatements))
    expect(sorted(after.map(ctx => ctx.sql))).toEqual(sorted(executedStatements))
    expect(sorted(reports.map(metrics => metrics.sql))).toEqual(sorted(executedStatements))
    for (const ctx of after) {
      const outcome = outcomeOf.get(ctx.sql)
      if (outcome?.expectation === 'fails') expect(ctx.error, ctx.sql).toBe(outcome.value)
      else expect('error' in ctx, ctx.sql).toBe(false)
    }
    for (const metrics of reports) {
      expect(metrics.error, metrics.sql).toBe(outcomeOf.get(metrics.sql)?.expectation === 'fails')
    }
    expect(readFile<{ id: number }>(ordersPath, 'SELECT id FROM orders ORDER BY id').map(row => row.id)).toEqual(
      calls.flatMap(call => call.insertedIds).sort((left, right) => left - right),
    )
  })
})
