import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  postJson,
  RawSocket,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
} from '../lib/served-registry.js'
import {
  type Answer,
  type Caller,
  clientCaller,
  httpCaller,
  METERED_OPERATIONS,
  ORDERS_OF_CUSTOMER,
  PLACE_AND_REPRICE,
  PLACE_ORDER,
  webSocketCaller,
} from './metered-callers.js'
import {
  type Billed,
  billedAs,
  type Counts,
  INSERT_ORDER,
  indexesOn,
  leaksNoCounts,
  Meter,
  meteredRegistry,
  NOTHING,
  ORDER_BY_ID,
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

const SEEDED_ORDERS = [240, 130, 75, 310]
const TENANT_IDS = ['acme', 'globex', 'initech', 'umbrella']
const REPRICE_BY_CUSTOMER = 'UPDATE orders SET total = ? WHERE customer = ?'
const DELETE_BY_ID = 'DELETE FROM orders WHERE id = ?'
const LOADED_ROWS = 60
const FAILING_LOAD_ROW = 37

let dir: string
let meter: Meter
let sirannon: Sirannon

interface Exercise {
  expected: Billed[]
  answers: Answer[]
}

function orderRows(prefix: string, count: number, nullTotalAt = -1): unknown[][] {
  return Array.from({ length: count }, (_, index) => [`${prefix}-${index}`, index === nullTotalAt ? null : index])
}

async function scanAfterOneInsertUpdatingOneRow(tenant: Tenant): Promise<Counts> {
  expect(await scansWholeTable(tenant.path, REPRICE_BY_CUSTOMER, [8, 'x'], 'orders')).toBe(true)
  return {
    rowsRead: (await rowsIn(tenant.path, 'orders')) + 1,
    rowsWritten: 1 + (await indexesOn(tenant.path, 'orders')),
  }
}

async function totalUpdateByRowid(tenant: Tenant): Promise<Counts> {
  return { rowsRead: ROWID_LOOKUP.rowsRead, rowsWritten: 1 + (await indexesOn(tenant.path, 'orders')) }
}

async function exercise(call: Caller, tenant: Tenant): Promise<Exercise> {
  const expected: Billed[] = []
  const answers: Answer[] = []
  const step = async (send: () => Promise<Answer>, succeeds: boolean, ...bills: Billed[]): Promise<void> => {
    const answer = await send()
    expect(answer.ok, `${call.label} on ${tenant.id}: ${answer.text}`).toBe(succeeds)
    answers.push(answer)
    expected.push(...bills)
  }
  const sought = `${tenant.id}-customer-7`
  expect(await scansWholeTable(tenant.path, SCAN_BY_CUSTOMER, [sought], 'orders')).toBe(true)
  const storedRows = () => rowsIn(tenant.path, 'orders')

  await step(
    () => call.query(SCAN_BY_CUSTOMER, [sought]),
    true,
    billedAs(SCAN_BY_CUSTOMER, false, await wholeTableScan(tenant.path, 'orders')),
  )
  await step(() => call.query(ORDER_BY_ID, [3]), true, billedAs(ORDER_BY_ID, false, ROWID_LOOKUP))
  await step(
    () => call.execute(INSERT_ORDER, ['walk-in', 4]),
    true,
    billedAs(INSERT_ORDER, false, await rowInserts(tenant.path, 'orders')),
  )
  await step(
    () =>
      call.transaction([
        { sql: INSERT_ORDER, params: ['first', 1] },
        { sql: SET_TOTAL_BY_ID, params: [8, 2] },
      ]),
    true,
    billedAs(INSERT_ORDER, false, await rowInserts(tenant.path, 'orders')),
    billedAs(SET_TOTAL_BY_ID, false, await totalUpdateByRowid(tenant)),
  )

  const beforeRollback = await storedRows()
  await step(
    () =>
      call.transaction([
        { sql: INSERT_ORDER, params: ['rolled-back', 1] },
        { sql: REPRICE_BY_CUSTOMER, params: [8, 'rolled-back'] },
        { sql: INSERT_ORDER, params: ['refused', null] },
        { sql: DELETE_BY_ID, params: [1] },
      ]),
    false,
    billedAs(INSERT_ORDER, true, await rowInserts(tenant.path, 'orders')),
    billedAs(REPRICE_BY_CUSTOMER, true, await scanAfterOneInsertUpdatingOneRow(tenant)),
    billedAs(INSERT_ORDER, true, NOTHING),
    billedAs(DELETE_BY_ID, true, NOTHING),
  )
  expect(await storedRows()).toBe(beforeRollback)

  await step(
    () => call.batch(INSERT_ORDER, orderRows('batch', 3)),
    true,
    billedAs(INSERT_ORDER, false, await rowInserts(tenant.path, 'orders', 3)),
  )
  const beforeFailedBatch = await storedRows()
  await step(
    () => call.batch(INSERT_ORDER, orderRows('failed-batch', 3, 1)),
    false,
    billedAs(INSERT_ORDER, true, await rowInserts(tenant.path, 'orders', 1)),
  )
  expect(await storedRows()).toBe(beforeFailedBatch)

  await step(
    () => call.load(INSERT_ORDER, orderRows('loaded', LOADED_ROWS)),
    true,
    billedAs(INSERT_ORDER, false, await rowInserts(tenant.path, 'orders', LOADED_ROWS)),
  )
  const beforeFailedLoad = await storedRows()
  await step(
    () => call.load(INSERT_ORDER, orderRows('failed-load', LOADED_ROWS, FAILING_LOAD_ROW)),
    false,
    billedAs(INSERT_ORDER, true, await rowInserts(tenant.path, 'orders', FAILING_LOAD_ROW)),
  )
  expect(await storedRows()).toBe(beforeFailedLoad)

  await step(
    () => call.namedQuery(ORDERS_OF_CUSTOMER, { customer: sought }),
    true,
    billedAs(SCAN_BY_CUSTOMER, false, await wholeTableScan(tenant.path, 'orders')),
  )
  await step(
    () => call.namedExecute(PLACE_ORDER, { customer: 'named', total: 5 }),
    true,
    billedAs(INSERT_ORDER, false, await rowInserts(tenant.path, 'orders')),
  )
  await step(
    () => call.namedExecute(PLACE_AND_REPRICE, { customer: 'named-pair', id: 4 }),
    true,
    billedAs(INSERT_ORDER, false, await rowInserts(tenant.path, 'orders')),
    billedAs(SET_TOTAL_BY_ID, false, await totalUpdateByRowid(tenant)),
  )
  await step(
    () => call.namedExecute(PLACE_ORDER, { customer: 'named-refused', total: null }),
    false,
    billedAs(INSERT_ORDER, true, NOTHING),
  )
  return { expected, answers }
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering every route that executes SQL', () => {
  beforeEach(() => {
    dir = scratchDirectory('metered-routes')
    meter = new Meter()
    sirannon = meteredRegistry(meter)
  })

  afterEach(releaseServedRegistries)

  describe.each(TOPOLOGIES)('with $label', topology => {
    it('bills each statement of every route once, to the database that the request names, over every transport at once', async () => {
      const tenants: Tenant[] = []
      for (const [index, id] of TENANT_IDS.entries()) {
        tenants.push(await openOrders(sirannon, dir, id, SEEDED_ORDERS[index], topology.options))
      }
      const served = await serveRegistry(sirannon, { acceptSql: true, sharedOperations: METERED_OPERATIONS })
      const callers = [
        httpCaller(served.base, 'acme'),
        await webSocketCaller(served.wsBase, 'globex'),
        clientCaller(served.base, 'initech', 'http'),
        clientCaller(served.base, 'umbrella', 'websocket'),
      ]
      meter.clear()

      const exercises = await Promise.all(callers.map((call, index) => exercise(call, tenants[index])))
      for (const call of callers) call.close()

      for (const [index, tenant] of tenants.entries()) {
        expect(meter.billed(tenant.id), `${callers[index].label} on ${tenant.id}`).toEqual(exercises[index].expected)
        for (const answer of exercises[index].answers) expect(leaksNoCounts(answer.text), answer.text).toBe(true)
      }
      expect(meter.reports).toHaveLength(exercises.reduce((sum, run) => sum + run.expected.length, 0))
      expect(meter.reports.every(metrics => metrics.source === undefined)).toBe(true)
    })
  })

  it('reports nothing for a request that the server refuses before SQLite sees it, and bills no rows to a statement that SQLite cannot prepare', async () => {
    const acme = await openOrders(sirannon, dir, 'acme', SEEDED_ORDERS[0])
    const open = await serveRegistry(sirannon, { acceptSql: true, sharedOperations: METERED_OPERATIONS })
    const closed = await serveRegistry(sirannon, { sharedOperations: METERED_OPERATIONS })
    meter.clear()

    const refused: Array<[label: string, answer: Promise<{ status: number }>]> = [
      ['invalid JSON', sendRaw(open.base, 'POST', '/db/acme/query', '{"sql": ')],
      ['an empty body', sendRaw(open.base, 'POST', '/db/acme/execute', '')],
      ['a body that is a list', sendRaw(open.base, 'POST', '/db/acme/query', '[]')],
      ['no sql', postJson(open.base, '/db/acme/query', { params: [] })],
      ['sql that is a number', postJson(open.base, '/db/acme/execute', { sql: 7 })],
      ['params that are a string', postJson(open.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: 'x' })],
      [
        'a broken tagged integer',
        postJson(open.base, '/db/acme/query', { sql: ORDER_BY_ID, params: [{ __sirannon_int: 'twelve' }] }),
      ],
      ['an empty transaction', postJson(open.base, '/db/acme/transaction', { statements: [] })],
      ['a transaction without sql', postJson(open.base, '/db/acme/transaction', { statements: [{ params: [] }] })],
      ['an empty batch', postJson(open.base, '/db/acme/batch', { sql: INSERT_ORDER, paramsBatch: [] })],
      ['a batch of strings', postJson(open.base, '/db/acme/batch', { sql: INSERT_ORDER, paramsBatch: ['x'] })],
      [
        'a load at an unknown durability',
        postJson(open.base, '/db/acme/load', { sql: INSERT_ORDER, paramsBatch: [['a', 1]], durability: 'never' }),
      ],
      [
        'a bad write concern',
        postJson(open.base, '/db/acme/execute', { sql: INSERT_ORDER, params: ['a', 1], writeConcern: { level: 'x' } }),
      ],
      ['an unknown database', postJson(open.base, '/db/hooli/query', { sql: 'SELECT 1' })],
      ['an undecodable database', sendRaw(open.base, 'POST', '/db/%E0%A4%A/query', '{"sql":"SELECT 1"}')],
      ['an unknown operation', postJson(open.base, '/db/acme/query/everything', { args: {} })],
      ['a missing argument', postJson(open.base, `/db/acme/query/${ORDERS_OF_CUSTOMER}`, { args: {} })],
      [
        'an extra argument',
        postJson(open.base, `/db/acme/execute/${PLACE_ORDER}`, { args: { customer: 'a', total: 1, tenant: 'b' } }),
      ],
      [
        'sql to a server that takes none',
        postJson(closed.base, '/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: ['x'] }),
      ],
    ]
    for (const [label, answer] of refused) {
      expect((await answer).status, label).toBeGreaterThanOrEqual(400)
    }

    const ws = await RawSocket.open(`${open.wsBase}/db/acme`)
    const wsRefusals = [
      { id: 'r1', type: 'query' },
      { id: 'r2', type: 'execute', sql: INSERT_ORDER, params: 'x' },
      { id: 'r3', type: 'transaction', statements: [] },
      { id: 'r4', type: 'batch', sql: INSERT_ORDER, paramsBatch: [] },
      { id: 'r5', type: 'load', sql: INSERT_ORDER, paramsBatch: [['a', 1]], checkpoint: 'yes' },
      { id: 'r6', type: 'query', name: 'everything', args: {} },
      { id: 'r7', type: 'mutate', sql: INSERT_ORDER },
    ]
    for (const message of wsRefusals) {
      expect((await ws.request(message)).type, JSON.stringify(message)).toBe('error')
    }
    const nextReply = ws.next()
    ws.sendText('not json at all')
    expect((await nextReply).type).toBe('error')
    expect(meter.reports).toEqual([])

    const unprepared = [
      'SELECT * FROM refunds',
      'SELEC id FROM orders',
      'SELECT * FROM _sirannon_changes',
      'INSERT INTO orders (customer, total, colour) VALUES (?, ?, ?)',
    ]
    for (const sql of unprepared) {
      const answer = await postJson(open.base, '/db/acme/query', { sql, params: ['a', 1, 'red'] })
      expect(answer.status, sql).toBeGreaterThanOrEqual(400)
      expect(answer.status, sql).toBeLessThan(500)
      expect((await ws.request({ id: sql, type: 'execute', sql, params: ['a', 1, 'red'] })).type, sql).toBe('error')
    }
    expect(meter.billed('acme')).toEqual(
      unprepared.flatMap(sql => [billedAs(sql, true, NOTHING), billedAs(sql, true, NOTHING)]),
    )
    expect(await rowsIn(acme.path, 'orders')).toBe(SEEDED_ORDERS[0])
  })
})
