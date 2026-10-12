import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { QueryMetrics } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../../server/server.js'
import { builtSirannonSqliteBindingPath, SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  createMtlsCerts,
  createPrimary,
  createReplica,
  type ManagedNode,
  type MtlsCerts,
  stopNode,
  waitForReady,
  waitForReplica,
} from '../lib/index.js'

const PRIMARY_ID = 'primary-metering-aaaa'
const REPLICA_ID = 'replica-metering-bbbb'
const DATABASE_ID = 'orders'
const SEEDED_ORDERS = 300
const INDEXES_ON_ORDERS = 1
const ROW_AND_INDEX_ENTRIES_PER_INSERT = 1 + INDEXES_ON_ORDERS
const REPLICATION_DEADLINE_MS = 10_000
const READY_DEADLINE_MS = 20_000

const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const SET_TOTAL_BY_ID = 'UPDATE orders SET total = ? WHERE id = ?'
const BLANK_TOTAL_BY_ID = 'UPDATE orders SET total = NULL WHERE id = ?'
const SCAN_BY_CUSTOMER = 'SELECT * FROM orders WHERE customer = ?'
const INSERT_PAYROLL = 'INSERT INTO payroll (employee, salary) VALUES (?, ?)'
const PAYROLL_REFUSAL = 'payroll is closed to the API'

const binding = builtSirannonSqliteBindingPath()

interface MeteredNode {
  node: ManagedNode
  sirannon: Sirannon
  server: SirannonServer
  baseUrl: string
  reports: QueryMetrics[]
}

interface MeteredPair {
  primary: MeteredNode
  replica: MeteredNode
}

interface HttpAnswer {
  status: number
  text: string
}

let certs: MtlsCerts
const running: MeteredNode[] = []

function countingDriver(): SQLiteDriver {
  return betterSqlite3({ rowCounts: true, sqliteBindingPath: binding ?? undefined })
}

async function seedOrders(conn: SQLiteConnection): Promise<void> {
  await conn.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)')
  await conn.exec('CREATE INDEX orders_by_total ON orders (total)')
  await conn.exec('CREATE TABLE payroll (id INTEGER PRIMARY KEY, employee TEXT NOT NULL, salary INTEGER NOT NULL)')
  const insert = await conn.prepare(INSERT_ORDER)
  for (let index = 0; index < SEEDED_ORDERS; index++) {
    await insert.run(`customer-${index}`, index % 10)
  }
}

function meteredRegistry(reports: QueryMetrics[]): Sirannon {
  const sirannon = new Sirannon({
    driver: countingDriver(),
    metrics: { onQueryComplete: metrics => reports.push(metrics) },
  })
  sirannon.onBeforeQuery(context => {
    if (/\bpayroll\b/i.test(context.sql)) throw new Error(PAYROLL_REFUSAL)
  })
  return sirannon
}

async function serve(node: ManagedNode, sirannon: Sirannon, reports: QueryMetrics[]): Promise<MeteredNode> {
  const server = createServer(sirannon, {
    host: '127.0.0.1',
    port: 0,
    acceptSql: true,
    resolveExecutionTarget: id => (id === DATABASE_ID ? node.engine : null),
  })
  await server.listen()
  const served = { node, sirannon, server, baseUrl: `http://127.0.0.1:${server.listeningPort}`, reports }
  running.push(served)
  return served
}

async function startPair(engineDriver: () => SQLiteDriver): Promise<MeteredPair> {
  const primaryReports: QueryMetrics[] = []
  const primaryRegistry = meteredRegistry(primaryReports)
  const primaryNode = await createPrimary({
    nodeId: PRIMARY_ID,
    certs,
    registry: { sirannon: primaryRegistry, driver: engineDriver(), databaseId: DATABASE_ID },
    initialize: async (conn, tracker) => {
      await seedOrders(conn)
      await tracker.watch(conn, 'orders')
      await tracker.watch(conn, 'payroll')
    },
  })
  const primary = await serve(primaryNode, primaryRegistry, primaryReports)

  const replicaReports: QueryMetrics[] = []
  const replicaRegistry = meteredRegistry(replicaReports)
  const replicaNode = await createReplica({
    nodeId: REPLICA_ID,
    certs,
    primaryHost: 'localhost',
    primaryPort: primaryNode.port,
    registry: { sirannon: replicaRegistry, driver: engineDriver(), databaseId: DATABASE_ID },
    configOverrides: { writeForwarding: true },
  })
  const replica = await serve(replicaNode, replicaRegistry, replicaReports)
  await waitForReady(replicaNode.engine, READY_DEADLINE_MS)
  primaryReports.length = 0
  replicaReports.length = 0
  return { primary, replica }
}

async function post(target: MeteredNode, route: string, body: unknown): Promise<HttpAnswer> {
  const response = await fetch(`${target.baseUrl}/db/${DATABASE_ID}/${route}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: response.status, text: await response.text() }
}

async function countRows(conn: SQLiteConnection, table: 'orders' | 'payroll'): Promise<number> {
  const row = (await (await conn.prepare(`SELECT count(*) AS n FROM ${table}`)).get()) as { n: number | bigint }
  return Number(row.n)
}

function reportsOf(target: MeteredNode, sql: string): QueryMetrics[] {
  return target.reports.filter(metrics => metrics.sql === sql)
}

async function caughtUp({ primary, replica }: MeteredPair): Promise<void> {
  await waitForReplica(
    replica.node.engine,
    primary.node.nodeId,
    primary.node.engine.getCurrentSeq(),
    REPLICATION_DEADLINE_MS,
  )
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe.skipIf(binding === null)('metering and query hooks on a replicated server', () => {
  beforeAll(async () => {
    certs = await createMtlsCerts([PRIMARY_ID, REPLICA_ID])
  })

  afterAll(() => {
    certs?.cleanup()
  })

  afterEach(async () => {
    for (const target of running.splice(0).reverse()) {
      await target.server.close()
      await stopNode(target.node)
      await target.sirannon.shutdown()
    }
  })

  it('bills every write on the primary that executes it, forwarded writes and failed writes included', async () => {
    const pair = await startPair(countingDriver)
    const { primary, replica } = pair

    const direct = await post(primary, 'execute', { sql: INSERT_ORDER, params: ['walk-in', 4] })
    const transaction = await post(primary, 'transaction', {
      statements: [
        { sql: INSERT_ORDER, params: ['first', 2] },
        { sql: INSERT_ORDER, params: ['second', 3] },
      ],
    })
    const forwarded = await post(replica, 'execute', { sql: SET_TOTAL_BY_ID, params: [7, 5] })
    const refused = await post(replica, 'execute', { sql: BLANK_TOTAL_BY_ID, params: [6] })

    expect([direct.status, transaction.status, forwarded.status]).toEqual([200, 200, 200])
    expect(refused.status).toBeGreaterThanOrEqual(400)
    expect(refused.status).toBeLessThan(500)

    const insertBill = { error: false, changes: 1, rowsRead: 0, rowsWritten: ROW_AND_INDEX_ENTRIES_PER_INSERT }
    expect(reportsOf(primary, INSERT_ORDER)).toEqual([
      expect.objectContaining(insertBill),
      expect.objectContaining(insertBill),
      expect.objectContaining(insertBill),
    ])
    expect(reportsOf(primary, SET_TOTAL_BY_ID)).toEqual([
      expect.objectContaining({ error: false, changes: 1, rowsRead: 1, rowsWritten: 1 + INDEXES_ON_ORDERS }),
    ])
    expect(reportsOf(primary, BLANK_TOTAL_BY_ID)).toEqual([
      expect.objectContaining({ error: true, rowsRead: 1, rowsWritten: 0 }),
    ])
    expect(replica.reports).toEqual([])

    await caughtUp(pair)
    const rowsOnReplica = await countRows(replica.node.conn, 'orders')
    expect(rowsOnReplica).toBe(SEEDED_ORDERS + 3)
    const read = await post(replica, 'query', { sql: SCAN_BY_CUSTOMER, params: ['first'] })

    expect(read.status).toBe(200)
    expect(reportsOf(replica, SCAN_BY_CUSTOMER)).toEqual([
      expect.objectContaining({ error: false, rowsReturned: 1, rowsRead: rowsOnReplica, rowsWritten: 0 }),
    ])
    expect(reportsOf(primary, SCAN_BY_CUSTOMER)).toEqual([])
    for (const answer of [direct, transaction, forwarded, refused, read]) {
      expect(answer.text).not.toMatch(/rowsRead|rowsWritten/)
    }
  })

  it('refuses a write that a query hook rejects, whether it reaches the primary directly or through the replica', async () => {
    const pair = await startPair(countingDriver)
    const { primary, replica } = pair

    const direct = await post(primary, 'execute', { sql: INSERT_PAYROLL, params: ['ada', 100] })
    const forwarded = await post(replica, 'execute', { sql: INSERT_PAYROLL, params: ['grace', 200] })
    const directTransaction = await post(primary, 'transaction', {
      statements: [
        { sql: INSERT_ORDER, params: ['bundled', 1] },
        { sql: INSERT_PAYROLL, params: ['linus', 300] },
      ],
    })
    const forwardedTransaction = await post(replica, 'transaction', {
      statements: [
        { sql: INSERT_ORDER, params: ['bundled-remotely', 1] },
        { sql: INSERT_PAYROLL, params: ['barbara', 400] },
      ],
    })
    const afterwards = await post(replica, 'execute', { sql: INSERT_ORDER, params: ['still-served', 1] })

    for (const refused of [direct, forwarded, directTransaction, forwardedTransaction]) {
      expect(refused.status).toBeGreaterThanOrEqual(400)
    }
    expect(afterwards.status).toBe(200)
    expect(await countRows(primary.node.conn, 'payroll')).toBe(0)
    expect(await countRows(primary.node.conn, 'orders')).toBe(SEEDED_ORDERS + 1)
    await caughtUp(pair)
    expect(await countRows(replica.node.conn, 'payroll')).toBe(0)
    expect(await countRows(replica.node.conn, 'orders')).toBe(SEEDED_ORDERS + 1)
  })

  it('forwards a transaction from the replica and bills each of its statements on the primary', async () => {
    const pair = await startPair(countingDriver)
    const { primary, replica } = pair

    const forwarded = await post(replica, 'transaction', {
      statements: [
        { sql: INSERT_ORDER, params: ['remote-first', 8] },
        { sql: SET_TOTAL_BY_ID, params: [9, 12] },
      ],
    })

    expect(forwarded.status).toBe(200)
    expect(reportsOf(primary, INSERT_ORDER)).toEqual([
      expect.objectContaining({ error: false, changes: 1, rowsRead: 0, rowsWritten: ROW_AND_INDEX_ENTRIES_PER_INSERT }),
    ])
    expect(reportsOf(primary, SET_TOTAL_BY_ID)).toEqual([
      expect.objectContaining({ error: false, changes: 1, rowsRead: 1, rowsWritten: 1 + INDEXES_ON_ORDERS }),
    ])
    expect(replica.reports).toEqual([])
    await caughtUp(pair)
    expect(await countRows(replica.node.conn, 'orders')).toBe(SEEDED_ORDERS + 1)
  })

  it('reports no row counts for a write on an engine connection that counts no rows, so that a bill never shows zero for uncounted work', async () => {
    const { primary } = await startPair(() => betterSqlite3())

    const direct = await post(primary, 'execute', { sql: INSERT_ORDER, params: ['uncounted', 4] })

    expect(direct.status).toBe(200)
    const [report] = reportsOf(primary, INSERT_ORDER)
    expect(report).toMatchObject({ error: false, changes: 1 })
    expect(report).not.toHaveProperty('rowsRead')
    expect(report).not.toHaveProperty('rowsWritten')
  })
})
