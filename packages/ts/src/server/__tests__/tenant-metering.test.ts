import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  builtRowCountingBindingPath,
  ROW_COUNTING_SQLITE_REQUIRED,
} from '../../__tests__/helpers/row-counting-sqlite.js'
import { Sirannon } from '../../core/sirannon.js'
import type { QueryMetrics } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../server.js'

const ACME_ORDERS = 500
const GLOBEX_ORDERS = 200
const INSERT_ORDER = 'INSERT INTO orders (customer, total) VALUES (?, ?)'
const SCAN_BY_CUSTOMER = 'SELECT * FROM orders WHERE customer = ?'
const MALFORMED_JSON_OF_CUSTOMER = 'SELECT json(customer) FROM orders WHERE customer = ?'
const LAST_ACME_CUSTOMER = `customer-${ACME_ORDERS - 1}`

const binding = builtRowCountingBindingPath()

interface TenantBill {
  rowsRead: number
  rowsWritten: number
}

let dir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let reported: QueryMetrics[]

function billFor(databaseId: string): TenantBill {
  const bill: TenantBill = { rowsRead: 0, rowsWritten: 0 }
  for (const metrics of reported) {
    if (metrics.databaseId !== databaseId) continue
    bill.rowsRead += metrics.rowsRead ?? Number.NaN
    bill.rowsWritten += metrics.rowsWritten ?? Number.NaN
  }
  return bill
}

async function post(path: string, body: unknown): Promise<{ status: number; text: string }> {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: res.status, text: await res.text() }
}

function queryOverWebSocket(databaseId: string, sql: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${baseUrl.replace('http', 'ws')}/db/${databaseId}`)
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for the WebSocket result')), 3000)
    ws.addEventListener('open', () => ws.send(JSON.stringify({ id: 'q1', type: 'query', sql })))
    ws.addEventListener('message', event => {
      clearTimeout(timeout)
      ws.close()
      resolve(String(event.data))
    })
    ws.addEventListener('error', () => reject(new Error('WebSocket connect error')))
  })
}

async function seed(databaseId: string, orders: number): Promise<void> {
  const db = await sirannon.open(databaseId, join(dir, `${databaseId}.db`))
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)')
  await db.execute('CREATE INDEX orders_by_total ON orders (total)')
  await db.watch('orders')
  await db.bulkLoad(
    INSERT_ORDER,
    Array.from({ length: orders }, (_, index) => [`customer-${index}`, index % 10]),
  )
}

describe('the row-counting build', () => {
  it.skipIf(!ROW_COUNTING_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe.skipIf(binding === null)('per-tenant metering through the server', () => {
  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-tenant-metering-'))
    reported = []
    sirannon = new Sirannon({
      driver: betterSqlite3({ rowCounts: true, rowCountsBindingPath: binding ?? undefined }),
      metrics: { onQueryComplete: metrics => reported.push(metrics) },
    })
    await seed('acme', ACME_ORDERS)
    await seed('globex', GLOBEX_ORDERS)
    reported = []
    server = createServer(sirannon, { acceptSql: true, port: 0 })
    await server.listen()
    baseUrl = `http://127.0.0.1:${server.listeningPort}`
  })

  afterEach(async () => {
    await server.close()
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  it('bills each tenant for the rows that its own requests read and write, failed requests included', async () => {
    const scan = await post('/db/acme/query', { sql: SCAN_BY_CUSTOMER, params: [LAST_ACME_CUSTOMER] })
    const failed = await post('/db/acme/query', { sql: MALFORMED_JSON_OF_CUSTOMER, params: [LAST_ACME_CUSTOMER] })
    const insert = await post('/db/acme/execute', { sql: INSERT_ORDER, params: ['new-customer', 1] })
    const transaction = await post('/db/acme/transaction', {
      statements: [
        { sql: INSERT_ORDER, params: ['first', 2] },
        { sql: INSERT_ORDER, params: ['second', 3] },
      ],
    })
    const counted = await queryOverWebSocket('globex', 'SELECT count(*) AS n FROM orders')

    expect([scan.status, insert.status, transaction.status]).toEqual([200, 200, 200])
    expect(failed.status).toBeGreaterThanOrEqual(400)
    expect(JSON.parse(counted)).toMatchObject({ type: 'result', data: { rows: [{ n: GLOBEX_ORDERS }] } })

    expect(billFor('acme')).toEqual({ rowsRead: ACME_ORDERS * 2, rowsWritten: 6 })
    expect(billFor('globex')).toEqual({ rowsRead: GLOBEX_ORDERS, rowsWritten: 0 })
    for (const response of [scan, insert, transaction, failed]) {
      expect(response.text).not.toMatch(/rowsRead|rowsWritten/)
    }
    expect(counted).not.toMatch(/rowsRead|rowsWritten/)
  })
})
