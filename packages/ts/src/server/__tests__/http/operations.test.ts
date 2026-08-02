import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import type { OperationRegistry } from '../../../core/operation-registry.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
  results: { changes: number; lastInsertRowId: number | string }[]
  capabilities: string[]
  registry?: { digest: string }
  error: { code: string; message: string }
}

interface Account {
  tenantId: string
}

const operations: OperationRegistry<Account> = {
  test: {
    reads: {
      ordersForCustomer: {
        args: ['customerName'],
        statement: args => ({
          sql: 'SELECT * FROM orders WHERE customer = :customerName ORDER BY id',
          params: { customerName: args.customerName },
        }),
      },
      'orders for customer': {
        args: ['customerName'],
        statement: args => ({
          sql: 'SELECT * FROM orders WHERE customer = :customerName ORDER BY id',
          params: { customerName: args.customerName },
        }),
      },
      ordersForTenant: {
        fromIdentity: { tenant: 'tenantId' },
        statement: args => ({
          sql: 'SELECT * FROM orders WHERE tenant = :tenant ORDER BY id',
          params: { tenant: args.tenant },
        }),
      },
    },
    writes: {
      placeOrder: {
        args: ['customerName', 'amount'],
        statements: args => ({
          sql: 'INSERT INTO orders (customer, amount, tenant) VALUES (:customerName, :amount, :tenant)',
          params: { customerName: args.customerName, amount: args.amount, tenant: 'acme' },
        }),
      },
      archiveCustomer: {
        args: ['customerName'],
        statements: args => [
          { sql: 'UPDATE orders SET archived = 1 WHERE customer = :customerName', params: args },
          { sql: 'DELETE FROM orders WHERE customer = :customerName AND amount = 0', params: args },
        ],
      },
    },
  },
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer<Account>
let baseUrl: string

const driver = betterSqlite3()

async function start(options?: { authenticate?: () => Account | undefined }): Promise<void> {
  server = createServer<Account>(sirannon, {
    acceptSql: true,
    port: 0,
    operations,
    authenticate: options?.authenticate,
  })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-operations-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute(
    'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount INTEGER, tenant TEXT, archived INTEGER DEFAULT 0)',
  )
  await db.execute("INSERT INTO orders (customer, amount, tenant) VALUES ('Alice', 120, 'acme')")
  await db.execute("INSERT INTO orders (customer, amount, tenant) VALUES ('Bob', 80, 'globex')")
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('POST /db/:id/query/:name', () => {
  it('runs a registered read with client arguments', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/ordersForCustomer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { customerName: 'Alice' } }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows).toHaveLength(1)
    expect(body.rows[0].amount).toBe(120)
  })

  it('leaves the SQL route reachable at its own path', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT COUNT(*) AS total FROM orders' }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows[0].total).toBe(2)
  })

  it('decodes a percent-encoded operation name', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/${encodeURIComponent('orders for customer')}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { customerName: 'Alice' } }),
    })
    expect(res.status).toBe(200)
    expect(((await res.json()) as ApiResponse).rows).toHaveLength(1)
  })

  it('rejects a malformed percent-encoded name', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/bad%ZZname`, { method: 'POST' })
    expect(res.status).toBe(400)
    expect(((await res.json()) as ApiResponse).error.code).toBe('INVALID_REQUEST')
  })

  it('rejects an unregistered name with UNKNOWN_QUERY', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/noSuchOperation`, { method: 'POST' })
    expect(res.status).toBe(404)
    expect(((await res.json()) as ApiResponse).error.code).toBe('UNKNOWN_QUERY')
  })

  it('rejects a missing declared argument', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/ordersForCustomer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: {} }),
    })
    expect(res.status).toBe(400)
    expect(((await res.json()) as ApiResponse).error.code).toBe('MISSING_ARGUMENT')
  })

  it('rejects an undeclared argument', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/ordersForCustomer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { customerName: 'Alice', limit: 5 } }),
    })
    expect(res.status).toBe(400)
    expect(((await res.json()) as ApiResponse).error.code).toBe('ARGUMENT_NOT_ALLOWED')
  })

  it('refuses a caller-supplied identity argument instead of overriding it', async () => {
    await start({ authenticate: () => ({ tenantId: 'acme' }) })
    const res = await fetch(`${baseUrl}/db/test/query/ordersForTenant`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { tenant: 'globex' } }),
    })
    expect(res.status).toBe(400)
    expect(((await res.json()) as ApiResponse).error.code).toBe('ARGUMENT_NOT_ALLOWED')
  })

  it('fills an identity argument from the authenticated identity', async () => {
    await start({ authenticate: () => ({ tenantId: 'globex' }) })
    const res = await fetch(`${baseUrl}/db/test/query/ordersForTenant`, { method: 'POST' })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows).toHaveLength(1)
    expect(body.rows[0].customer).toBe('Bob')
  })

  it('rejects an identity argument when the request carries no identity', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/ordersForTenant`, { method: 'POST' })
    expect(res.status).toBe(401)
    expect(((await res.json()) as ApiResponse).error.code).toBe('IDENTITY_REQUIRED')
  })

  it('maps a denial thrown by authenticate to its own status', async () => {
    await start({
      authenticate: () => {
        throw new RequestDeniedError(403, 'FORBIDDEN_TENANT', 'This token cannot reach that database')
      },
    })
    const res = await fetch(`${baseUrl}/db/test/query/ordersForCustomer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { customerName: 'Alice' } }),
    })
    expect(res.status).toBe(403)
    expect(((await res.json()) as ApiResponse).error.code).toBe('FORBIDDEN_TENANT')
  })
})

describe('POST /db/:id/execute/:name', () => {
  it('runs a registered single-statement write', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/execute/placeOrder`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { customerName: 'Carol', amount: 45 } }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.results).toHaveLength(1)
    expect(body.results[0].changes).toBe(1)

    const db = await sirannon.resolve('test')
    if (db === null || db === undefined) throw new Error('database test is not open')
    expect(await db.query('SELECT customer FROM orders WHERE amount = 45')).toHaveLength(1)
  })

  it('runs a multi-statement write atomically', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/execute/archiveCustomer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ args: { customerName: 'Alice' } }),
    })
    expect(res.status).toBe(200)
    expect(((await res.json()) as ApiResponse).results).toHaveLength(2)
  })

  it('rejects an unregistered write name', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/execute/noSuchWrite`, { method: 'POST' })
    expect(res.status).toBe(404)
    expect(((await res.json()) as ApiResponse).error.code).toBe('UNKNOWN_QUERY')
  })

  it('does not resolve a read name on the write route', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/execute/ordersForCustomer`, { method: 'POST' })
    expect(res.status).toBe(404)
  })
})

describe('GET /capabilities', () => {
  it('advertises the registry digest and the named-query capability', async () => {
    await start()
    const res = await fetch(`${baseUrl}/capabilities`)
    const body = (await res.json()) as ApiResponse
    expect(body.capabilities).toContain('query.named')
    expect(body.capabilities).toContain('query.sql')
    expect(body.registry?.digest).toMatch(/^[0-9a-f]{64}$/)
  })

  it('omits the registry when no operations are registered', async () => {
    server = createServer<Account>(sirannon, { acceptSql: true, port: 0 })
    await server.listen()
    const res = await fetch(`http://127.0.0.1:${server.listeningPort}/capabilities`)
    const body = (await res.json()) as ApiResponse
    expect(body.registry).toBeUndefined()
    expect(body.capabilities).not.toContain('query.named')
  })
})
