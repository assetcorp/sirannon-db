import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { RemoteError } from '../../../client/types.js'
import { type DatabaseOperations, operationRef } from '../../../core/operation-registry.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  readSqlite,
  scratchDirectory,
  sendRaw,
  trackedRegistry,
} from '../lib/served-registry.js'
import { ordersOperations, type Tenant } from './orders-operations.js'
import { openOrders, releaseAccessServers, SEEDED_ORDERS, serveTyped } from './served-access.js'

const ADD_INVOICE = operationRef<{ amount: number }>('addInvoice')
const OPEN_INVOICES = operationRef<Record<string, never>, { id: number; amount: number }>('openInvoices')

const sharedOperations: DatabaseOperations<Tenant> = {
  reads: {
    openInvoices: { statement: () => ({ sql: 'SELECT id, amount FROM invoices WHERE paid = 0 ORDER BY id' }) },
    ordersOfCustomer: { args: ['customer'], statement: () => ({ sql: "SELECT 'shared' AS source" }) },
  },
  writes: {
    addInvoice: {
      args: ['amount'],
      statements: args => ({ sql: 'INSERT INTO invoices (amount) VALUES (?)', params: [args.amount] }),
    },
  },
}

let dir: string

beforeEach(() => {
  dir = scratchDirectory('shared-operations')
})

afterEach(releaseAccessServers)

function openInvoices(path: string): Array<{ id: number; amount: number }> {
  return readSqlite<{ id: bigint; amount: bigint }>(
    path,
    'SELECT id, amount FROM invoices WHERE paid = 0 ORDER BY id',
  ).map(row => ({ id: Number(row.id), amount: Number(row.amount) }))
}

describe('operations shared by every database', () => {
  it('serves the shared set on a database opened after the server starts and after it reopens, and lets a database replace a shared name', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, { operations: ordersOperations, sharedOperations })
    const tenantPath = join(dir, 'tenant-new.db')
    const tenant = await sirannon.open('tenant-new', tenantPath)
    await tenant.execute(
      'CREATE TABLE invoices (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, paid INTEGER DEFAULT 0)',
    )

    const overHttp = await postJson(served.base, '/db/tenant-new/execute/addInvoice', { args: { amount: 300 } })
    const ws = await RawSocket.open(`${served.wsBase}/db/tenant-new`)
    const overWebSocket = await ws.request({ id: 'add', type: 'execute', name: 'addInvoice', args: { amount: 120 } })
    const client = new SirannonClient(served.base, { transport: 'http' })
    const invoices = client.database('tenant-new')
    await invoices.execute(ADD_INVOICE, { amount: 45 })

    expect(overHttp.status).toBe(200)
    expect(overWebSocket.type).toBe('result')
    expect(openInvoices(tenantPath)).toEqual([
      { id: 1, amount: 300 },
      { id: 2, amount: 120 },
      { id: 3, amount: 45 },
    ])
    expect(await invoices.query(OPEN_INVOICES, {})).toEqual(openInvoices(tenantPath))

    await sirannon.close('tenant-new')
    const closed = await postJson(served.base, '/db/tenant-new/query/openInvoices', {})
    expect([closed.status, errorCodeOf(closed.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    await sirannon.open('tenant-new', tenantPath)
    expect(await invoices.query(OPEN_INVOICES, {})).toEqual(openInvoices(tenantPath))

    const own = await postJson(served.base, '/db/orders/query/ordersOfCustomer', { args: { customer: 'ama' } })
    const shared = await postJson(served.base, '/db/tenant-new/query/ordersOfCustomer', { args: { customer: 'ama' } })
    const sharedNeedsItsArgument = await postJson(served.base, '/db/tenant-new/query/ordersOfCustomer', {})
    const ownOnly = await postJson(served.base, '/db/tenant-new/query/ordersOfTenant', {})
    const neither = await postJson(served.base, '/db/orders/query/closedInvoices', {})
    const ghost = await postJson(served.base, '/db/ghost/query/openInvoices', {})
    expect(own.body).toEqual({ rows: [{ id: 1, customer: 'ama', total: SEEDED_ORDERS[0]?.total }] })
    expect(shared.body).toEqual({ rows: [{ source: 'shared' }] })
    expect([sharedNeedsItsArgument.status, errorCodeOf(sharedNeedsItsArgument.body)]).toEqual([400, 'MISSING_ARGUMENT'])
    expect([ownOnly.status, errorCodeOf(ownOnly.body)]).toEqual([404, 'UNKNOWN_QUERY'])
    expect([neither.status, errorCodeOf(neither.body)]).toEqual([404, 'UNKNOWN_QUERY'])
    expect([ghost.status, errorCodeOf(ghost.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    client.close()
  })

  it('keeps refusing SQL from the network while named calls work, on the server and in the published client before it sends', async () => {
    const sirannon = trackedRegistry()
    const { path } = await openOrders(sirannon, dir)
    const served = await serveTyped<Tenant>(sirannon, { sharedOperations })

    const capabilities = await sendRaw(served.base, 'GET', '/capabilities')
    const statement = await postJson(served.base, '/db/orders/execute', { sql: 'DELETE FROM orders' })
    const ws = await RawSocket.open(`${served.wsBase}/db/orders`)
    const statementOverWebSocket = await ws.request({ id: 'sql', type: 'execute', sql: 'DELETE FROM orders' })
    const named = await postJson(served.base, '/db/orders/query/ordersOfCustomer', { args: { customer: 'x' } })

    expect(capabilities.body).toEqual({ capabilities: ['query.named'], registry: { digest: expect.any(String) } })
    expect([statement.status, errorCodeOf(statement.body)]).toEqual([403, 'SQL_NOT_ACCEPTED'])
    expect(statementOverWebSocket.error?.code).toBe('SQL_NOT_ACCEPTED')
    expect(named.body).toEqual({ rows: [{ source: 'shared' }] })

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(served.base, { transport, autoReconnect: false })
      let refused: unknown
      try {
        await client.database('orders').execute('DELETE FROM orders')
      } catch (err) {
        refused = err
      }
      expect(refused, transport).toBeInstanceOf(RemoteError)
      expect((refused as RemoteError).code, transport).toBe('SQL_NOT_ACCEPTED')
      client.close()
    }
    expect(readSqlite(path, 'SELECT count(*) AS n FROM orders')).toEqual([{ n: BigInt(SEEDED_ORDERS.length) }])
  })
})
