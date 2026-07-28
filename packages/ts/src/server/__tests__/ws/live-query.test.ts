import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { OperationArguments, OperationRegistry } from '../../../core/operation-registry.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { operationRegistryDigest } from '../../operation-lookup.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages } from '../helpers.js'

interface Identity {
  tenantId: string
}

const driver = betterSqlite3()

const operations: OperationRegistry<Identity> = {
  shop: {
    reads: {
      openOrders: {
        args: ['status'],
        fromIdentity: { tenant: 'tenantId' },
        statement: (args: OperationArguments) => ({
          sql: 'SELECT id, reference, total FROM orders WHERE tenant_id = ? AND status = ? ORDER BY id',
          params: [args.tenant, args.status],
        }),
      },
      everyOrder: {
        statement: () => ({ sql: 'SELECT id, reference FROM orders ORDER BY id' }),
      },
    },
    writes: {
      cancelOrder: {
        args: ['id'],
        fromIdentity: { tenant: 'tenantId' },
        statements: (args: OperationArguments) => ({
          sql: 'UPDATE orders SET status = ? WHERE id = ? AND tenant_id = ?',
          params: ['cancelled', args.id, args.tenant],
        }),
      },
    },
  },
}

let tempDir: string
let sirannon: Sirannon
let db: Database

async function waitFor(predicate: () => boolean, timeout = 4000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`Timed out after ${timeout}ms waiting for the expected messages`)
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

function messagesOfType(conn: MockWSConnection, type: string): Record<string, unknown>[] {
  return parseMessages(conn).filter(msg => msg.type === type)
}

function subscribe(conn: MockWSConnection, handler: ReturnType<typeof createWSHandler>, body: object): void {
  handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', ...body }))
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-live-'))
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('shop', join(tempDir, 'shop.db'))
  await db.execute(
    'CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id TEXT NOT NULL, reference TEXT NOT NULL, status TEXT NOT NULL, total INTEGER NOT NULL)',
  )
  await db.execute("INSERT INTO orders (tenant_id, reference, status, total) VALUES ('acme', 'A-1', 'open', 120)")
  await db.execute("INSERT INTO orders (tenant_id, reference, status, total) VALUES ('acme', 'A-2', 'open', 340)")
  await db.execute("INSERT INTO orders (tenant_id, reference, status, total) VALUES ('other', 'B-1', 'open', 900)")
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('live queries over WebSocket', () => {
  it('answers with the first rows of the named read, scoped by the identity', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open' } })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    const subscribed = messagesOfType(conn, 'subscribed')[0]
    expect(subscribed.rows).toEqual([
      { id: 1, reference: 'A-1', total: 120 },
      { id: 2, reference: 'A-2', total: 340 },
    ])
    await handler.close()
  })

  it('sends one operation for a row that enters the result', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open' } })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    await db.execute("INSERT INTO orders (tenant_id, reference, status, total) VALUES ('acme', 'A-3', 'open', 50)")
    await waitFor(() => messagesOfType(conn, 'live').length === 1)

    expect(messagesOfType(conn, 'live')[0].ops).toEqual([
      { op: 'insert', index: 2, row: { id: 4, reference: 'A-3', total: 50 } },
    ])
    await handler.close()
  })

  it('sends a delete for a row updated out of the result and nothing for another tenant', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open' } })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    await db.execute("UPDATE orders SET status = 'cancelled' WHERE reference = 'A-1'")
    await waitFor(() => messagesOfType(conn, 'live').length === 1)
    expect(messagesOfType(conn, 'live')[0].ops).toEqual([{ op: 'delete', index: 0 }])

    await db.execute("UPDATE orders SET total = 999 WHERE reference = 'B-1'")
    await new Promise(resolve => setTimeout(resolve, 200))
    expect(messagesOfType(conn, 'live')).toHaveLength(1)
    await handler.close()
  })

  it('carries a whole transaction in one message', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open' } })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    await db.executeTransaction([
      { sql: "INSERT INTO orders (tenant_id, reference, status, total) VALUES ('acme', 'A-3', 'open', 10)" },
      { sql: "INSERT INTO orders (tenant_id, reference, status, total) VALUES ('acme', 'A-4', 'open', 20)" },
    ])
    await waitFor(() => messagesOfType(conn, 'live').length === 1)

    const ops = messagesOfType(conn, 'live')[0].ops as { op: string; index: number }[]
    expect(ops.map(entry => [entry.op, entry.index])).toEqual([
      ['insert', 2],
      ['insert', 3],
    ])
    await handler.close()
  })

  it('reports a re-read, then replaces the rows, when a transaction outnumbers them', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open' } })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    await db.executeTransaction(
      Array.from({ length: 5 }, (_, index) => ({
        sql: 'INSERT INTO orders (tenant_id, reference, status, total) VALUES (?, ?, ?, ?)',
        params: ['acme', `A-${index + 10}`, 'open', index],
      })),
    )

    await waitFor(() => messagesOfType(conn, 'live').some(msg => msg.rows !== undefined))
    const live = messagesOfType(conn, 'live')
    expect(live[0].revalidating).toBe(true)
    expect(live[live.length - 1].rows).toHaveLength(7)
    await handler.close()
  })

  it('refuses an argument the server fills from the identity', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open', tenant: 'other' } })
    await waitFor(() => messagesOfType(conn, 'error').length === 1)

    expect(messagesOfType(conn, 'error')[0].error).toMatchObject({ code: 'ARGUMENT_NOT_ALLOWED' })
    await handler.close()
  })

  it('refuses an operation needing an identity the connection does not carry', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop')

    subscribe(conn, handler, { id: 'live-1', name: 'openOrders', args: { status: 'open' } })
    await waitFor(() => messagesOfType(conn, 'error').length === 1)

    expect(messagesOfType(conn, 'error')[0].error).toMatchObject({ code: 'IDENTITY_REQUIRED' })
    await handler.close()
  })

  it('refuses an unregistered name and a stale registry digest', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'archivedOrders' })
    await waitFor(() => messagesOfType(conn, 'error').length === 1)
    expect(messagesOfType(conn, 'error')[0].error).toMatchObject({ code: 'UNKNOWN_QUERY' })

    subscribe(conn, handler, { id: 'live-2', name: 'everyOrder', registryDigest: 'a-digest-from-another-build' })
    await waitFor(() => messagesOfType(conn, 'error').length === 2)
    expect(messagesOfType(conn, 'error')[1].error).toMatchObject({ code: 'REGISTRY_MISMATCH' })
    await handler.close()
  })

  it('accepts the digest the server announces', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'everyOrder', registryDigest: operationRegistryDigest(operations) })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    expect(messagesOfType(conn, 'subscribed')[0].rows).toHaveLength(3)
    await handler.close()
  })

  it('refuses a live subscription carrying a table or a resume cursor', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'everyOrder', table: 'orders' })
    await waitFor(() => messagesOfType(conn, 'error').length === 1)
    expect(messagesOfType(conn, 'error')[0].error).toMatchObject({ code: 'INVALID_MESSAGE' })

    subscribe(conn, handler, { id: 'live-2', name: 'everyOrder', sinceSeq: '1' })
    await waitFor(() => messagesOfType(conn, 'error').length === 2)
    expect(messagesOfType(conn, 'error')[1].error).toMatchObject({ code: 'INVALID_MESSAGE' })
    await handler.close()
  })

  it('stops sending after unsubscribe and drops the probe table', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    subscribe(conn, handler, { id: 'live-1', name: 'everyOrder' })
    await waitFor(() => messagesOfType(conn, 'subscribed').length === 1)

    handler.handleMessage(conn, JSON.stringify({ type: 'unsubscribe', id: 'live-1' }))
    await waitFor(() => messagesOfType(conn, 'unsubscribed').length === 1)

    await db.execute("INSERT INTO orders (tenant_id, reference, status, total) VALUES ('acme', 'A-9', 'open', 1)")
    await new Promise(resolve => setTimeout(resolve, 200))
    expect(messagesOfType(conn, 'live')).toHaveLength(0)
    await handler.close()
  })

  it('runs a registered read and a registered write over the same connection without SQL', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    handler.handleMessage(conn, JSON.stringify({ type: 'execute', id: 'w-1', name: 'cancelOrder', args: { id: 1 } }))
    await waitFor(() => messagesOfType(conn, 'result').length === 1)

    handler.handleMessage(
      conn,
      JSON.stringify({ type: 'query', id: 'r-1', name: 'openOrders', args: { status: 'open' } }),
    )
    await waitFor(() => messagesOfType(conn, 'result').length === 2)

    const results = messagesOfType(conn, 'result')
    expect((results[0].data as { results: { changes: number }[] }).results[0].changes).toBe(1)
    expect((results[1].data as { rows: unknown[] }).rows).toEqual([{ id: 2, reference: 'A-2', total: 340 }])
    await handler.close()
  })

  it('refuses SQL on a connection that serves registered operations', async () => {
    const handler = createWSHandler<Identity>(sirannon, { operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'shop', { tenantId: 'acme' })

    handler.handleMessage(conn, JSON.stringify({ type: 'query', id: 'q-1', sql: 'SELECT * FROM orders' }))
    await waitFor(() => messagesOfType(conn, 'error').length === 1)

    expect(messagesOfType(conn, 'error')[0].error).toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await handler.close()
  })
})
