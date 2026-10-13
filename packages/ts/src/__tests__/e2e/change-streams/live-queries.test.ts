import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { type OperationRegistry, operationRef, type ServerOptions } from '../../../core/types.js'
import { postJson, sendRaw, serveRegistry, trackedRegistry } from '../lib/served-registry.js'
import {
  pause,
  QUIET_PERIOD_MS,
  readFile,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  serveWithIdentity,
  type WireMessage,
  WireSocket,
  waitFor,
} from './harness.js'

type Row = Record<string, unknown>

const ORDERS_SCHEMA = [
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, reference TEXT NOT NULL, status TEXT NOT NULL, total INTEGER NOT NULL, ledger INTEGER)',
]
const INSERT_ORDER = 'INSERT INTO orders (tenant, reference, status, total, ledger) VALUES (?, ?, ?, ?, ?)'
const SAFE_MAX = BigInt(Number.MAX_SAFE_INTEGER)
const HUGE_LEDGER = { __sirannon_int: '9223372036854775807' }
const RANDOM_WRITES = 120
const RANDOM_SEED = 4_2026
const RECONNECT_INTERVAL_MS = 50
const OPEN_ORDERS_SQL =
  'SELECT id, reference, total, ledger FROM orders WHERE tenant = ? AND status = ? ORDER BY total, id'
const TOP_THREE_SQL = 'SELECT id, total FROM orders ORDER BY total DESC, id LIMIT 3'
const EVERY_ORDER_SQL = 'SELECT id, reference FROM orders ORDER BY id'

const operations: OperationRegistry<{ tenant: string }> = {
  shop: {
    reads: {
      openOrders: {
        args: ['status'],
        fromIdentity: { tenant: 'tenant' },
        statement: args => ({ sql: OPEN_ORDERS_SQL, params: [args.tenant, args.status] }),
      },
      topThree: { statement: () => ({ sql: TOP_THREE_SQL }) },
      everyOrder: { statement: () => ({ sql: EVERY_ORDER_SQL }) },
      joined: { statement: () => ({ sql: 'SELECT a.id FROM orders a JOIN orders b ON b.id = a.id' }) },
      unordered: { statement: () => ({ sql: 'SELECT id FROM orders LIMIT 2' }) },
    },
    writes: {
      cancel: { args: ['id'], statements: args => ({ sql: 'DELETE FROM orders WHERE id = ?', params: [args.id] }) },
    },
  },
}

const serverOptions: ServerOptions<{ tenant: string }> = {
  operations,
  authenticate: ctx => {
    const token = ctx.headers.authorization ?? ''
    return token.startsWith('Bearer ') ? { tenant: token.slice('Bearer '.length) } : undefined
  },
}

afterEach(releaseChangeStreams)

async function shop(
  label: string,
  registry?: Parameters<typeof serveDatabase>[0]['registry'],
): Promise<ServedDatabase> {
  const served = await serveDatabase({
    label,
    databaseId: 'shop',
    schema: ORDERS_SCHEMA,
    registry,
    server: serverOptions,
  })
  for (const [tenant, reference, status, total] of [
    ['acme', 'A-1', 'open', 30],
    ['acme', 'A-2', 'open', 10],
    ['acme', 'A-3', 'open', 20],
    ['acme', 'A-4', 'paid', 40],
    ['other', 'B-1', 'open', 15],
  ] as const) {
    await served.db.execute(INSERT_ORDER, [tenant, reference, status, total, null])
  }
  return served
}

function wireValue(value: unknown): unknown {
  if (typeof value !== 'bigint') return value
  return value >= -SAFE_MAX && value <= SAFE_MAX ? Number(value) : { __sirannon_int: value.toString() }
}

function fileRows(path: string, sql: string, params: unknown[] = []): Row[] {
  return readFile(path, sql, params).map(row =>
    Object.fromEntries(Object.entries(row).map(([column, value]) => [column, wireValue(value)])),
  )
}

function applied(rows: readonly Row[], message: WireMessage): Row[] {
  if (message.rows !== undefined) return [...message.rows]
  const next = [...rows]
  for (const op of message.ops ?? []) {
    if (op.op === 'insert') next.splice(op.index, 0, op.row ?? {})
    else if (op.op === 'update') next[op.index] = op.row ?? {}
    else next.splice(op.index, 1)
  }
  return next
}

function heldCopy(socket: WireSocket, id: string): Row[] {
  let rows: Row[] = []
  for (const message of socket.of(id)) {
    if (message.type === 'subscribed') rows = [...(message.rows ?? [])]
    if (message.type === 'live') rows = applied(rows, message)
  }
  return rows
}

function nextRandom(state: { seed: number }): number {
  state.seed = (state.seed * 1103515245 + 12345) % 2147483648
  return Math.floor(state.seed / 65536)
}

async function write(served: ServedDatabase, statements: Array<{ sql: string; params?: unknown[] }>): Promise<void> {
  const answer = await postJson(served.served.base, '/db/shop/transaction', { statements })
  expect(answer.status, answer.text).toBe(200)
}

describe('live queries over the WebSocket protocol', () => {
  it('answer with the rows of the named read scoped by the identity, and keep a copy current that matches a read of the file', async () => {
    const served = await shop('live-copies')
    const acme = await WireSocket.open(`${served.served.wsBase}/db/shop`, { authorization: 'Bearer acme' })
    const other = await WireSocket.open(`${served.served.wsBase}/db/shop`, { authorization: 'Bearer other' })
    const acmeOpen = await acme.request({ id: 'open', type: 'subscribe', name: 'openOrders', args: { status: 'open' } })
    await other.request({ id: 'open', type: 'subscribe', name: 'openOrders', args: { status: 'open' } })
    await acme.request({ id: 'top', type: 'subscribe', name: 'topThree' })

    await write(served, [
      { sql: INSERT_ORDER, params: ['acme', 'A-5', 'open', 25, HUGE_LEDGER] },
      { sql: INSERT_ORDER, params: ['acme', 'A-6', 'open', 5, null] },
    ])
    await waitFor(() => acme.of('open', 'live').length === 1, 'the live message for one transaction')
    const random = { seed: RANDOM_SEED }
    for (let step = 0; step < RANDOM_WRITES; step++) {
      const choice = nextRandom(random) % 6
      const id = 1 + (nextRandom(random) % (6 + step))
      const tenant = nextRandom(random) % 3 === 0 ? 'other' : 'acme'
      const status = nextRandom(random) % 2 === 0 ? 'open' : 'paid'
      const total = nextRandom(random) % 50
      if (choice <= 1) await write(served, [{ sql: INSERT_ORDER, params: [tenant, `R-${step}`, status, total, null] }])
      else if (choice === 2)
        await write(served, [{ sql: 'UPDATE orders SET status = ? WHERE id = ?', params: [status, id] }])
      else if (choice === 3)
        await write(served, [{ sql: 'UPDATE orders SET total = ? WHERE id = ?', params: [total, id] }])
      else if (choice === 4) await write(served, [{ sql: 'DELETE FROM orders WHERE id = ?', params: [id] }])
      else {
        await write(served, [
          { sql: 'UPDATE orders SET tenant = ? WHERE id = ?', params: [tenant, id] },
          {
            sql: INSERT_ORDER,
            params: [tenant, `T-${step}`, 'open', total, { __sirannon_int: (SAFE_MAX + BigInt(step)).toString() }],
          },
        ])
      }
    }
    await pause(QUIET_PERIOD_MS * 2)

    expect(acmeOpen.rows).toEqual([
      { id: 2, reference: 'A-2', total: 10, ledger: null },
      { id: 3, reference: 'A-3', total: 20, ledger: null },
      { id: 1, reference: 'A-1', total: 30, ledger: null },
    ])
    expect(acme.of('open', 'live')[0]?.ops).toEqual([
      { op: 'insert', index: 2, row: { id: 6, reference: 'A-5', total: 25, ledger: HUGE_LEDGER } },
      { op: 'insert', index: 0, row: { id: 7, reference: 'A-6', total: 5, ledger: null } },
    ])
    expect(heldCopy(acme, 'open')).toEqual(fileRows(served.path, OPEN_ORDERS_SQL, ['acme', 'open']))
    expect(heldCopy(other, 'open')).toEqual(fileRows(served.path, OPEN_ORDERS_SQL, ['other', 'open']))
    expect(heldCopy(acme, 'top')).toEqual(fileRows(served.path, TOP_THREE_SQL))
    expect(heldCopy(acme, 'open').length).toBeGreaterThan(0)
  })

  it('report a second read and then replace the rows when a transaction holds more changes than the result has rows', async () => {
    const served = await shop('live-reread')
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const subscribed = await ws.request({ id: 'every', type: 'subscribe', name: 'everyOrder' })

    await write(
      served,
      Array.from({ length: 8 }, (_, index) => ({
        sql: INSERT_ORDER,
        params: ['acme', `X-${index}`, 'open', index, null],
      })),
    )
    await waitFor(() => ws.of('every', 'live').some(message => message.rows !== undefined), 'the replaced rows')
    await pause(QUIET_PERIOD_MS)

    expect(subscribed.rows).toHaveLength(5)
    expect(
      ws.of('every', 'live').map(message => (message.revalidating === true ? 'revalidating' : message.rows)),
    ).toEqual(['revalidating', fileRows(served.path, EVERY_ORDER_SQL)])
  })

  it('refuse a subscription that names no read, breaks the argument rules, carries a stale digest or a table field, or names a read it cannot keep current', async () => {
    const served = await shop('live-refusals')
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`, { authorization: 'Bearer acme' })
    const anonymous = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const capabilities = await sendRaw(served.served.base, 'GET', '/capabilities')
    const digest = (capabilities.body as { registry: { digest: string } }).registry.digest
    const refusals: Array<[label: string, message: Record<string, unknown>, code: string]> = [
      ['an unregistered name', { name: 'archivedOrders' }, 'UNKNOWN_QUERY'],
      ['a write', { name: 'cancel', args: { id: 1 } }, 'UNKNOWN_QUERY'],
      [
        'an argument filled from the identity',
        { name: 'openOrders', args: { status: 'open', tenant: 'other' } },
        'ARGUMENT_NOT_ALLOWED',
      ],
      [
        'an undeclared argument',
        { name: 'openOrders', args: { status: 'open', region: 'emea' } },
        'ARGUMENT_NOT_ALLOWED',
      ],
      [
        'a prototype key as an argument',
        { name: 'everyOrder', args: JSON.parse('{"__proto__":{"status":"open"}}') },
        'ARGUMENT_NOT_ALLOWED',
      ],
      ['a missing argument', { name: 'openOrders' }, 'MISSING_ARGUMENT'],
      ['a stale digest', { name: 'everyOrder', registryDigest: 'a-digest-from-another-build' }, 'REGISTRY_MISMATCH'],
      ['a numeric digest', { name: 'everyOrder', registryDigest: 5 }, 'INVALID_MESSAGE'],
      ['a list of arguments', { name: 'openOrders', args: ['open'] }, 'INVALID_MESSAGE'],
      ['text as the arguments', { name: 'openOrders', args: 'open' }, 'INVALID_MESSAGE'],
      [
        'a malformed envelope argument',
        { name: 'openOrders', args: { status: { __sirannon_int: 'x' } } },
        'INVALID_MESSAGE',
      ],
      ['a table', { name: 'everyOrder', table: 'orders' }, 'INVALID_MESSAGE'],
      ['a table list', { name: 'everyOrder', tables: ['orders'] }, 'INVALID_MESSAGE'],
      ['a filter', { name: 'everyOrder', filter: { id: 1 } }, 'INVALID_MESSAGE'],
      ['a cursor', { name: 'everyOrder', sinceSeq: '1' }, 'INVALID_MESSAGE'],
      ['an epoch', { name: 'everyOrder', epoch: 'abc' }, 'INVALID_MESSAGE'],
      ['a device', { name: 'everyOrder', deviceId: 'a'.repeat(32) }, 'INVALID_MESSAGE'],
      ['a schema version', { name: 'everyOrder', schemaVersion: 1 }, 'INVALID_MESSAGE'],
      ['a join', { name: 'joined' }, 'CDC_ERROR'],
      ['a limit without an order', { name: 'unordered' }, 'CDC_ERROR'],
    ]

    for (const [index, [label, message, code]] of refusals.entries()) {
      const reply = await ws.request({ ...message, id: `refused-${index}`, type: 'subscribe' })
      expect([reply.type, reply.error?.code], label).toEqual(['error', code])
    }
    const unidentified = await anonymous.request({
      id: 'scoped',
      type: 'subscribe',
      name: 'openOrders',
      args: { status: 'open' },
    })
    const accepted = await ws.request({ id: 'accepted', type: 'subscribe', name: 'everyOrder', registryDigest: digest })
    const duplicate = await ws.request({ id: 'accepted', type: 'subscribe', name: 'topThree' })
    await write(served, [{ sql: INSERT_ORDER, params: ['acme', 'A-9', 'open', 1, null] }])
    await waitFor(() => ws.of('accepted', 'live').length > 0, 'the accepted live query')
    await pause(QUIET_PERIOD_MS)

    expect([unidentified.type, unidentified.error?.code]).toEqual(['error', 'IDENTITY_REQUIRED'])
    expect(accepted.rows).toEqual(fileRows(served.path, EVERY_ORDER_SQL).slice(0, 5))
    expect([duplicate.type, duplicate.error?.code]).toEqual(['error', 'DUPLICATE_SUBSCRIPTION'])
    expect(ws.messages.filter(message => message.type === 'live').map(message => message.id)).toEqual(['accepted'])
  })

  it('stop after an unsubscribe or a closed socket, even while the query is still opening, and leave no query reading', async () => {
    const liveReads: string[] = []
    const served = await shop('live-release', {
      metrics: {
        onQueryComplete: metrics => {
          if (metrics.source === 'live') liveReads.push(metrics.sql)
        },
      },
    })
    const settled = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const racing = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const closing = await WireSocket.open(`${served.served.wsBase}/db/shop`)

    await settled.request({ id: 'every', type: 'subscribe', name: 'everyOrder' })
    const unsubscribed = await settled.request({ id: 'every', type: 'unsubscribe' })
    racing.send({ id: 'every', type: 'subscribe', name: 'everyOrder' })
    const raced = await racing.request({ id: 'every', type: 'unsubscribe' }, ['unsubscribed', 'error'])
    closing.send({ id: 'every', type: 'subscribe', name: 'topThree' })
    closing.close()
    await closing.closed
    await pause(QUIET_PERIOD_MS)
    const readsBefore = liveReads.length
    for (const total of [1, 2, 3])
      await write(served, [{ sql: INSERT_ORDER, params: ['acme', `Q-${total}`, 'open', total, null] }])
    await pause(QUIET_PERIOD_MS)
    const readsWhileReleased = liveReads.length - readsBefore
    const control = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await control.request({ id: 'control', type: 'subscribe', name: 'everyOrder' })
    await write(served, [{ sql: INSERT_ORDER, params: ['acme', 'Q-4', 'open', 4, null] }])
    await waitFor(() => control.of('control', 'live').length > 0, 'the control live query')

    expect([unsubscribed.type, raced.type]).toEqual(['unsubscribed', 'unsubscribed'])
    expect(readsWhileReleased).toBe(0)
    expect(liveReads.length).toBeGreaterThan(readsBefore)
    expect([...settled.messages, ...racing.messages].filter(message => message.type === 'live')).toEqual([])
  })

  it('report CDC_ERROR once when the read can no longer run, free the identifier, and keep the rows written to the file', async () => {
    const served = await shop('live-failure')
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await ws.request({ id: 'every', type: 'subscribe', name: 'everyOrder' })

    const renamed = await postJson(served.served.base, '/db/shop/execute', {
      sql: 'ALTER TABLE orders RENAME COLUMN reference TO ref',
    })
    await write(
      served,
      Array.from({ length: 8 }, (_, index) => ({
        sql: 'INSERT INTO orders (tenant, ref, status, total) VALUES (?, ?, ?, ?)',
        params: ['acme', `F-${index}`, 'open', index],
      })),
    )
    await waitFor(() => ws.of('every', 'error').length > 0, 'the failure')
    const reused = await ws.request({ id: 'every', type: 'subscribe', table: 'orders' })
    await write(served, [
      { sql: 'INSERT INTO orders (tenant, ref, status, total) VALUES (?, ?, ?, ?)', params: ['acme', 'G', 'open', 1] },
    ])
    await ws.waitForEvents('every', 1)

    expect(renamed.status, renamed.text).toBe(200)
    expect(ws.of('every', 'error').map(message => message.error?.code)).toEqual(['CDC_ERROR'])
    expect(ws.of('every', 'live').map(message => message.revalidating)).toEqual([true])
    expect(reused.type).toBe('subscribed')
    expect(readFile(served.path, 'SELECT count(*) AS n FROM orders')).toEqual([{ n: 14n }])
  })

  it('serve a shared read as a live query on a database with no operations of its own', async () => {
    const served = await serveDatabase({ label: 'live-shared', databaseId: 'branch', schema: ORDERS_SCHEMA })
    await served.served.server.close()
    const shared = await serveRegistry(served.sirannon, {
      sharedOperations: { reads: { everyOrder: { statement: () => ({ sql: EVERY_ORDER_SQL }) } } },
    })
    const capabilities = await sendRaw(shared.base, 'GET', '/capabilities')
    const digest = (capabilities.body as { registry: { digest: string } }).registry.digest
    const ws = await WireSocket.open(`${shared.wsBase}/db/branch`)

    const subscribed = await ws.request({ id: 'shared', type: 'subscribe', name: 'everyOrder', registryDigest: digest })
    await served.db.execute(INSERT_ORDER, ['acme', 'S-1', 'open', 1, null])
    await waitFor(() => ws.of('shared', 'live').some(message => message.rows !== undefined), 'the shared live query')

    expect(subscribed.rows).toEqual([])
    expect(heldCopy(ws, 'shared')).toEqual(fileRows(served.path, EVERY_ORDER_SQL))
  })

  it('keep a published-client live query current across a server restart', async () => {
    const served = await shop('live-client-restart')
    const port = served.served.port
    const client = new SirannonClient(served.served.base, { reconnectInterval: RECONNECT_INTERVAL_MS })
    const everyOrder = operationRef<Record<string, never>, { id: number; reference: string }>('everyOrder')
    const query = await client.database('shop').live(everyOrder, {})
    const rowsHeld = () => {
      const state = query.getState()
      return state.status === 'ready' ? state.rows : []
    }

    await served.db.execute(INSERT_ORDER, ['acme', 'C-1', 'open', 1, null])
    await waitFor(() => rowsHeld().length === 6, 'the first change')
    await served.served.server.close()
    await served.sirannon.shutdown()
    const second = trackedRegistry()
    const reopened = await second.open('shop', served.path)
    await reopened.execute(INSERT_ORDER, ['acme', 'C-2', 'open', 2, null])
    await serveWithIdentity(second, { ...serverOptions, port, acceptSql: true })
    await waitFor(() => rowsHeld().length === 7, 'the rows read again after the restart')
    await reopened.execute('DELETE FROM orders WHERE id = 1')
    await waitFor(() => rowsHeld().length === 6, 'the change after the restart')
    await query.close()
    client.close()

    expect(rowsHeld()).toEqual(fileRows(served.path, EVERY_ORDER_SQL))
  })
})
