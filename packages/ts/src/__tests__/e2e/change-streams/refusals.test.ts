import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { OperationRegistry } from '../../../core/types.js'
import { singleConnectionDriver } from '../../helpers/fault-drivers/single-connection-driver.js'
import { countSqlite, postJson, serveRegistry, trackedRegistry } from '../lib/served-registry.js'
import {
  pause,
  QUIET_PERIOD_MS,
  readFile,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  triggerNames,
  WireSocket,
} from './harness.js'

const ORDERS_SCHEMA = [
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, status TEXT NOT NULL)',
  "CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open'",
  "INSERT INTO orders (customer, status) VALUES ('ama', 'open'), ('kofi', 'paid')",
]
const TOO_MANY_TABLES = 501
const LONG_NAME_LENGTH = 100_000

const operations: OperationRegistry = {
  shop: { reads: { everyOrder: { statement: () => ({ sql: 'SELECT id, customer FROM orders ORDER BY id' }) } } },
  memory: { reads: { everyNote: { statement: () => ({ sql: 'SELECT id FROM notes ORDER BY id' }) } } },
}

afterEach(releaseChangeStreams)

function shop(): Promise<ServedDatabase> {
  return serveDatabase({ label: 'refusals', databaseId: 'shop', schema: ORDERS_SCHEMA, server: { operations } })
}

async function stillServes(ws: WireSocket, served: ServedDatabase, id: string): Promise<void> {
  const subscribed = await ws.request({ id, type: 'subscribe', table: 'orders' })
  await postJson(served.served.base, '/db/shop/execute', {
    sql: "INSERT INTO orders (customer, status) VALUES ('after', 'open')",
  })
  const [event] = await ws.waitForEvents(id, 1)
  expect(subscribed.type).toBe('subscribed')
  expect(event?.row.customer).toBe('after')
}

describe('subscribe messages built to break the server', () => {
  it('answers each malformed subscribe or unsubscribe message with INVALID_MESSAGE, installs nothing, and keeps serving', async () => {
    const served = await shop()
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const malformed: Array<[label: string, message: Record<string, unknown>]> = [
      ['no table', {}],
      ['a numeric table', { table: 5 }],
      ['a null table', { table: null }],
      ['an object table', { table: { name: 'orders' } }],
      ['a list as the table', { table: ['orders'] }],
      ['a boolean table', { table: true }],
      ['an empty table list', { tables: [] }],
      ['a string table list', { tables: 'orders' }],
      ['a numeric entry in the table list', { tables: [5] }],
      ['an empty entry in the table list', { tables: [''] }],
      ['one table listed without a device', { tables: ['orders'] }],
      ['two tables listed without a device', { tables: ['orders', 'orders'] }],
      ['too many tables', { tables: Array.from({ length: TOO_MANY_TABLES }, () => 'orders') }],
      ['a staged stream without a device', { table: 'orders', stagedStream: true }],
      ['a staged stream that is not a boolean', { table: 'orders', stagedStream: 'yes' }],
      ['a list as the filter', { table: 'orders', filter: ['open'] }],
      ['a string as the filter', { table: 'orders', filter: 'open' }],
      ['a number as the filter', { table: 'orders', filter: 5 }],
      ['true as the filter', { table: 'orders', filter: true }],
      ['an integer envelope with letters', { table: 'orders', filter: { id: { __sirannon_int: '12abc' } } }],
      ['an integer envelope with a fraction', { table: 'orders', filter: { id: { __sirannon_int: '1.5' } } }],
      ['an integer envelope of 20 digits', { table: 'orders', filter: { id: { __sirannon_int: '1'.repeat(20) } } }],
      ['a BLOB envelope of odd length', { table: 'orders', filter: { customer: { __sirannon_blob: 'F' } } }],
      ['a BLOB envelope that is not hex', { table: 'orders', filter: { customer: { __sirannon_blob: 'GG' } } }],
      ['a nested malformed envelope', { table: 'orders', filter: { customer: { inner: { __sirannon_int: 'x' } } } }],
      ['a cursor of letters', { table: 'orders', sinceSeq: 'abc' }],
      ['a negative cursor', { table: 'orders', sinceSeq: '-1' }],
      ['a fractional cursor', { table: 'orders', sinceSeq: '1.5' }],
      ['an empty cursor', { table: 'orders', sinceSeq: '' }],
      ['a cursor with a leading space', { table: 'orders', sinceSeq: ' 1' }],
      ['a hexadecimal cursor', { table: 'orders', sinceSeq: '0x10' }],
      ['a cursor in exponent form', { table: 'orders', sinceSeq: '1e3' }],
      ['a numeric cursor', { table: 'orders', sinceSeq: 5 }],
      ['a null cursor', { table: 'orders', sinceSeq: null }],
      ['a numeric epoch', { table: 'orders', sinceSeq: '0', epoch: 5 }],
      ['an object epoch', { table: 'orders', sinceSeq: '0', epoch: {} }],
      ['a null epoch', { table: 'orders', epoch: null }],
    ]

    for (const [index, [label, message]] of malformed.entries()) {
      const reply = await ws.request({ ...message, id: `bad-${index}`, type: 'subscribe' })
      expect([reply.type, reply.error?.code], label).toEqual(['error', 'INVALID_MESSAGE'])
    }
    ws.sendText('{"type":"unsubscribe"}')
    ws.sendText('{"type":"unsubscribe","id":7}')
    ws.sendText('{"type":"subscribe","id":["feed"],"table":"orders"}')
    await pause(QUIET_PERIOD_MS)
    const unanswered = ws.messages.slice(-3)

    expect(unanswered.map(message => [message.id, message.error?.code])).toEqual([
      ['', 'INVALID_MESSAGE'],
      ['7', 'INVALID_MESSAGE'],
      ['', 'INVALID_MESSAGE'],
    ])
    expect(ws.messages.filter(message => message.type !== 'error')).toEqual([])
    expect(triggerNames(served.path)).toEqual([])
    await stillServes(ws, served, 'valid')
  })

  it('refuses a table that is unknown, reserved, or not a plain identifier, and installs no trigger for it', async () => {
    const served = await shop()
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const refused: Array<[table: string, code: string]> = [
      ['missing', 'CDC_ERROR'],
      ['', 'CDC_ERROR'],
      ['orders; DROP TABLE orders', 'CDC_ERROR'],
      ['"orders"', 'CDC_ERROR'],
      ['orders--', 'CDC_ERROR'],
      ['orders WHERE 1', 'CDC_ERROR'],
      ['ordërs', 'CDC_ERROR'],
      ['1orders', 'CDC_ERROR'],
      ['a'.repeat(LONG_NAME_LENGTH), 'CDC_ERROR'],
      ['__proto__', 'CDC_ERROR'],
      ['constructor', 'CDC_ERROR'],
      ['sqlite_master', 'FORBIDDEN_SQL'],
      ['sqlite_schema', 'FORBIDDEN_SQL'],
      ['_sirannon_changes', 'FORBIDDEN_SQL'],
      ['_sirannon_meta', 'FORBIDDEN_SQL'],
    ]

    for (const [index, [table, code]] of refused.entries()) {
      const reply = await ws.request({ id: `table-${index}`, type: 'subscribe', table })
      expect([reply.type, reply.error?.code], table.slice(0, 40)).toEqual(['error', code])
    }

    expect(triggerNames(served.path)).toEqual([])
    expect(countSqlite(served.path, 'orders')).toBe(2)
    await stillServes(ws, served, 'valid')
  })

  it('refuses a view with CDC_ERROR and keeps delivering to subscribers of orders after another client subscribes to ORDERS', async () => {
    const served = await shop()
    const victim = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const attacker = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await victim.request({ id: 'orders', type: 'subscribe', table: 'orders' })
    const view = await attacker.request({ id: 'view', type: 'subscribe', table: 'open_orders' })
    await attacker.request({ id: 'shouted', type: 'subscribe', table: 'ORDERS' })
    await postJson(served.served.base, '/db/shop/execute', {
      sql: "INSERT INTO orders (customer, status) VALUES ('esi', 'open')",
    })
    const [event] = await victim.waitForEvents('orders', 1)
    const [shoutedEvent] = await attacker.waitForEvents('shouted', 1)

    expect([view.type, view.error?.code]).toEqual(['error', 'CDC_ERROR'])
    expect(event?.row.customer).toBe('esi')
    expect(shoutedEvent?.row.customer).toBe('esi')
    expect(readFile<{ table_name: string }>(served.path, 'SELECT DISTINCT table_name FROM _sirannon_changes')).toEqual([
      { table_name: 'orders' },
    ])
  })

  it('keeps keys that name the object prototype as plain data in messages, filters, and change rows', async () => {
    const served = await serveDatabase({
      label: 'prototype-keys',
      databaseId: 'shop',
      schema: ['CREATE TABLE quirks (id INTEGER PRIMARY KEY, "constructor" TEXT, "__proto__" TEXT, "toString" TEXT)'],
    })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)

    ws.sendText('{"__proto__":{"table":"quirks"},"type":"subscribe","id":"inherited-table"}')
    await pause(QUIET_PERIOD_MS)
    ws.sendText('{"type":"subscribe","id":"everything","table":"quirks","__proto__":{"filter":{"id":999}}}')
    ws.sendText('{"type":"subscribe","id":"shadowed","table":"quirks","filter":{"__proto__":"shadow"}}')
    ws.sendText('{"type":"subscribe","id":"made","table":"quirks","filter":{"constructor":"maker"}}')
    ws.sendText('{"type":"subscribe","id":"absent","table":"quirks","filter":{"hasOwnProperty":"x"}}')
    await pause(QUIET_PERIOD_MS)
    await postJson(served.served.base, '/db/shop/batch', {
      sql: 'INSERT INTO quirks ("constructor", "__proto__", "toString") VALUES (?, ?, ?)',
      paramsBatch: [
        ['maker', 'shadow', 'first'],
        ['other', 'plain', 'second'],
      ],
    })
    await ws.waitForEvents('everything', 2)
    const firstRow = JSON.parse('{"id":1,"constructor":"maker","__proto__":"shadow","toString":"first"}')
    const secondRow = JSON.parse('{"id":2,"constructor":"other","__proto__":"plain","toString":"second"}')

    expect(ws.of('inherited-table').map(message => message.error?.code)).toEqual(['INVALID_MESSAGE'])
    expect(ws.events('everything').map(event => event.row)).toEqual([firstRow, secondRow])
    expect(Object.hasOwn(ws.events('everything')[0]?.row ?? {}, '__proto__')).toBe(true)
    expect(ws.events('shadowed').map(event => event.row)).toEqual([firstRow])
    expect(ws.events('made').map(event => event.row)).toEqual([firstRow])
    expect(ws.events('absent')).toEqual([])
    expect(ws.of('absent', 'subscribed')).toHaveLength(1)
    expect(({} as Record<string, unknown>).filter).toBeUndefined()
    expect(Object.hasOwn(Object.prototype, 'table')).toBe(false)
    expect(Object.hasOwn(Object.prototype, 'filter')).toBe(false)
  })

  it('refuses table subscriptions and live queries on a read-only database and on an in-memory database', async () => {
    const served = await shop()
    const readOnly = trackedRegistry()
    await readOnly.open('shop', served.path, { readOnly: true })
    const readOnlyServer = await serveRegistry(readOnly, { acceptSql: true, operations })
    const inMemory = trackedRegistry({ driver: singleConnectionDriver() })
    const memoryDb = await inMemory.open('memory', ':memory:')
    await memoryDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY)')
    const memoryServer = await serveRegistry(inMemory, { acceptSql: true, operations })
    const onReadOnly = await WireSocket.open(`${readOnlyServer.wsBase}/db/shop`)
    const onMemory = await WireSocket.open(`${memoryServer.wsBase}/db/memory`)

    const replies = [
      await onReadOnly.request({ id: 'table', type: 'subscribe', table: 'orders' }),
      await onReadOnly.request({ id: 'resume', type: 'subscribe', table: 'orders', sinceSeq: '0' }),
      await onReadOnly.request({ id: 'live', type: 'subscribe', name: 'everyOrder' }),
      await onMemory.request({ id: 'table', type: 'subscribe', table: 'notes' }),
      await onMemory.request({ id: 'live', type: 'subscribe', name: 'everyNote' }),
    ]

    expect(replies.map(reply => [reply.type, reply.error?.code])).toEqual([
      ['error', 'READ_ONLY'],
      ['error', 'READ_ONLY'],
      ['error', 'READ_ONLY'],
      ['error', 'CDC_UNSUPPORTED'],
      ['error', 'CDC_UNSUPPORTED'],
    ])
    expect(triggerNames(served.path)).toEqual([])
    expect(readFile(join(served.dir, 'shop.db'), 'SELECT customer FROM orders ORDER BY id')).toEqual([
      { customer: 'ama' },
      { customer: 'kofi' },
    ])
  })
})
