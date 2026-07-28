import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

async function flushAsync(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`flushAsync timed out after ${timeout}ms: condition never became true`)
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

interface WireEvent {
  type: string
  row: Record<string, unknown>
  seq: string
  txId?: string
  txEnd?: boolean
}

function changeEvents(conn: MockWSConnection): WireEvent[] {
  return parseMessages(conn)
    .filter(message => message.type === 'change')
    .map(message => message.event as unknown as WireEvent)
}

async function subscribe(handler: ReturnType<typeof createWSHandler>, table: string, filter?: unknown) {
  const conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
  handler.handleMessage(
    conn,
    JSON.stringify({ id: 'sub-1', type: 'subscribe', table, ...(filter === undefined ? {} : { filter }) }),
  )
  await flushAsync(() => parseMessages(conn).some(message => message.type === 'subscribed'))
  return conn
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-txend-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('transaction boundaries on an ordinary subscription', () => {
  it('marks the last change of a transaction and no earlier one', async () => {
    const handler = createWSHandler(sirannon, { acceptSql: true })
    const db = await sirannon.open('mydb', join(tempDir, 'txend.db'))
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')

    const conn = await subscribe(handler, 'orders')

    await db.executeTransaction([
      { sql: 'INSERT INTO orders (id, total) VALUES (1, 100)' },
      { sql: 'INSERT INTO orders (id, total) VALUES (2, 200)' },
      { sql: 'INSERT INTO orders (id, total) VALUES (3, 300)' },
    ])
    await flushAsync(() => changeEvents(conn).length >= 3)

    const events = changeEvents(conn)
    expect(events).toHaveLength(3)
    expect(events.map(event => event.txEnd)).toEqual([undefined, undefined, true])
    expect(new Set(events.map(event => event.txId)).size).toBe(1)

    await handler.close()
  })

  it('marks each single-statement write as its own transaction', async () => {
    const handler = createWSHandler(sirannon, { acceptSql: true })
    const db = await sirannon.open('mydb', join(tempDir, 'txend-single.db'))
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')

    const conn = await subscribe(handler, 'orders')

    await db.execute('INSERT INTO orders (id, total) VALUES (1, 100)')
    await db.execute('INSERT INTO orders (id, total) VALUES (2, 200)')
    await flushAsync(() => changeEvents(conn).length >= 2)

    const events = changeEvents(conn)
    expect(events).toHaveLength(2)
    expect(events.map(event => event.txEnd)).toEqual([true, true])

    await handler.close()
  })

  it('marks the last change that survives the filter, not the last of the transaction', async () => {
    const handler = createWSHandler(sirannon, { acceptSql: true })
    const db = await sirannon.open('mydb', join(tempDir, 'txend-filter.db'))
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT)')

    const conn = await subscribe(handler, 'orders', { region: 'emea' })

    await db.executeTransaction([
      { sql: "INSERT INTO orders (id, region) VALUES (1, 'emea')" },
      { sql: "INSERT INTO orders (id, region) VALUES (2, 'emea')" },
      { sql: "INSERT INTO orders (id, region) VALUES (3, 'apac')" },
    ])
    await flushAsync(() => changeEvents(conn).length >= 2)
    await new Promise(resolve => setTimeout(resolve, 150))

    const events = changeEvents(conn)
    expect(events).toHaveLength(2)
    expect(events.map(event => event.row.id)).toEqual([1, 2])
    expect(events.map(event => event.txEnd)).toEqual([undefined, true])

    await handler.close()
  })

  it('marks replayed history for a resuming subscriber', async () => {
    const handler = createWSHandler(sirannon, { acceptSql: true })
    const db = await sirannon.open('mydb', join(tempDir, 'txend-resume.db'))
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
    await db.watch('orders')

    const live = await subscribe(handler, 'orders')
    const subscribed = parseMessages(live).find(message => message.type === 'subscribed') as Record<string, unknown>
    const cursor = subscribed.seq as string
    const epoch = subscribed.epoch as string

    await db.executeTransaction([
      { sql: 'INSERT INTO orders (id, total) VALUES (1, 100)' },
      { sql: 'INSERT INTO orders (id, total) VALUES (2, 200)' },
    ])
    await flushAsync(() => changeEvents(live).length >= 2)

    const resumed = createMockConnection()
    await handler.handleOpen(resumed, 'mydb')
    handler.handleMessage(
      resumed,
      JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'orders', sinceSeq: cursor, epoch }),
    )
    await flushAsync(() => changeEvents(resumed).length >= 2)

    const replayed = changeEvents(resumed)
    expect(replayed).toHaveLength(2)
    expect(replayed.map(event => event.txEnd)).toEqual([undefined, true])

    await handler.close()
  })
})
