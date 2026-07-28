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
  oldRow?: Record<string, unknown>
  seq: string
}

function changeEvents(conn: MockWSConnection): WireEvent[] {
  return parseMessages(conn)
    .filter(message => message.type === 'change')
    .map(message => message.event as unknown as WireEvent)
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-filter-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('filtered WebSocket subscriptions', () => {
  it('reports a row updated out of the filter as a delete and one updated in as an insert', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'boundary.db'))
    await db.execute('CREATE TABLE tickets (id INTEGER PRIMARY KEY, status TEXT, title TEXT)')
    await db.execute("INSERT INTO tickets (id, status, title) VALUES (1, 'open', 'Broken link')")
    await db.execute("INSERT INTO tickets (id, status, title) VALUES (2, 'closed', 'Slow query')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(
      conn,
      JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'tickets', filter: { status: 'open' } }),
    )
    await flushAsync(() => parseMessages(conn).some(message => message.type === 'subscribed'))

    await db.execute("UPDATE tickets SET status = 'closed' WHERE id = 1")
    await db.execute("UPDATE tickets SET status = 'open' WHERE id = 2")
    await flushAsync(() => changeEvents(conn).length >= 2)

    const events = changeEvents(conn)
    expect(events).toHaveLength(2)
    expect(events[0].type).toBe('delete')
    expect(events[0].oldRow).toMatchObject({ id: 1, status: 'open' })
    expect(events[1].type).toBe('insert')
    expect(events[1].row).toMatchObject({ id: 2, status: 'open' })
    expect(events[1].oldRow).toBeUndefined()

    await handler.close()
  })

  it('reports the same boundary crossings when replaying to a resuming subscriber', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'boundary-resume.db'))
    await db.execute('CREATE TABLE tickets (id INTEGER PRIMARY KEY, status TEXT)')
    await db.watch('tickets')
    await db.execute("INSERT INTO tickets (id, status) VALUES (1, 'open')")

    const first = createMockConnection()
    await handler.handleOpen(first, 'mydb')
    handler.handleMessage(
      first,
      JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'tickets', filter: { status: 'open' } }),
    )
    await flushAsync(() => parseMessages(first).some(message => message.type === 'subscribed'))
    const subscribed = parseMessages(first).find(message => message.type === 'subscribed') as Record<string, unknown>
    const cursor = subscribed.seq as string
    const epoch = subscribed.epoch as string

    await db.execute("UPDATE tickets SET status = 'closed' WHERE id = 1")
    await flushAsync(() => changeEvents(first).length >= 1)

    const resumed = createMockConnection()
    await handler.handleOpen(resumed, 'mydb')
    handler.handleMessage(
      resumed,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'tickets',
        filter: { status: 'open' },
        sinceSeq: cursor,
        epoch,
      }),
    )
    await flushAsync(() => changeEvents(resumed).length >= 1)

    const replayed = changeEvents(resumed)
    expect(replayed).toHaveLength(1)
    expect(replayed[0].type).toBe('delete')
    expect(replayed[0].oldRow).toMatchObject({ id: 1, status: 'open' })

    await handler.close()
  })
})
