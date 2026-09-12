import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, type WSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages, wait } from '../helpers.js'

const DEVICE = 'bbbb1111bbbb1111bbbb1111bbbb1111'

let tempDir: string
let sirannon: Sirannon
let handler: WSHandler
let db: Database
let conn: MockWSConnection

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-subscribe-lifecycle-'))
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('mydb', join(tempDir, 'lifecycle.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  handler = createWSHandler(sirannon, { acceptSql: true })
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
})

afterEach(async () => {
  await handler.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function until(predicate: () => boolean, timeout = 3000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await wait(10)
  }
}

function messagesOfType(type: string): Record<string, unknown>[] {
  return parseMessages(conn).filter(msg => msg.type === type)
}

describe('subscriptions on a connection that is going away', () => {
  it('drops a subscription whose socket closed while it was opening', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    handler.handleClose(conn)

    await wait(100)
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'after the close')")
    await wait(200)

    expect(messagesOfType('change')).toHaveLength(0)
  })

  it('drops a device subscription whose socket closed while it was opening', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', tables: ['notes'], deviceId: DEVICE }))
    handler.handleClose(conn)

    await wait(100)
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'after the close')")
    await wait(200)

    expect(messagesOfType('change')).toHaveLength(0)
  })

  it('drops a resuming subscription whose socket closed while it was opening', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes', sinceSeq: '0' }))
    handler.handleClose(conn)

    await wait(100)
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'after the close')")
    await wait(200)

    expect(messagesOfType('change')).toHaveLength(0)
  })

  it('refuses the second of two subscriptions racing for one identifier and delivers each change once', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    await until(() => messagesOfType('subscribed').length + messagesOfType('error').length === 2)

    expect(messagesOfType('subscribed')).toHaveLength(1)
    expect(messagesOfType('error')[0]).toMatchObject({ id: 's1', error: { code: 'DUPLICATE_SUBSCRIPTION' } })

    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'once')")
    await until(() => messagesOfType('change').length > 0)
    await wait(200)

    expect(messagesOfType('change')).toHaveLength(1)
  })

  it('stops delivering once the surviving subscription is unsubscribed', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    await until(() => messagesOfType('subscribed').length + messagesOfType('error').length === 2)

    handler.handleMessage(conn, JSON.stringify({ type: 'unsubscribe', id: 's1' }))
    await until(() => messagesOfType('unsubscribed').length === 1)

    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'never delivered')")
    await wait(200)

    expect(messagesOfType('change')).toHaveLength(0)
  })
})
