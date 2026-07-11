import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { ServerExecutionTarget } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { WS_CLOSE_OVERLOADED } from '../../ws-connection.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, parseMessages } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

async function flushAsync(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`flushAsync timed out after ${timeout}ms: condition never became true`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-bp-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler backpressure', () => {
  it('closes the connection with an overload code when a reply is dropped', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'reply.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')

    conn.sendOutcome = 'dropped'
    handler.handleMessage(conn, JSON.stringify({ id: 'q-1', type: 'query', sql: 'SELECT * FROM users' }))

    await flushAsync(() => conn.closed)

    expect(conn.closed).toBe(true)
    expect(conn.closeCode).toBe(WS_CLOSE_OVERLOADED)
    expect(conn.messages).toHaveLength(0)
    await handler.close()
  })

  it('closes the connection when a CDC change event is dropped rather than losing it silently', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'change.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users' }))
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))

    conn.sendOutcome = 'dropped'
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await flushAsync(() => conn.closed)

    expect(conn.closed).toBe(true)
    expect(conn.closeCode).toBe(WS_CLOSE_OVERLOADED)
    expect(parseMessages(conn).some(m => m.type === 'change')).toBe(false)
    await handler.close()
  })

  it('closes the connection when a reply cannot be serialised rather than reporting it sent', async () => {
    const poisonRow = {
      toJSON: () => {
        throw new Error('unserialisable value')
      },
    }
    const unserialisableTarget: ServerExecutionTarget = {
      query: async <T>() => [poisonRow] as T[],
      execute: async () => ({ changes: 0, lastInsertRowId: 0 }),
      transaction: async fn => fn({} as never),
    }
    const handler = createWSHandler(sirannon, { resolveExecutionTarget: () => unserialisableTarget })
    await sirannon.open('mydb', join(tempDir, 'serialise.db'))

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'q-1', type: 'query', sql: 'SELECT balance FROM ledgers' }))

    await flushAsync(() => conn.closed)

    expect(conn.closed).toBe(true)
    expect(conn.closeCode).toBe(WS_CLOSE_OVERLOADED)
    expect(conn.messages).toHaveLength(0)
    await handler.close()
  })

  it('keeps serving when a frame is buffered under the backpressure limit', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'buffered.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')

    conn.sendOutcome = 'buffered'
    handler.handleMessage(conn, JSON.stringify({ id: 'q-1', type: 'query', sql: 'SELECT * FROM users' }))

    await flushAsync(() => conn.messages.length >= 1)

    expect(conn.closed).toBe(false)
    expect(parseMessages(conn).some(m => m.type === 'result')).toBe(true)
    await handler.close()
  })
})
