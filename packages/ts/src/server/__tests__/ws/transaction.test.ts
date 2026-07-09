import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, type WSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage, type MockWSConnection } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon
let handler: WSHandler
let db: Database
let conn: MockWSConnection

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

function hasMessageCount(target: MockWSConnection, count: number): () => boolean {
  return () => target.messages.length >= count
}

async function countUsers(): Promise<number> {
  const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM users')
  return rows[0].count
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-'))
  sirannon = new Sirannon({ driver })
  handler = createWSHandler(sirannon)
  db = await sirannon.open('mydb', join(tempDir, 'tx.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
})

afterEach(async () => {
  await handler.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler handleMessage - transaction', () => {
  it('executes multiple statements in one transaction and returns all results', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 't1',
        type: 'transaction',
        statements: [
          { sql: 'INSERT INTO users (name, age) VALUES (?, ?)', params: ['Alice', 30] },
          { sql: 'INSERT INTO users (name, age) VALUES (:name, :age)', params: { name: 'Bob', age: 25 } },
        ],
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.id).toBe('t1')
    expect(msg.type).toBe('result')
    const data = msg.data as { results: { changes: number }[] }
    expect(data.results).toHaveLength(2)
    expect(data.results[0].changes).toBe(1)
    expect(await countUsers()).toBe(2)
  })

  it('rolls every statement back when one fails', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 't2',
        type: 'transaction',
        statements: [{ sql: "INSERT INTO users (name, age) VALUES ('Alice', 30)" }, { sql: 'INVALID SQL GARBAGE' }],
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('QUERY_ERROR')
    expect(await countUsers()).toBe(0)
  })

  it('rejects a missing statements field', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 't3', type: 'transaction' }))
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('INVALID_MESSAGE')
  })

  it('rejects an empty statements array', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 't4', type: 'transaction', statements: [] }))
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('INVALID_MESSAGE')
  })

  it('rejects a statement without a sql field and names its index', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 't5',
        type: 'transaction',
        statements: [{ sql: 'SELECT 1' }, { params: [1] }],
      }),
    )
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { message: string }).message).toContain('index 1')
    expect(await countUsers()).toBe(0)
  })

  it('rejects an invalid writeConcern', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 't6',
        type: 'transaction',
        statements: [{ sql: "INSERT INTO users (name, age) VALUES ('Alice', 30)" }],
        writeConcern: { level: 'everyone' },
      }),
    )
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('INVALID_MESSAGE')
    expect(await countUsers()).toBe(0)
  })

  it('rejects a message above the configured payload limit', async () => {
    const smallHandler = createWSHandler(sirannon, { maxPayloadLength: 128 })
    const smallConn = createMockConnection()
    await smallHandler.handleOpen(smallConn, 'mydb')

    smallHandler.handleMessage(
      smallConn,
      JSON.stringify({
        id: 't7',
        type: 'transaction',
        statements: [{ sql: 'INSERT INTO users (name, age) VALUES (?, ?)', params: ['x'.repeat(256), 1] }],
      }),
    )
    await flushAsync(hasMessageCount(smallConn, 1))

    const msg = lastMessage(smallConn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('PAYLOAD_TOO_LARGE')
    expect(await countUsers()).toBe(0)
    await smallHandler.close()
  })
})
