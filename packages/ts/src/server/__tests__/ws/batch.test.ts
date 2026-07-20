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
  db = await sirannon.open('mydb', join(tempDir, 'batch.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
})

afterEach(async () => {
  await handler.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler handleMessage - batch', () => {
  it('applies one statement over many parameter sets in one transaction', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'b1',
        type: 'batch',
        sql: 'INSERT INTO users (name, age) VALUES (?, ?)',
        paramsBatch: [
          ['Alice', 30],
          ['Bob', 25],
          ['Carol', 28],
        ],
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.id).toBe('b1')
    expect(msg.type).toBe('result')
    const data = msg.data as { results: { changes: number; lastInsertRowId: number }[] }
    expect(data.results).toHaveLength(3)
    expect(data.results[2].lastInsertRowId).toBe(3)
    expect(await countUsers()).toBe(3)
  })

  it('supports named parameter sets', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'b2',
        type: 'batch',
        sql: 'INSERT INTO users (name, age) VALUES (:name, :age)',
        paramsBatch: [
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 },
        ],
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))

    expect(lastMessage(conn).type).toBe('result')
    expect(await countUsers()).toBe(2)
  })

  it('rolls the whole batch back when one parameter set fails', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'b3',
        type: 'batch',
        sql: 'INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
        paramsBatch: [
          [1, 'Alice', 30],
          [2, 'Bob', 25],
          [1, 'Duplicate', 40],
        ],
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('QUERY_ERROR')
    expect(await countUsers()).toBe(0)
  })

  it('rejects a missing sql field', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 'b4', type: 'batch', paramsBatch: [[1]] }))
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('INVALID_MESSAGE')
  })

  it('rejects a missing paramsBatch field', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 'b5', type: 'batch', sql: 'SELECT 1' }))
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { message: string }).message).toContain('paramsBatch')
  })

  it('rejects an empty paramsBatch array', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 'b6', type: 'batch', sql: 'SELECT 1', paramsBatch: [] }))
    await flushAsync(hasMessageCount(conn, 1))

    expect(lastMessage(conn).type).toBe('error')
  })

  it('rejects a parameter set that is not an object or array', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 'b7', type: 'batch', sql: 'SELECT ?', paramsBatch: [['a'], 'b'] }))
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { message: string }).message).toContain('index 1')
  })

  it('rejects an invalid writeConcern', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'b8',
        type: 'batch',
        sql: 'INSERT INTO users (name) VALUES (?)',
        paramsBatch: [['Alice']],
        writeConcern: { level: 'everyone' },
      }),
    )
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('INVALID_MESSAGE')
    expect(await countUsers()).toBe(0)
  })
})
