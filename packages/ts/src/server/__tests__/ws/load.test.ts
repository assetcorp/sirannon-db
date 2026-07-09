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

async function countReadings(): Promise<number> {
  const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
  return rows[0].count
}

async function writerSynchronousLevel(): Promise<number> {
  return db.transaction(async tx => {
    const rows = await tx.query<{ synchronous: number }>('PRAGMA synchronous')
    return rows[0].synchronous
  })
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-'))
  sirannon = new Sirannon({ driver })
  handler = createWSHandler(sirannon)
  db = await sirannon.open('mydb', join(tempDir, 'load.db'), { synchronous: 'full' })
  await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
})

afterEach(async () => {
  await handler.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler handleMessage - load', () => {
  it('loads rows and returns an aggregate summary', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'l1',
        type: 'load',
        sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
        paramsBatch: Array.from({ length: 100 }, (_, i) => [i + 1, i * 0.5]),
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.id).toBe('l1')
    expect(msg.type).toBe('result')
    const data = msg.data as { rowsLoaded: number; changes: number }
    expect(data.rowsLoaded).toBe(100)
    expect(data.changes).toBe(100)
    expect(await countReadings()).toBe(100)
  })

  it('restores the configured durability after the load', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'l2',
        type: 'load',
        sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
        paramsBatch: [[1, 1.0]],
      }),
    )
    await flushAsync(hasMessageCount(conn, 1))

    const fullLevel = 2
    expect(await writerSynchronousLevel()).toBe(fullLevel)
  })

  it('rolls the whole load back on failure and restores durability', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'l3',
        type: 'load',
        sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
        paramsBatch: [
          [1, 1.0],
          [1, 2.0],
        ],
      }),
    )
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('QUERY_ERROR')
    expect(await countReadings()).toBe(0)
    const fullLevel = 2
    expect(await writerSynchronousLevel()).toBe(fullLevel)
  })

  it('rejects an invalid durability', async () => {
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'l4',
        type: 'load',
        sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
        paramsBatch: [[1, 1.0]],
        durability: 'sometimes',
      }),
    )
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { message: string }).message).toContain('durability')
    expect(await countReadings()).toBe(0)
  })

  it('rejects a missing paramsBatch field', async () => {
    handler.handleMessage(conn, JSON.stringify({ id: 'l5', type: 'load', sql: 'SELECT 1' }))
    await flushAsync(hasMessageCount(conn, 1))

    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as { message: string }).message).toContain('paramsBatch')
  })

  it('rejects a message above the configured payload limit', async () => {
    const smallHandler = createWSHandler(sirannon, { maxPayloadLength: 128 })
    const smallConn = createMockConnection()
    await smallHandler.handleOpen(smallConn, 'mydb')

    smallHandler.handleMessage(
      smallConn,
      JSON.stringify({
        id: 'l6',
        type: 'load',
        sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
        paramsBatch: Array.from({ length: 50 }, (_, i) => [i + 1, i]),
      }),
    )
    await flushAsync(hasMessageCount(smallConn, 1))

    const msg = lastMessage(smallConn)
    expect(msg.type).toBe('error')
    expect((msg.error as { code: string }).code).toBe('PAYLOAD_TOO_LARGE')
    expect(await countReadings()).toBe(0)
    await smallHandler.close()
  })
})
