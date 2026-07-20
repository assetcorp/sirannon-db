import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage, type MockWSConnection } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

async function flushAsync(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`flushAsync timed out after ${timeout}ms`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

function hasMessageCount(conn: MockWSConnection, count: number): () => boolean {
  return () => conn.messages.length >= count
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-guard-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler internal-table guard', () => {
  it('rejects a query against the internal change log', async () => {
    const handler = createWSHandler(sirannon)
    await sirannon.open('mydb', join(tempDir, 'guard.db'))

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT * FROM _sirannon_changes' }))

    await flushAsync(hasMessageCount(conn, 1))
    const msg = lastMessage(conn)
    expect(msg.type).toBe('error')
    expect((msg.error as Record<string, unknown>).code).toBe('FORBIDDEN_SQL')
    await handler.close()
  })

  it('rejects a write to an internal table', async () => {
    const handler = createWSHandler(sirannon)
    await sirannon.open('mydb', join(tempDir, 'guard-write.db'))

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(
      conn,
      JSON.stringify({ id: 'r1', type: 'execute', sql: 'INSERT INTO _sirannon_changes (seq) VALUES (1)' }),
    )

    await flushAsync(hasMessageCount(conn, 1))
    expect((lastMessage(conn).error as Record<string, unknown>).code).toBe('FORBIDDEN_SQL')
    await handler.close()
  })

  it('rejects subscribing to an internal table', async () => {
    const handler = createWSHandler(sirannon)
    await sirannon.open('mydb', join(tempDir, 'guard-sub.db'))

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 's1', type: 'subscribe', table: '_sirannon_changes' }))

    await flushAsync(hasMessageCount(conn, 1))
    expect((lastMessage(conn).error as Record<string, unknown>).code).toBe('FORBIDDEN_SQL')
    await handler.close()
  })

  it('still serves ordinary user queries', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'guard-ok.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT * FROM users' }))

    await flushAsync(hasMessageCount(conn, 1))
    const msg = lastMessage(conn)
    expect(msg.type).toBe('result')
    expect((msg.data as Record<string, unknown>).rows).toEqual([{ id: 1, name: 'Alice' }])
    await handler.close()
  })

  it('allows reading the sqlite_ catalogue', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'guard-catalog.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'r1',
        type: 'query',
        sql: "SELECT name FROM sqlite_master WHERE type='table' AND name='users'",
      }),
    )

    await flushAsync(hasMessageCount(conn, 1))
    const msg = lastMessage(conn)
    expect(msg.type).toBe('result')
    expect((msg.data as Record<string, unknown>).rows).toEqual([{ name: 'users' }])
    await handler.close()
  })

  it('rejects PRAGMA writable_schema', async () => {
    const handler = createWSHandler(sirannon)
    await sirannon.open('mydb', join(tempDir, 'guard-writable.db'))

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'execute', sql: 'PRAGMA writable_schema = ON' }))

    await flushAsync(hasMessageCount(conn, 1))
    expect((lastMessage(conn).error as Record<string, unknown>).code).toBe('FORBIDDEN_SQL')
    await handler.close()
  })
})
