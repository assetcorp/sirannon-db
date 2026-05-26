import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
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
      throw new Error(`flushAsync timed out after ${timeout}ms: condition never became true`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

function hasMessageCount(conn: MockWSConnection, count: number): () => boolean {
  return () => conn.messages.length >= count
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler', () => {
  describe('handleMessage - query', () => {
    it('executes a query and returns rows in data.rows', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'query.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.execute("INSERT INTO users (name) VALUES ('Alice')")

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'req1',
          type: 'query',
          sql: 'SELECT * FROM users',
        }),
      )

      await flushAsync(hasMessageCount(conn, 1))

      const msg = lastMessage(conn)
      expect(msg.id).toBe('req1')
      expect(msg.type).toBe('result')
      const data = msg.data as Record<string, unknown>
      expect(data.rows).toEqual([{ id: 1, name: 'Alice' }])
      await handler.close()
    })

    it('supports named parameters', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'named.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT * FROM users WHERE name = :name',
          params: { name: 'Alice' },
        }),
      )

      await flushAsync(hasMessageCount(conn, 1))

      const data = lastMessage(conn).data as Record<string, unknown>
      const rows = data.rows as Record<string, unknown>[]
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      await handler.close()
    })

    it('supports positional parameters', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'pos.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT * FROM users WHERE age > ?',
          params: [26],
        }),
      )

      await flushAsync(hasMessageCount(conn, 1))

      const data = lastMessage(conn).data as Record<string, unknown>
      const rows = data.rows as Record<string, unknown>[]
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      await handler.close()
    })

    it('returns error for invalid SQL', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'bad.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SLECT * FORM users',
        }),
      )

      await flushAsync(hasMessageCount(conn, 1))

      const msg = lastMessage(conn)
      expect(msg.id).toBe('r1')
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('QUERY_ERROR')
      await handler.close()
    })

    it('returns error when sql field is missing', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'nosql.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error for non-object params', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'badparams.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT 1',
          params: 'not-valid',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns INTERNAL_ERROR for non-Sirannon query failures', async () => {
      const fakeSirannon = {
        resolve: async () =>
          ({
            id: 'mydb',
            path: '/tmp/test.db',
            readOnly: false,
            closed: false,
            query: () => {
              throw new Error('unexpected failure')
            },
          }) as unknown as Database,
      } as unknown as Sirannon
      const handler = createWSHandler(fakeSirannon)
      const conn = createMockConnection()

      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT 1',
        }),
      )

      await flushAsync(hasMessageCount(conn, 1))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INTERNAL_ERROR')
      expect((msg.error as Record<string, unknown>).message).toBe('An unexpected error occurred')
      await handler.close()
    })
  })
})
