import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Database } from '../../core/database.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { createWSHandler, WSHandler } from '../ws-handler.js'
import { createMockConnection, lastMessage, parseMessages } from './helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

async function flushAsync(): Promise<void> {
  await new Promise(r => setTimeout(r, 10))
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
  describe('createWSHandler', () => {
    it('returns a WSHandler instance', async () => {
      const handler = createWSHandler(sirannon)
      expect(handler).toBeInstanceOf(WSHandler)
      await handler.close()
    })
  })

  describe('handleOpen', () => {
    it('registers a connection for a valid database', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'test.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      expect(handler.connectionCount).toBe(1)
      expect(conn.closed).toBe(false)
      await handler.close()
    })

    it('rejects connection for unknown database', async () => {
      const handler = createWSHandler(sirannon)
      const conn = createMockConnection()
      await handler.handleOpen(conn, 'nonexistent')

      expect(conn.closed).toBe(true)
      expect(conn.closeCode).toBe(1008)

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('DATABASE_NOT_FOUND')
      await handler.close()
    })

    it('rejects connection for closed database', async () => {
      const handler = createWSHandler(sirannon)
      const dbPath = join(tempDir, 'closed.db')
      await sirannon.open('mydb', dbPath)
      await sirannon.close('mydb')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      expect(conn.closed).toBe(true)
      await handler.close()
    })

    it('rejects connection when handler is shut down', async () => {
      const handler = createWSHandler(sirannon)
      await handler.close()

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      expect(conn.closed).toBe(true)
      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('HANDLER_CLOSED')
    })

    it('rejects connection when sirannon returns a closed database object', async () => {
      const fakeSirannon = {
        resolve: async () =>
          ({
            id: 'closed-db',
            path: '/tmp/closed.db',
            readOnly: false,
            closed: true,
          }) as Database,
      } as unknown as Sirannon
      const handler = createWSHandler(fakeSirannon)
      const conn = createMockConnection()

      await handler.handleOpen(conn, 'closed-db')

      expect(conn.closed).toBe(true)
      expect(conn.closeCode).toBe(1008)
      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('DATABASE_CLOSED')
      await handler.close()
    })
  })

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

      await flushAsync()

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

      await flushAsync()

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

      await flushAsync()

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

      await flushAsync()

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

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INTERNAL_ERROR')
      expect((msg.error as Record<string, unknown>).message).toBe('An unexpected error occurred')
      await handler.close()
    })
  })

  describe('handleMessage - execute', () => {
    it('executes a mutation and returns changes', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'exec.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'execute',
          sql: "INSERT INTO users (name) VALUES ('Alice')",
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.id).toBe('r1')
      expect(msg.type).toBe('result')
      const data = msg.data as Record<string, unknown>
      expect(data.changes).toBe(1)
      expect(data.lastInsertRowId).toBeDefined()
      await handler.close()
    })

    it('returns error for invalid execute SQL', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'badexec.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'execute',
          sql: 'INSERT INTO nonexistent VALUES (1)',
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      await handler.close()
    })

    it('returns error when sql field is missing', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'noexecsql.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'execute' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error for non-object execute params', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'badexecparams.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'execute',
          sql: 'SELECT 1',
          params: 'invalid',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })
  })

  describe('handleMessage - subscribe', () => {
    it('subscribes to a table and returns subscribed confirmation', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'sub.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.id).toBe('sub-1')
      expect(msg.type).toBe('subscribed')
      await handler.close()
    })

    it('receives change events after subscribing', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'change.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const messages = parseMessages(conn)
      const changeMsg = messages.find(m => m.type === 'change') as Record<string, unknown>

      expect(changeMsg).toBeDefined()
      expect(changeMsg.id).toBe('sub-1')
      const event = changeMsg.event as Record<string, unknown>
      expect(event.type).toBe('insert')
      expect(event.table).toBe('users')
      expect((event.row as Record<string, unknown>).name).toBe('Alice')
      expect(typeof event.seq).toBe('string')
      expect(typeof event.timestamp).toBe('number')
      await handler.close()
    })

    it('receives filtered change events', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'filter.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
          filter: { name: 'Alice' },
        }),
      )

      await flushAsync()

      await db.execute("INSERT INTO users (name) VALUES ('Bob')")
      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      const event = changeMessages[0].event as Record<string, unknown>
      expect((event.row as Record<string, unknown>).name).toBe('Alice')
      await handler.close()
    })

    it('rejects duplicate subscription IDs on the same connection', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'dup.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('DUPLICATE_SUBSCRIPTION')
      await handler.close()
    })

    it('returns error when subscribing on a read-only database', async () => {
      const handler = createWSHandler(sirannon)
      const dbPath = join(tempDir, 'ro.db')
      const setup = await Database.create('setup', dbPath, driver)
      await setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await setup.close()

      await sirannon.open('mydb', dbPath, { readOnly: true })

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('READ_ONLY')
      await handler.close()
    })

    it('returns error for nonexistent table', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'notable.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'nonexistent',
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('CDC_ERROR')
      await handler.close()
    })

    it('returns error when table field is missing', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'notab.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error for invalid filter type', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'badfilt.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
          filter: [1, 2, 3],
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error when subscribing to in-memory database', async () => {
      const fakeSirannon = {
        resolve: async () =>
          ({
            id: 'memorydb',
            path: ':memory:',
            readOnly: false,
            closed: false,
          }) as unknown as Database,
      } as unknown as Sirannon
      const handler = createWSHandler(fakeSirannon)

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'memorydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('CDC_UNSUPPORTED')
      await handler.close()
    })
  })

  describe('handleMessage - unsubscribe', () => {
    it('unsubscribes and sends confirmation', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'unsub.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.id).toBe('sub-1')
      expect(msg.type).toBe('unsubscribed')
      await handler.close()
    })

    it('stops receiving change events after unsubscribe', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'unsubstop.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(0)
      await handler.close()
    })

    it('returns error for unknown subscription ID', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'badunsub.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-99',
          type: 'unsubscribe',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('SUBSCRIPTION_NOT_FOUND')
      await handler.close()
    })
  })

  describe('handleMessage - validation', () => {
    it('returns error for invalid JSON', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'json.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, 'not json at all{{{')

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_JSON')
      await handler.close()
    })

    it('returns error for non-object JSON', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'arr.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, '["array"]')

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error for missing type field', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'notype.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', sql: 'SELECT 1' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error for missing id field', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'noid.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ type: 'query', sql: 'SELECT 1' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      await handler.close()
    })

    it('returns error for unknown message type', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'unknown.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'magic' }))

      const msg = lastMessage(conn)
      expect(msg.id).toBe('r1')
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('UNKNOWN_TYPE')
      await handler.close()
    })

    it('returns error for oversized messages', async () => {
      const handler = createWSHandler(sirannon, {
        maxPayloadLength: 100,
      })
      await sirannon.open('mydb', join(tempDir, 'big.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, 'x'.repeat(200))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('PAYLOAD_TOO_LARGE')
      await handler.close()
    })

    it('ignores messages from unregistered connections', async () => {
      const handler = createWSHandler(sirannon)
      const conn = createMockConnection()

      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT 1' }))

      expect(conn.messages).toHaveLength(0)
      await handler.close()
    })
  })

  describe('handleClose', () => {
    it('removes the connection from the handler', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'close.db'))

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      expect(handler.connectionCount).toBe(1)

      handler.handleClose(conn)
      expect(handler.connectionCount).toBe(0)
      await handler.close()
    })

    it('cleans up subscriptions on close', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'cleanup.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      handler.handleClose(conn)

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(0)
      await handler.close()
    })

    it('handles close for unregistered connection gracefully', async () => {
      const handler = createWSHandler(sirannon)
      const conn = createMockConnection()
      expect(() => handler.handleClose(conn)).not.toThrow()
      await handler.close()
    })
  })

  describe('handler shutdown', () => {
    it('closes all connections on shutdown', async () => {
      const handler = createWSHandler(sirannon)
      await sirannon.open('mydb', join(tempDir, 'shutdown.db'))

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      await handler.handleOpen(conn1, 'mydb')
      await handler.handleOpen(conn2, 'mydb')

      expect(handler.connectionCount).toBe(2)

      await handler.close()

      expect(handler.connectionCount).toBe(0)
      expect(conn1.closed).toBe(true)
      expect(conn1.closeCode).toBe(1001)
      expect(conn2.closed).toBe(true)
      expect(conn2.closeCode).toBe(1001)
    })

    it('shutdown is idempotent', async () => {
      const handler = createWSHandler(sirannon)
      await handler.close()
      await expect(handler.close()).resolves.toBeUndefined()
    })
  })

  describe('multiple connections to the same database', () => {
    it('both connections can query independently', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'multi.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.execute("INSERT INTO users (name) VALUES ('Alice')")

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      await handler.handleOpen(conn1, 'mydb')
      await handler.handleOpen(conn2, 'mydb')

      handler.handleMessage(
        conn1,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT * FROM users',
        }),
      )
      handler.handleMessage(
        conn2,
        JSON.stringify({
          id: 'r2',
          type: 'query',
          sql: 'SELECT * FROM users',
        }),
      )

      await flushAsync()

      const data1 = lastMessage(conn1).data as Record<string, unknown>
      const data2 = lastMessage(conn2).data as Record<string, unknown>
      expect((data1.rows as unknown[]).length).toBe(1)
      expect((data2.rows as unknown[]).length).toBe(1)
      await handler.close()
    })

    it('both connections receive change events for the same table', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'multisub.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      await handler.handleOpen(conn1, 'mydb')
      await handler.handleOpen(conn2, 'mydb')

      handler.handleMessage(
        conn1,
        JSON.stringify({
          id: 'sub-a',
          type: 'subscribe',
          table: 'users',
        }),
      )
      handler.handleMessage(
        conn2,
        JSON.stringify({
          id: 'sub-b',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const changes1 = parseMessages(conn1).filter(m => m.type === 'change')
      const changes2 = parseMessages(conn2).filter(m => m.type === 'change')

      expect(changes1).toHaveLength(1)
      expect(changes1[0].id).toBe('sub-a')
      expect(changes2).toHaveLength(1)
      expect(changes2[0].id).toBe('sub-b')
      await handler.close()
    })

    it('closing one connection does not affect the other', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'multiclose.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      await handler.handleOpen(conn1, 'mydb')
      await handler.handleOpen(conn2, 'mydb')

      handler.handleMessage(
        conn1,
        JSON.stringify({
          id: 'sub-a',
          type: 'subscribe',
          table: 'users',
        }),
      )
      handler.handleMessage(
        conn2,
        JSON.stringify({
          id: 'sub-b',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      handler.handleClose(conn1)

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const changes1 = parseMessages(conn1).filter(m => m.type === 'change')
      const changes2 = parseMessages(conn2).filter(m => m.type === 'change')

      expect(changes1).toHaveLength(0)
      expect(changes2).toHaveLength(1)
      await handler.close()
    })
  })

  describe('connections to different databases', () => {
    it('queries from different databases are isolated', async () => {
      const handler = createWSHandler(sirannon)
      const db1 = await sirannon.open('db1', join(tempDir, 'db1.db'))
      const db2 = await sirannon.open('db2', join(tempDir, 'db2.db'))

      await db1.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      await db1.execute("INSERT INTO items (name) VALUES ('from-db1')")
      await db2.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      await db2.execute("INSERT INTO items (name) VALUES ('from-db2')")

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      await handler.handleOpen(conn1, 'db1')
      await handler.handleOpen(conn2, 'db2')

      handler.handleMessage(
        conn1,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT name FROM items',
        }),
      )
      handler.handleMessage(
        conn2,
        JSON.stringify({
          id: 'r2',
          type: 'query',
          sql: 'SELECT name FROM items',
        }),
      )

      await flushAsync()

      const data1 = lastMessage(conn1).data as Record<string, unknown>
      const data2 = lastMessage(conn2).data as Record<string, unknown>
      const rows1 = data1.rows as Record<string, unknown>[]
      const rows2 = data2.rows as Record<string, unknown>[]
      expect(rows1[0].name).toBe('from-db1')
      expect(rows2[0].name).toBe('from-db2')
      await handler.close()
    })
  })

  describe('CDC lifecycle', () => {
    it('handles subscribe -> data changes -> unsubscribe -> more changes correctly', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'lifecycle.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      await db.execute("INSERT INTO users (name) VALUES ('Bob')")
      await new Promise(r => setTimeout(r, 200))

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      const event = changeMessages[0].event as Record<string, unknown>
      expect((event.row as Record<string, unknown>).name).toBe('Alice')
      await handler.close()
    })

    it('captures update and delete events', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'upddel.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
      await db.execute("DELETE FROM users WHERE name = 'Alice'")
      await new Promise(r => setTimeout(r, 200))

      const events = parseMessages(conn)
        .filter(m => m.type === 'change')
        .map(m => m.event as Record<string, unknown>)

      expect(events).toHaveLength(3)
      expect(events[0].type).toBe('insert')
      expect(events[1].type).toBe('update')
      expect((events[1].oldRow as Record<string, unknown>).age).toBe(30)
      expect((events[1].row as Record<string, unknown>).age).toBe(31)
      expect(events[2].type).toBe('delete')
      await handler.close()
    })

    it('resubscribing after unsubscribe works', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'resub.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      const subMessages = parseMessages(conn).filter(m => m.type === 'subscribed')
      expect(subMessages).toHaveLength(2)

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await new Promise(r => setTimeout(r, 200))

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      await handler.close()
    })

    it('stops CDC polling after repeated polling errors', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'poll-errors.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      await handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync()

      const contexts = (
        handler as unknown as {
          cdcContexts: Map<string, { tracker: { poll: () => unknown[] } }>
        }
      ).cdcContexts
      const ctx = contexts.get('mydb')
      expect(ctx).toBeDefined()

      let pollCalls = 0
      if (ctx) {
        ctx.tracker.poll = () => {
          pollCalls++
          throw new Error('poll failed')
        }
      }

      await new Promise(r => setTimeout(r, 2_000))
      expect(pollCalls).toBeGreaterThanOrEqual(10)
      await handler.close()
    })

    it('supports timer handles without unref in CDC polling setup', async () => {
      const setIntervalSpy = vi.spyOn(globalThis, 'setInterval').mockImplementation((fn: TimerHandler) => {
        if (typeof fn === 'function') {
          fn()
        }
        return 1 as unknown as ReturnType<typeof setInterval>
      })
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval').mockImplementation(() => {})

      try {
        const handler = createWSHandler(sirannon)
        const db = await sirannon.open('mydb', join(tempDir, 'no-unref.db'))
        await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

        const conn = createMockConnection()
        await handler.handleOpen(conn, 'mydb')
        handler.handleMessage(
          conn,
          JSON.stringify({
            id: 'sub-1',
            type: 'subscribe',
            table: 'users',
          }),
        )

        await flushAsync()

        await handler.close()
        expect(setIntervalSpy).toHaveBeenCalled()
        expect(clearIntervalSpy).toHaveBeenCalled()
      } finally {
        setIntervalSpy.mockRestore()
        clearIntervalSpy.mockRestore()
      }
    })
  })
})
