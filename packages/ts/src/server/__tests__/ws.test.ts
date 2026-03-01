import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../core/database.js'
import { Sirannon } from '../../core/sirannon.js'
import { createWSHandler, type WSConnection, WSHandler } from '../ws-handler.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tempDir: string
let sirannon: Sirannon

interface MockWSConnection extends WSConnection {
  messages: string[]
  closed: boolean
  closeCode?: number
  closeReason?: string
}

function createMockConnection(): MockWSConnection {
  const conn: MockWSConnection = {
    messages: [],
    closed: false,
    closeCode: undefined,
    closeReason: undefined,
    send(data: string) {
      conn.messages.push(data)
    },
    close(code?: number, reason?: string) {
      conn.closed = true
      conn.closeCode = code
      conn.closeReason = reason
    },
  }
  return conn
}

function parseMessages(conn: MockWSConnection): Record<string, unknown>[] {
  return conn.messages.map(m => JSON.parse(m))
}

function lastMessage(conn: MockWSConnection): Record<string, unknown> {
  return JSON.parse(conn.messages[conn.messages.length - 1])
}

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-'))
  sirannon = new Sirannon()
})

afterEach(() => {
  sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('WSHandler', () => {
  describe('createWSHandler', () => {
    it('returns a WSHandler instance', () => {
      const handler = createWSHandler(sirannon)
      expect(handler).toBeInstanceOf(WSHandler)
      handler.close()
    })
  })

  describe('handleOpen', () => {
    it('registers a connection for a valid database', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'test.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      expect(handler.connectionCount).toBe(1)
      expect(conn.closed).toBe(false)
      handler.close()
    })

    it('rejects connection for unknown database', () => {
      const handler = createWSHandler(sirannon)
      const conn = createMockConnection()
      handler.handleOpen(conn, 'nonexistent')

      expect(conn.closed).toBe(true)
      expect(conn.closeCode).toBe(1008)

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('DATABASE_NOT_FOUND')
      handler.close()
    })

    it('rejects connection for closed database', () => {
      const handler = createWSHandler(sirannon)
      const dbPath = join(tempDir, 'closed.db')
      sirannon.open('mydb', dbPath)
      sirannon.close('mydb')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      expect(conn.closed).toBe(true)
      handler.close()
    })

    it('rejects connection when handler is shut down', () => {
      const handler = createWSHandler(sirannon)
      handler.close()

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      expect(conn.closed).toBe(true)
      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('HANDLER_CLOSED')
    })
  })

  describe('handleMessage - query', () => {
    it('executes a query and returns rows in data.rows', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'query.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.execute("INSERT INTO users (name) VALUES ('Alice')")

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'req1',
          type: 'query',
          sql: 'SELECT * FROM users',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.id).toBe('req1')
      expect(msg.type).toBe('result')
      const data = msg.data as Record<string, unknown>
      expect(data.rows).toEqual([{ id: 1, name: 'Alice' }])
      handler.close()
    })

    it('supports named parameters', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'named.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT * FROM users WHERE name = :name',
          params: { name: 'Alice' },
        }),
      )

      const data = lastMessage(conn).data as Record<string, unknown>
      const rows = data.rows as Record<string, unknown>[]
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      handler.close()
    })

    it('supports positional parameters', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'pos.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SELECT * FROM users WHERE age > ?',
          params: [26],
        }),
      )

      const data = lastMessage(conn).data as Record<string, unknown>
      const rows = data.rows as Record<string, unknown>[]
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      handler.close()
    })

    it('returns error for invalid SQL', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'bad.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'query',
          sql: 'SLECT * FORM users',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.id).toBe('r1')
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('QUERY_ERROR')
      handler.close()
    })

    it('returns error when sql field is missing', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'nosql.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      handler.close()
    })

    it('returns error for non-object params', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'badparams.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
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
      handler.close()
    })
  })

  describe('handleMessage - execute', () => {
    it('executes a mutation and returns changes', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'exec.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'execute',
          sql: "INSERT INTO users (name) VALUES ('Alice')",
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.id).toBe('r1')
      expect(msg.type).toBe('result')
      const data = msg.data as Record<string, unknown>
      expect(data.changes).toBe(1)
      expect(data.lastInsertRowId).toBeDefined()
      handler.close()
    })

    it('returns error for invalid execute SQL', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'badexec.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'r1',
          type: 'execute',
          sql: 'INSERT INTO nonexistent VALUES (1)',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      handler.close()
    })

    it('returns error when sql field is missing', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'noexecsql.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'execute' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      handler.close()
    })
  })

  describe('handleMessage - subscribe', () => {
    it('subscribes to a table and returns subscribed confirmation', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'sub.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.id).toBe('sub-1')
      expect(msg.type).toBe('subscribed')
      handler.close()
    })

    it('receives change events after subscribing', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'change.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

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
      handler.close()
    })

    it('receives filtered change events', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'filter.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
          filter: { name: 'Alice' },
        }),
      )

      db.execute("INSERT INTO users (name) VALUES ('Bob')")
      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      const event = changeMessages[0].event as Record<string, unknown>
      expect((event.row as Record<string, unknown>).name).toBe('Alice')
      handler.close()
    })

    it('rejects duplicate subscription IDs on the same connection', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'dup.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
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

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('DUPLICATE_SUBSCRIPTION')
      handler.close()
    })

    it('returns error when subscribing on a read-only database', () => {
      const handler = createWSHandler(sirannon)
      const dbPath = join(tempDir, 'ro.db')
      const setup = new Database('setup', dbPath)
      setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      setup.close()

      sirannon.open('mydb', dbPath, { readOnly: true })

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('READ_ONLY')
      handler.close()
    })

    it('returns error for nonexistent table', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'notable.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'nonexistent',
        }),
      )

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('CDC_ERROR')
      handler.close()
    })

    it('returns error when table field is missing', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'notab.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
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
      handler.close()
    })

    it('returns error for invalid filter type', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'badfilt.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
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
      handler.close()
    })
  })

  describe('handleMessage - unsubscribe', () => {
    it('unsubscribes and sends confirmation', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'unsub.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

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
      handler.close()
    })

    it('stops receiving change events after unsubscribe', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'unsubstop.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(0)
      handler.close()
    })

    it('returns error for unknown subscription ID', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'badunsub.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
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
      handler.close()
    })
  })

  describe('handleMessage - validation', () => {
    it('returns error for invalid JSON', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'json.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, 'not json at all{{{')

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_JSON')
      handler.close()
    })

    it('returns error for non-object JSON', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'arr.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, '["array"]')

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      handler.close()
    })

    it('returns error for missing type field', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'notype.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', sql: 'SELECT 1' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      handler.close()
    })

    it('returns error for missing id field', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'noid.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ type: 'query', sql: 'SELECT 1' }))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
      handler.close()
    })

    it('returns error for unknown message type', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'unknown.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'magic' }))

      const msg = lastMessage(conn)
      expect(msg.id).toBe('r1')
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('UNKNOWN_TYPE')
      handler.close()
    })

    it('returns error for oversized messages', () => {
      const handler = createWSHandler(sirannon, {
        maxPayloadLength: 100,
      })
      sirannon.open('mydb', join(tempDir, 'big.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(conn, 'x'.repeat(200))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('PAYLOAD_TOO_LARGE')
      handler.close()
    })

    it('ignores messages from unregistered connections', () => {
      const handler = createWSHandler(sirannon)
      const conn = createMockConnection()

      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT 1' }))

      expect(conn.messages).toHaveLength(0)
      handler.close()
    })
  })

  describe('handleClose', () => {
    it('removes the connection from the handler', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'close.db'))

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      expect(handler.connectionCount).toBe(1)

      handler.handleClose(conn)
      expect(handler.connectionCount).toBe(0)
      handler.close()
    })

    it('cleans up subscriptions on close', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'cleanup.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      handler.handleClose(conn)

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(0)
      handler.close()
    })

    it('handles close for unregistered connection gracefully', () => {
      const handler = createWSHandler(sirannon)
      const conn = createMockConnection()
      expect(() => handler.handleClose(conn)).not.toThrow()
      handler.close()
    })
  })

  describe('handler shutdown', () => {
    it('closes all connections on shutdown', () => {
      const handler = createWSHandler(sirannon)
      sirannon.open('mydb', join(tempDir, 'shutdown.db'))

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      handler.handleOpen(conn1, 'mydb')
      handler.handleOpen(conn2, 'mydb')

      expect(handler.connectionCount).toBe(2)

      handler.close()

      expect(handler.connectionCount).toBe(0)
      expect(conn1.closed).toBe(true)
      expect(conn1.closeCode).toBe(1001)
      expect(conn2.closed).toBe(true)
      expect(conn2.closeCode).toBe(1001)
    })

    it('shutdown is idempotent', () => {
      const handler = createWSHandler(sirannon)
      handler.close()
      expect(() => handler.close()).not.toThrow()
    })
  })

  describe('multiple connections to the same database', () => {
    it('both connections can query independently', () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'multi.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.execute("INSERT INTO users (name) VALUES ('Alice')")

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      handler.handleOpen(conn1, 'mydb')
      handler.handleOpen(conn2, 'mydb')

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

      const data1 = lastMessage(conn1).data as Record<string, unknown>
      const data2 = lastMessage(conn2).data as Record<string, unknown>
      expect((data1.rows as unknown[]).length).toBe(1)
      expect((data2.rows as unknown[]).length).toBe(1)
      handler.close()
    })

    it('both connections receive change events for the same table', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'multisub.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      handler.handleOpen(conn1, 'mydb')
      handler.handleOpen(conn2, 'mydb')

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

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      const changes1 = parseMessages(conn1).filter(m => m.type === 'change')
      const changes2 = parseMessages(conn2).filter(m => m.type === 'change')

      expect(changes1).toHaveLength(1)
      expect(changes1[0].id).toBe('sub-a')
      expect(changes2).toHaveLength(1)
      expect(changes2[0].id).toBe('sub-b')
      handler.close()
    })

    it('closing one connection does not affect the other', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'multiclose.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      handler.handleOpen(conn1, 'mydb')
      handler.handleOpen(conn2, 'mydb')

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

      handler.handleClose(conn1)

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      const changes1 = parseMessages(conn1).filter(m => m.type === 'change')
      const changes2 = parseMessages(conn2).filter(m => m.type === 'change')

      expect(changes1).toHaveLength(0)
      expect(changes2).toHaveLength(1)
      handler.close()
    })
  })

  describe('connections to different databases', () => {
    it('queries from different databases are isolated', () => {
      const handler = createWSHandler(sirannon)
      const db1 = sirannon.open('db1', join(tempDir, 'db1.db'))
      const db2 = sirannon.open('db2', join(tempDir, 'db2.db'))

      db1.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      db1.execute("INSERT INTO items (name) VALUES ('from-db1')")
      db2.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      db2.execute("INSERT INTO items (name) VALUES ('from-db2')")

      const conn1 = createMockConnection()
      const conn2 = createMockConnection()
      handler.handleOpen(conn1, 'db1')
      handler.handleOpen(conn2, 'db2')

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

      const data1 = lastMessage(conn1).data as Record<string, unknown>
      const data2 = lastMessage(conn2).data as Record<string, unknown>
      const rows1 = data1.rows as Record<string, unknown>[]
      const rows2 = data2.rows as Record<string, unknown>[]
      expect(rows1[0].name).toBe('from-db1')
      expect(rows2[0].name).toBe('from-db2')
      handler.close()
    })
  })

  describe('CDC lifecycle', () => {
    it('handles subscribe → data changes → unsubscribe → more changes correctly', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'lifecycle.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      db.execute("INSERT INTO users (name) VALUES ('Bob')")
      await wait(200)

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      const event = changeMessages[0].event as Record<string, unknown>
      expect((event.row as Record<string, unknown>).name).toBe('Alice')
      handler.close()
    })

    it('captures update and delete events', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'upddel.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
      db.execute("DELETE FROM users WHERE name = 'Alice'")
      await wait(200)

      const events = parseMessages(conn)
        .filter(m => m.type === 'change')
        .map(m => m.event as Record<string, unknown>)

      expect(events).toHaveLength(3)
      expect(events[0].type).toBe('insert')
      expect(events[1].type).toBe('update')
      expect((events[1].oldRow as Record<string, unknown>).age).toBe(30)
      expect((events[1].row as Record<string, unknown>).age).toBe(31)
      expect(events[2].type).toBe('delete')
      handler.close()
    })

    it('resubscribing after unsubscribe works', async () => {
      const handler = createWSHandler(sirannon)
      const db = sirannon.open('mydb', join(tempDir, 'resub.db'))
      db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const conn = createMockConnection()
      handler.handleOpen(conn, 'mydb')

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'unsubscribe',
        }),
      )

      // Resubscribe with the same ID (should work after unsubscribe)
      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      const subMessages = parseMessages(conn).filter(m => m.type === 'subscribed')
      expect(subMessages).toHaveLength(2)

      db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await wait(200)

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      handler.close()
    })
  })
})
