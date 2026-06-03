import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage, type MockWSConnection, parseMessages } from '../helpers.js'

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

      await flushAsync(hasMessageCount(conn, 1))

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

      await flushAsync(hasMessageCount(conn, 1))

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

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

    it('does not replay changes that happened before a WebSocket subscription starts', async () => {
      const handler = createWSHandler(sirannon)
      const db = await sirannon.open('mydb', join(tempDir, 'live-only.db'))
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.watch('users')
      await db.execute("INSERT INTO users (name) VALUES ('Historical')")

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

      await flushAsync(hasMessageCount(conn, 1))
      await new Promise(resolve => setTimeout(resolve, 150))

      const preLiveChanges = parseMessages(conn).filter(m => m.type === 'change')
      expect(preLiveChanges).toHaveLength(0)

      await db.execute("INSERT INTO users (name) VALUES ('Live')")
      await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

      const changeMessages = parseMessages(conn).filter(m => m.type === 'change')
      expect(changeMessages).toHaveLength(1)
      const event = changeMessages[0].event as Record<string, unknown>
      expect((event.row as Record<string, unknown>).name).toBe('Live')
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

      await flushAsync(hasMessageCount(conn, 1))

      await db.execute("INSERT INTO users (name) VALUES ('Bob')")
      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

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

      await flushAsync(hasMessageCount(conn, 1))

      handler.handleMessage(
        conn,
        JSON.stringify({
          id: 'sub-1',
          type: 'subscribe',
          table: 'users',
        }),
      )

      await flushAsync(hasMessageCount(conn, 2))

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

      await flushAsync(hasMessageCount(conn, 1))

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

      await flushAsync(hasMessageCount(conn, 1))

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

      await flushAsync(hasMessageCount(conn, 1))

      const msg = lastMessage(conn)
      expect(msg.type).toBe('error')
      expect((msg.error as Record<string, unknown>).code).toBe('CDC_UNSUPPORTED')
      await handler.close()
    })
  })
})
