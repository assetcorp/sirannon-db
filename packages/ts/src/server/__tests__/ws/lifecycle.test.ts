import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, WSHandler } from '../../ws-handler.js'
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

      await flushAsync(hasMessageCount(conn, 1))

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
})
