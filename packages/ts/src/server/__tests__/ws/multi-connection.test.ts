import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage, parseMessages } from '../helpers.js'

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
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler', () => {
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

      await flushAsync(() => conn1.messages.length >= 1 && conn2.messages.length >= 1)

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

      await flushAsync(() => conn1.messages.length >= 1 && conn2.messages.length >= 1)

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await flushAsync(
        () =>
          parseMessages(conn1).some(m => m.type === 'change') && parseMessages(conn2).some(m => m.type === 'change'),
      )

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

      await flushAsync(() => conn1.messages.length >= 1 && conn2.messages.length >= 1)

      handler.handleClose(conn1)

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await flushAsync(() => parseMessages(conn2).some(m => m.type === 'change'))

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

      await flushAsync(() => conn1.messages.length >= 1 && conn2.messages.length >= 1)

      const data1 = lastMessage(conn1).data as Record<string, unknown>
      const data2 = lastMessage(conn2).data as Record<string, unknown>
      const rows1 = data1.rows as Record<string, unknown>[]
      const rows2 = data2.rows as Record<string, unknown>[]
      expect(rows1[0].name).toBe('from-db1')
      expect(rows2[0].name).toBe('from-db2')
      await handler.close()
    })
  })
})
