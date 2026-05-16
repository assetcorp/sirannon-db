import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages } from '../helpers.js'

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

      await flushAsync(hasMessageCount(conn, 1))

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

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

      await flushAsync(hasMessageCount(conn, 1))

      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
      await db.execute("DELETE FROM users WHERE name = 'Alice'")
      await flushAsync(() => parseMessages(conn).filter(m => m.type === 'change').length >= 3)

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

      await flushAsync(hasMessageCount(conn, 1))

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

      await flushAsync(() => parseMessages(conn).filter(m => m.type === 'subscribed').length >= 2)

      const subMessages = parseMessages(conn).filter(m => m.type === 'subscribed')
      expect(subMessages).toHaveLength(2)

      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

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

      await flushAsync(hasMessageCount(conn, 1))

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

      await flushAsync(() => pollCalls >= 10, 5000)
      expect(pollCalls).toBeGreaterThanOrEqual(10)
      await handler.close()
    })

    it('supports timer handles without unref in CDC polling setup', async () => {
      const setIntervalSpy = vi.spyOn(globalThis, 'setInterval').mockImplementation(((fn: unknown) => {
        if (typeof fn === 'function') {
          fn()
        }
        return 1 as unknown as ReturnType<typeof setInterval>
      }) as typeof globalThis.setInterval)
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

        await flushAsync(hasMessageCount(conn, 1))

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
