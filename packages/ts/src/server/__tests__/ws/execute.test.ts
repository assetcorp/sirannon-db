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

      await flushAsync(hasMessageCount(conn, 1))

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

      await flushAsync(hasMessageCount(conn, 1))

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
})
