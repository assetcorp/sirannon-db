import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler', () => {
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
})
