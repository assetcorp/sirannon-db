import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { OperationRegistry } from '../../../core/operation-registry.js'
import type { ServerExecutionTarget } from '../../../core/server-options.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { QueryOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage, type MockWSConnection } from '../helpers.js'

const driver = betterSqlite3()

const operations: OperationRegistry = {
  mydb: {
    reads: {
      everyUser: { statement: () => ({ sql: 'SELECT id, name FROM users ORDER BY id' }) },
    },
  },
}

let tempDir: string
let sirannon: Sirannon
let observed: (QueryOptions | undefined)[]

async function settle(conn: MockWSConnection, count: number, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (conn.messages.length < count) {
    if (Date.now() - start >= timeout) throw new Error(`Timed out waiting for ${count} messages`)
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

function recordingTarget(target: ServerExecutionTarget): ServerExecutionTarget {
  return {
    ...target,
    query: (sql, params, options) => {
      observed.push(options)
      return target.query(sql, params, options)
    },
    queryForWire: undefined,
    execute: (sql, params, options) => target.execute(sql, params, options),
    transaction: (fn, options) => target.transaction(fn, options),
  }
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-read-concern-'))
  sirannon = new Sirannon({ driver })
  observed = []
  const db = await sirannon.open('mydb', join(tempDir, 'read-concern.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.execute("INSERT INTO users (name) VALUES ('Alice')")
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('a read concern over WebSocket', () => {
  it('reaches the execution target for a statement and for a registered read', async () => {
    const handler = createWSHandler(sirannon, {
      acceptSql: true,
      operations,
      resolveExecutionTarget: id => {
        const database = sirannon.get(id)
        return database ? recordingTarget(database) : null
      },
    })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')

    handler.handleMessage(
      conn,
      JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT id FROM users', readConcern: { level: 'linearizable' } }),
    )
    await settle(conn, 1)

    handler.handleMessage(
      conn,
      JSON.stringify({ id: 'r2', type: 'query', name: 'everyUser', readConcern: { level: 'majority' } }),
    )
    await settle(conn, 2)

    expect(observed).toEqual([{ readConcern: { level: 'linearizable' } }, { readConcern: { level: 'majority' } }])
    expect((lastMessage(conn) as { type: string }).type).toBe('result')

    await handler.close()
  })

  it('omits the option when the message carries no read concern', async () => {
    const handler = createWSHandler(sirannon, {
      acceptSql: true,
      resolveExecutionTarget: id => {
        const database = sirannon.get(id)
        return database ? recordingTarget(database) : null
      },
    })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')

    handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT id FROM users' }))
    await settle(conn, 1)

    expect(observed).toEqual([undefined])
    await handler.close()
  })

  it('refuses an invalid read concern', async () => {
    const handler = createWSHandler(sirannon, { acceptSql: true, operations })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')

    for (const message of [
      { id: 'r1', type: 'query', sql: 'SELECT 1', readConcern: { level: 'strong' } },
      { id: 'r2', type: 'query', name: 'everyUser', readConcern: 'linearizable' },
    ]) {
      conn.messages.length = 0
      handler.handleMessage(conn, JSON.stringify(message))
      await settle(conn, 1)
      const reply = lastMessage(conn) as { type: string; error: { code: string } }
      expect(reply.type).toBe('error')
      expect(reply.error.code).toBe('INVALID_MESSAGE')
    }

    await handler.close()
  })
})
