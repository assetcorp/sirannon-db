import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { OperationRegistry } from '../../../core/operation-registry.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, lastMessage } from '../helpers.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
  capabilities: string[]
  error: { code: string; message: string }
}

const SQL_ROUTES = ['query', 'execute', 'transaction', 'batch', 'load']

const operations: OperationRegistry = {
  test: {
    reads: {
      allUsers: { statement: () => ({ sql: 'SELECT * FROM users ORDER BY id' }) },
    },
  },
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

const driver = betterSqlite3()

async function start(acceptSql?: boolean): Promise<void> {
  server = createServer(sirannon, { port: 0, operations, ...(acceptSql === undefined ? {} : { acceptSql }) })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

function postSql(route: string): Promise<Response> {
  return fetch(`${baseUrl}/db/test/${route}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: 'SELECT 1 AS val', params: [], paramsBatch: [[]], statements: [{ sql: 'SELECT 1' }] }),
  })
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sql-gate-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.execute("INSERT INTO users (name) VALUES ('Alice')")
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('SQL over the wire is off by default', () => {
  it('refuses every SQL route with 403 SQL_NOT_ACCEPTED', async () => {
    await start()
    for (const route of SQL_ROUTES) {
      const res = await postSql(route)
      expect(res.status, route).toBe(403)
      expect(((await res.json()) as ApiResponse).error.code, route).toBe('SQL_NOT_ACCEPTED')
    }
  })

  it('keeps registered operations reachable', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/query/allUsers`, { method: 'POST' })
    expect(res.status).toBe(200)
    expect(((await res.json()) as ApiResponse).rows).toHaveLength(1)
  })

  it('omits the query.sql capability', async () => {
    await start()
    const body = (await (await fetch(`${baseUrl}/capabilities`)).json()) as ApiResponse
    expect(body.capabilities).not.toContain('query.sql')
    expect(body.capabilities).toContain('query.named')
  })

  it('answers an unknown route with NOT_FOUND rather than a SQL refusal', async () => {
    await start()
    const res = await fetch(`${baseUrl}/db/test/nonsense`, { method: 'POST' })
    expect(res.status).toBe(404)
    expect(((await res.json()) as ApiResponse).error.code).toBe('NOT_FOUND')
  })
})

describe('SQL over the wire when accepted', () => {
  it('serves the SQL routes and advertises the capability', async () => {
    await start(true)
    const res = await postSql('query')
    expect(res.status).toBe(200)
    expect(((await res.json()) as ApiResponse).rows[0].val).toBe(1)

    const body = (await (await fetch(`${baseUrl}/capabilities`)).json()) as ApiResponse
    expect(body.capabilities).toContain('query.sql')
  })
})

describe('WebSocket SQL messages', () => {
  it('refuses each SQL message type by default', async () => {
    await start()
    const handler = createWSHandler(sirannon)
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'test')

    for (const type of SQL_ROUTES) {
      conn.messages.length = 0
      handler.handleMessage(conn, JSON.stringify({ id: 'r1', type, sql: 'SELECT 1' }))
      const reply = lastMessage(conn) as { type: string; error: { code: string } }
      expect(reply.type, type).toBe('error')
      expect(reply.error.code, type).toBe('SQL_NOT_ACCEPTED')
    }

    await handler.close()
  })

  it('serves them when SQL is accepted', async () => {
    await start()
    const handler = createWSHandler(sirannon, { acceptSql: true })
    const conn = createMockConnection()
    await handler.handleOpen(conn, 'test')

    handler.handleMessage(conn, JSON.stringify({ id: 'r1', type: 'query', sql: 'SELECT 1 AS val' }))
    await new Promise(resolve => setTimeout(resolve, 50))

    const reply = lastMessage(conn) as { type: string }
    expect(reply.type).toBe('result')

    await handler.close()
  })
})
