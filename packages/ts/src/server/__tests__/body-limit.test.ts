import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import type { ServerOptions } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../server.js'

interface ApiResponse {
  changes: number
  error: { code: string; message: string }
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer | null

const driver = betterSqlite3()

async function startServer(options?: Omit<ServerOptions, 'port'>): Promise<string> {
  server = createServer(sirannon, { ...options, port: 0 })
  await server.listen()
  return `http://127.0.0.1:${server.listeningPort}`
}

function waitForOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('WebSocket open timed out')), 3000)
    ws.addEventListener(
      'open',
      () => {
        clearTimeout(timeout)
        resolve()
      },
      { once: true },
    )
    ws.addEventListener(
      'error',
      () => {
        clearTimeout(timeout)
        reject(new Error('WebSocket connect error'))
      },
      { once: true },
    )
  })
}

function waitForClose(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    if (ws.readyState === WebSocket.CLOSED) {
      resolve()
      return
    }
    const timeout = setTimeout(() => reject(new Error('WebSocket close timed out')), 3000)
    ws.addEventListener(
      'close',
      () => {
        clearTimeout(timeout)
        resolve()
      },
      { once: true },
    )
  })
}

function sendAndReceive(ws: WebSocket, msg: Record<string, unknown>): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for WS response')), 3000)
    const handler = (event: MessageEvent) => {
      clearTimeout(timeout)
      ws.removeEventListener('message', handler)
      resolve(JSON.parse(String(event.data)))
    }
    ws.addEventListener('message', handler)
    ws.send(JSON.stringify(msg))
  })
}

async function expectRowCount(baseUrl: string, expected: number): Promise<void> {
  const res = await fetch(`${baseUrl}/db/test/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: 'SELECT COUNT(*) AS count FROM documents' }),
  })
  const body = (await res.json()) as { rows: { count: number }[] }
  expect(body.rows[0].count).toBe(expected)
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-body-limit-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)')
  server = null
})

afterEach(async () => {
  if (server) await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('maxBodyBytes option validation', () => {
  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])('rejects %s', value => {
    expect(() => createServer(sirannon, { port: 0, maxBodyBytes: value })).toThrow(/positive integer/)
  })

  it('accepts a positive integer', () => {
    const created = createServer(sirannon, { port: 0, maxBodyBytes: 4_194_304 })
    expect(created).toBeDefined()
  })

  it('accepts the unsigned 32-bit maximum', () => {
    const created = createServer(sirannon, { port: 0, maxBodyBytes: 4_294_967_295 })
    expect(created).toBeDefined()
  })

  it.each([
    4_294_967_296,
    5 * 2 ** 30,
    Number.MAX_SAFE_INTEGER,
  ])('rejects %s because uWebSockets.js would wrap it modulo 2^32', value => {
    expect(() => createServer(sirannon, { port: 0, maxBodyBytes: value })).toThrow(/at most 4294967295/)
  })
})

describe('maxWebSocketBackpressureBytes option validation', () => {
  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])('rejects %s', value => {
    expect(() => createServer(sirannon, { port: 0, maxWebSocketBackpressureBytes: value })).toThrow(/positive integer/)
  })

  it('rejects a value below maxBodyBytes', () => {
    expect(() =>
      createServer(sirannon, { port: 0, maxBodyBytes: 2_097_152, maxWebSocketBackpressureBytes: 1_048_576 }),
    ).toThrow(/at least maxBodyBytes/)
  })

  it('accepts the unsigned 32-bit maximum', () => {
    const created = createServer(sirannon, { port: 0, maxWebSocketBackpressureBytes: 4_294_967_295 })
    expect(created).toBeDefined()
  })

  it.each([
    4_294_967_296,
    5 * 2 ** 30,
    Number.MAX_SAFE_INTEGER,
  ])('rejects %s because uWebSockets.js would wrap it modulo 2^32', value => {
    expect(() => createServer(sirannon, { port: 0, maxWebSocketBackpressureBytes: value })).toThrow(
      /at most 4294967295/,
    )
  })
})

describe('HTTP body limit', () => {
  it('rejects a body above the default 1 MB limit with 413', async () => {
    const baseUrl = await startServer()
    const oversized = 'x'.repeat(1_048_576 + 1024)
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO documents (content) VALUES (?)', params: [oversized] }),
    }).catch(() => null)
    if (res === null) {
      await expectRowCount(baseUrl, 0)
      return
    }
    expect(res.status).toBe(413)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('PAYLOAD_TOO_LARGE')
  })

  it('accepts a body above 1 MB when the limit is raised', async () => {
    const baseUrl = await startServer({ maxBodyBytes: 8_388_608 })
    const large = 'x'.repeat(2_097_152)
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO documents (content) VALUES (?)', params: [large] }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.changes).toBe(1)
  })

  it('still rejects a body above a raised limit with 413', async () => {
    const baseUrl = await startServer({ maxBodyBytes: 2_097_152 })
    const oversized = 'x'.repeat(2_097_152 + 1024)
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO documents (content) VALUES (?)', params: [oversized] }),
    }).catch(() => null)
    if (res === null) {
      await expectRowCount(baseUrl, 0)
      return
    }
    expect(res.status).toBe(413)
  })

  it('rejects a body above a lowered limit with 413', async () => {
    const baseUrl = await startServer({ maxBodyBytes: 256 })
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO documents (content) VALUES (?)', params: ['y'.repeat(512)] }),
    }).catch(() => null)
    if (res === null) {
      await expectRowCount(baseUrl, 0)
      return
    }
    expect(res.status).toBe(413)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('PAYLOAD_TOO_LARGE')
  })
})

describe('WebSocket payload limit', () => {
  it('closes the connection for a message above the default 1 MB limit', async () => {
    const baseUrl = await startServer()
    const ws = new WebSocket(`${baseUrl.replace('http', 'ws')}/db/test`)
    await waitForOpen(ws)
    const closed = waitForClose(ws)
    ws.send(JSON.stringify({ type: 'query', id: 'q1', sql: 'SELECT ?', params: ['x'.repeat(1_048_576 + 1024)] }))
    await closed
  })

  it('accepts a message above 1 MB when the limit is raised', async () => {
    const baseUrl = await startServer({ maxBodyBytes: 8_388_608 })
    const ws = new WebSocket(`${baseUrl.replace('http', 'ws')}/db/test`)
    await waitForOpen(ws)
    const large = 'x'.repeat(2_097_152)
    const msg = await sendAndReceive(ws, {
      type: 'execute',
      id: 'e1',
      sql: 'INSERT INTO documents (content) VALUES (?)',
      params: [large],
    })
    expect(msg.type).toBe('result')
    ws.close()
  })
})
