import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
  changes: number
  lastInsertRowId: number | string
  results: { changes: number; lastInsertRowId: number | string }[]
  error: { code: string; message: string }
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

  server = createServer(sirannon, { port: 0 })
  await server.listen()
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('onRequest', () => {
  it('denies requests with custom status, code, and message', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: () => ({ status: 403, code: 'FORBIDDEN', message: 'Access denied' }),
    })
    await hookServer.listen()
    const hookUrl = `http://127.0.0.1:${hookServer.listeningPort}`

    try {
      const res = await fetch(`${hookUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })
      expect(res.status).toBe(403)
      const body = (await res.json()) as ApiResponse
      expect(body.error.code).toBe('FORBIDDEN')
      expect(body.error.message).toBe('Access denied')
    } finally {
      await hookServer.close()
    }
  })

  it('allows requests when hook returns void', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: ({ headers }) => {
        if (headers.authorization !== 'Bearer valid-token') {
          return { status: 401, code: 'UNAUTHORIZED', message: 'Authentication required' }
        }
      },
    })
    await hookServer.listen()
    const hookUrl = `http://127.0.0.1:${hookServer.listeningPort}`

    try {
      const res = await fetch(`${hookUrl}/db/test/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Bearer valid-token',
        },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.status).toBe(200)
    } finally {
      await hookServer.close()
    }
  })

  it('does not run onRequest for health endpoints', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: () => ({ status: 403, code: 'FORBIDDEN', message: 'Blocked' }),
    })
    await hookServer.listen()
    const hookUrl = `http://127.0.0.1:${hookServer.listeningPort}`

    try {
      const healthRes = await fetch(`${hookUrl}/health`)
      expect(healthRes.status).toBe(200)

      const readyRes = await fetch(`${hookUrl}/health/ready`)
      expect(readyRes.status).toBe(200)
    } finally {
      await hookServer.close()
    }
  })

  it('handles async onRequest hook (allow and deny)', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: async ({ headers }) => {
        await new Promise(r => setTimeout(r, 5))
        if (headers['x-api-key'] !== 'secret') {
          return { status: 401, code: 'UNAUTHORIZED', message: 'Bad key' }
        }
      },
    })
    await hookServer.listen()
    const hookUrl = `http://127.0.0.1:${hookServer.listeningPort}`

    try {
      const denied = await fetch(`${hookUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })
      expect(denied.status).toBe(401)

      const allowed = await fetch(`${hookUrl}/db/test/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Api-Key': 'secret',
        },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(allowed.status).toBe(200)
    } finally {
      await hookServer.close()
    }
  })

  it('returns 500 HOOK_ERROR when hook throws', async () => {
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: () => {
        throw new Error('hook crashed')
      },
    })
    await hookServer.listen()
    const hookUrl = `http://127.0.0.1:${hookServer.listeningPort}`

    try {
      const res = await fetch(`${hookUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })
      expect(res.status).toBe(500)
      const body = (await res.json()) as ApiResponse
      expect(body.error.code).toBe('HOOK_ERROR')
    } finally {
      await hookServer.close()
    }
  })

  it('populates context with method, path, databaseId, remoteAddress, and headers', async () => {
    let capturedCtx: Record<string, unknown> | undefined
    const hookServer = createServer(sirannon, {
      port: 0,
      onRequest: ctx => {
        capturedCtx = { ...ctx }
        return undefined
      },
    })
    await hookServer.listen()
    const hookUrl = `http://127.0.0.1:${hookServer.listeningPort}`

    try {
      await fetch(`${hookUrl}/db/test/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Custom': 'test-value',
        },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })

      expect(capturedCtx).toBeDefined()
      expect(capturedCtx?.method).toBe('post')
      expect(capturedCtx?.path).toBe('/db/test/query')
      expect(capturedCtx?.databaseId).toBe('test')
      expect(typeof capturedCtx?.remoteAddress).toBe('string')
      const headers = capturedCtx?.headers as Record<string, string>
      expect(headers['x-custom']).toBe('test-value')
    } finally {
      await hookServer.close()
    }
  })
})
