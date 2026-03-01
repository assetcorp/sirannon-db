import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { createServer, type SirannonServer } from '../server.js'

/** Union response type covering all HTTP endpoint shapes in tests. */
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
let baseUrl: string

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-'))
  sirannon = new Sirannon()
  const db = sirannon.open('test', join(tempDir, 'test.db'))
  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  const port = server.listeningPort
  baseUrl = `http://127.0.0.1:${port}`
})

afterEach(async () => {
  await server.close()
  sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('POST /db/:id/query', () => {
  it('returns rows for a valid SELECT', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT * FROM users ORDER BY id' }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows).toHaveLength(2)
    expect(body.rows[0].name).toBe('Alice')
    expect(body.rows[1].name).toBe('Bob')
  })

  it('supports named parameters', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT * FROM users WHERE name = :name',
        params: { name: 'Alice' },
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows).toHaveLength(1)
    expect(body.rows[0].age).toBe(30)
  })

  it('supports positional parameters', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT * FROM users WHERE age > ?',
        params: [26],
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows).toHaveLength(1)
    expect(body.rows[0].name).toBe('Alice')
  })

  it('returns empty rows for no matches', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT * FROM users WHERE id = 999',
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rows).toEqual([])
  })

  it('returns 404 for unknown database', async () => {
    const res = await fetch(`${baseUrl}/db/nope/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1' }),
    })
    expect(res.status).toBe(404)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('DATABASE_NOT_FOUND')
  })

  it('returns 400 for missing sql field', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('INVALID_REQUEST')
  })

  it('returns 400 for invalid JSON body', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: 'not json',
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('INVALID_JSON')
  })

  it('returns 400 for empty body', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '',
    })
    expect(res.status).toBe(400)
  })

  it('returns 400 for invalid SQL', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELCT GARBAGE' }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('QUERY_ERROR')
  })
})

describe('POST /db/:id/execute', () => {
  it('executes an INSERT and returns changes', async () => {
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "INSERT INTO users (name, age) VALUES ('Carol', 28)",
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.changes).toBe(1)
    expect(body.lastInsertRowId).toBeDefined()
  })

  it('executes an UPDATE and returns affected row count', async () => {
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'UPDATE users SET age = 31 WHERE name = :name',
        params: { name: 'Alice' },
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.changes).toBe(1)
  })

  it('executes a DELETE and returns affected row count', async () => {
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "DELETE FROM users WHERE name = 'Bob'",
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.changes).toBe(1)
  })

  it('returns 404 for unknown database', async () => {
    const res = await fetch(`${baseUrl}/db/nope/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: "INSERT INTO x VALUES ('a')" }),
    })
    expect(res.status).toBe(404)
  })

  it('returns 400 for missing sql field', async () => {
    const res = await fetch(`${baseUrl}/db/test/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ params: [] }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('INVALID_REQUEST')
  })
})

describe('POST /db/:id/transaction', () => {
  it('executes multiple statements in a transaction', async () => {
    const res = await fetch(`${baseUrl}/db/test/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        statements: [
          {
            sql: "INSERT INTO users (name, age) VALUES ('Carol', 28)",
          },
          {
            sql: "INSERT INTO users (name, age) VALUES ('Dave', 35)",
          },
        ],
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.results).toHaveLength(2)
    expect(body.results[0].changes).toBe(1)
    expect(body.results[1].changes).toBe(1)

    // Verify both rows exist
    const verify = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'SELECT COUNT(*) as count FROM users',
      }),
    })
    const verifyBody = (await verify.json()) as ApiResponse
    expect(verifyBody.rows[0].count).toBe(4)
  })

  it('rolls back on failure', async () => {
    const res = await fetch(`${baseUrl}/db/test/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        statements: [
          {
            sql: "INSERT INTO users (name, age) VALUES ('Eve', 22)",
          },
          { sql: 'INVALID SQL GARBAGE' },
        ],
      }),
    })
    // Transaction errors return 400
    expect(res.status).toBe(400)

    // Verify rollback: Eve should not exist
    const verify = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: "SELECT * FROM users WHERE name = 'Eve'",
      }),
    })
    const verifyBody = (await verify.json()) as ApiResponse
    expect(verifyBody.rows).toHaveLength(0)
  })

  it('returns 400 for missing statements field', async () => {
    const res = await fetch(`${baseUrl}/db/test/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('INVALID_REQUEST')
  })

  it('returns 400 for empty statements array', async () => {
    const res = await fetch(`${baseUrl}/db/test/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ statements: [] }),
    })
    expect(res.status).toBe(400)
  })

  it('returns 400 when a statement has no sql', async () => {
    const res = await fetch(`${baseUrl}/db/test/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        statements: [{ params: [] }],
      }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.message).toContain('index 0')
  })

  it('returns 404 for unknown database', async () => {
    const res = await fetch(`${baseUrl}/db/nope/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        statements: [{ sql: 'SELECT 1' }],
      }),
    })
    expect(res.status).toBe(404)
  })
})

describe('error responses', () => {
  it('returns 404 for unknown routes', async () => {
    const res = await fetch(`${baseUrl}/nonexistent`, {
      method: 'GET',
    })
    expect(res.status).toBe(404)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('NOT_FOUND')
  })

  it('error responses include code and message', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'DROP TABLE nonexistent' }),
    })
    const body = (await res.json()) as ApiResponse
    expect(body.error).toBeDefined()
    expect(typeof body.error.code).toBe('string')
    expect(typeof body.error.message).toBe('string')
  })

  it('response Content-Type is application/json', async () => {
    const res = await fetch(`${baseUrl}/db/test/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1 as val' }),
    })
    expect(res.headers.get('content-type')).toBe('application/json')
  })
})

describe('CORS', () => {
  it('sets CORS headers when cors: true', async () => {
    // Create a separate server with CORS enabled
    const corsServer = createServer(sirannon, { port: 0, cors: true })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      // Preflight
      const preflight = await fetch(`${corsUrl}/db/test/query`, {
        method: 'OPTIONS',
      })
      expect(preflight.status).toBe(204)
      expect(preflight.headers.get('access-control-allow-origin')).toBe('*')
      expect(preflight.headers.get('access-control-allow-methods')).toContain('POST')

      // Regular request
      const res = await fetch(`${corsUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.headers.get('access-control-allow-origin')).toBe('*')
    } finally {
      await corsServer.close()
    }
  })

  it('uses custom CORS origin', async () => {
    const corsServer = createServer(sirannon, {
      port: 0,
      cors: { origin: 'https://app.example.com' },
    })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      const res = await fetch(`${corsUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.headers.get('access-control-allow-origin')).toBe('https://app.example.com')
    } finally {
      await corsServer.close()
    }
  })
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
