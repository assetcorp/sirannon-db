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
let baseUrl: string

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
  const port = server.listeningPort
  baseUrl = `http://127.0.0.1:${port}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
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
    expect(res.status).toBe(400)

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

  it('rejects a statement whose params is neither an object nor an array', async () => {
    const res = await fetch(`${baseUrl}/db/test/transaction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        statements: [{ sql: 'INSERT INTO users (name, age) VALUES (?, ?)', params: 'nope' }],
      }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('INVALID_REQUEST')
    expect(body.error.message).toContain('index 0')
  })
})
