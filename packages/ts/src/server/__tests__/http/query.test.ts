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
