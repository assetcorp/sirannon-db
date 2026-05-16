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
