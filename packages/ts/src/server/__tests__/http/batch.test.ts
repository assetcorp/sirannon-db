import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
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

async function countUsers(): Promise<number> {
  const res = await fetch(`${baseUrl}/db/test/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: 'SELECT COUNT(*) AS count FROM users' }),
  })
  const body = (await res.json()) as ApiResponse
  return body.rows[0].count as number
}

describe('POST /db/:id/batch', () => {
  it('applies one statement over many parameter sets in one transaction', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'INSERT INTO users (name, age) VALUES (?, ?)',
        paramsBatch: [
          ['Alice', 30],
          ['Bob', 25],
          ['Carol', 28],
        ],
      }),
    })
    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.results).toHaveLength(3)
    expect(body.results[0].changes).toBe(1)
    expect(body.results[2].lastInsertRowId).toBe(3)
    expect(await countUsers()).toBe(3)
  })

  it('supports named parameters', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'INSERT INTO users (name, age) VALUES (:name, :age)',
        paramsBatch: [
          { name: 'Alice', age: 30 },
          { name: 'Bob', age: 25 },
        ],
      }),
    })
    expect(res.status).toBe(200)
    expect(await countUsers()).toBe(2)
  })

  it('rolls the whole batch back when one parameter set fails', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
        paramsBatch: [
          [1, 'Alice', 30],
          [2, 'Bob', 25],
          [1, 'Duplicate', 40],
        ],
      }),
    })
    expect(res.status).toBe(400)
    expect(await countUsers()).toBe(0)
  })

  it('is visible to CDC subscribers', async () => {
    const db = sirannon.get('test')
    if (!db) throw new Error('database not open')
    await db.watch('users')
    const events: ChangeEvent[] = []
    const sub = db.on('users').subscribe(event => {
      events.push(event)
    })

    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'INSERT INTO users (name, age) VALUES (?, ?)',
        paramsBatch: [
          ['Alice', 30],
          ['Bob', 25],
        ],
      }),
    })
    expect(res.status).toBe(200)

    await new Promise(resolve => setTimeout(resolve, 300))
    sub.unsubscribe()
    expect(events).toHaveLength(2)
    expect(events.every(event => event.type === 'insert')).toBe(true)
  })

  it('returns 400 for missing sql field', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ paramsBatch: [[1]] }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('INVALID_REQUEST')
  })

  it('returns 400 for missing paramsBatch field', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO users (name) VALUES (?)' }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.message).toContain('paramsBatch')
  })

  it('returns 400 for an empty paramsBatch array', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO users (name) VALUES (?)', paramsBatch: [] }),
    })
    expect(res.status).toBe(400)
  })

  it('returns 400 when a parameter set is not an object or array', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INSERT INTO users (name) VALUES (?)', paramsBatch: [['a'], 'b'] }),
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.message).toContain('index 1')
  })

  it('returns 400 for an invalid writeConcern', async () => {
    const res = await fetch(`${baseUrl}/db/test/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: 'INSERT INTO users (name) VALUES (?)',
        paramsBatch: [['a']],
        writeConcern: { level: 'everyone' },
      }),
    })
    expect(res.status).toBe(400)
  })

  it('returns 404 for unknown database', async () => {
    const res = await fetch(`${baseUrl}/db/nope/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1', paramsBatch: [[]] }),
    })
    expect(res.status).toBe(404)
  })
})
