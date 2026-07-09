import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { ServerExecutionTarget } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
  rowsLoaded: number
  changes: number
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
  const db = await sirannon.open('test', join(tempDir, 'test.db'), { synchronous: 'full' })
  await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function postLoad(body: unknown): Promise<Response> {
  return fetch(`${baseUrl}/db/test/load`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

async function countReadings(): Promise<number> {
  const db = sirannon.get('test')
  if (!db) throw new Error('database not open')
  const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
  return rows[0].count
}

async function writerSynchronousLevel(): Promise<number> {
  const db = sirannon.get('test')
  if (!db) throw new Error('database not open')
  return db.transaction(async tx => {
    const rows = await tx.query<{ synchronous: number }>('PRAGMA synchronous')
    return rows[0].synchronous
  })
}

describe('POST /db/:id/load', () => {
  it('loads rows and returns an aggregate summary', async () => {
    const paramsBatch = Array.from({ length: 250 }, (_, i) => [i + 1, i * 0.5])
    const res = await postLoad({ sql: 'INSERT INTO readings (id, value) VALUES (?, ?)', paramsBatch })

    expect(res.status).toBe(200)
    const body = (await res.json()) as ApiResponse
    expect(body.rowsLoaded).toBe(250)
    expect(body.changes).toBe(250)
    expect(await countReadings()).toBe(250)
  })

  it('restores the configured durability after the load', async () => {
    await postLoad({
      sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
      paramsBatch: [
        [1, 1.0],
        [2, 2.0],
      ],
    })

    const fullLevel = 2
    expect(await writerSynchronousLevel()).toBe(fullLevel)
  })

  it('rolls the whole load back on failure and restores durability', async () => {
    const res = await postLoad({
      sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
      paramsBatch: [
        [1, 1.0],
        [1, 2.0],
      ],
    })

    expect(res.status).toBe(400)
    expect(await countReadings()).toBe(0)
    const fullLevel = 2
    expect(await writerSynchronousLevel()).toBe(fullLevel)
  })

  it('accepts an explicit normal load durability', async () => {
    const res = await postLoad({
      sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
      paramsBatch: [[1, 1.0]],
      durability: 'normal',
    })
    expect(res.status).toBe(200)
  })

  it('returns 400 for an invalid durability', async () => {
    const res = await postLoad({
      sql: 'INSERT INTO readings (id, value) VALUES (?, ?)',
      paramsBatch: [[1, 1.0]],
      durability: 'sometimes',
    })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.message).toContain('durability')
  })

  it('returns 400 for missing sql', async () => {
    const res = await postLoad({ paramsBatch: [[1]] })
    expect(res.status).toBe(400)
  })

  it('returns 400 for missing paramsBatch', async () => {
    const res = await postLoad({ sql: 'INSERT INTO readings (id) VALUES (?)' })
    expect(res.status).toBe(400)
    const body = (await res.json()) as ApiResponse
    expect(body.error.message).toContain('paramsBatch')
  })

  it('returns 404 for unknown database', async () => {
    const res = await fetch(`${baseUrl}/db/nope/load`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1', paramsBatch: [[]] }),
    })
    expect(res.status).toBe(404)
  })

  it('returns 501 when the execution target does not support bulk load', async () => {
    const target: ServerExecutionTarget = {
      query: async () => [],
      execute: async () => ({ changes: 0, lastInsertRowId: 0 }),
      transaction: async fn => fn(undefined as never),
    }
    const proxyServer = createServer(sirannon, { port: 0, resolveExecutionTarget: () => target })
    await proxyServer.listen()

    try {
      const res = await fetch(`http://127.0.0.1:${proxyServer.listeningPort}/db/test/load`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'INSERT INTO readings (id) VALUES (?)', paramsBatch: [[1]] }),
      })
      expect(res.status).toBe(501)
      const body = (await res.json()) as ApiResponse
      expect(body.error.code).toBe('BULK_LOAD_UNSUPPORTED')
    } finally {
      await proxyServer.close()
    }
  })
})
