import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
  error: { code: string; message: string }
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-guard-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function post(path: string, body: unknown): Promise<{ status: number; body: ApiResponse }> {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: res.status, body: (await res.json()) as ApiResponse }
}

describe('internal-table guard over HTTP', () => {
  it('rejects reading the internal change log', async () => {
    const { status, body } = await post('/db/test/query', { sql: 'SELECT * FROM _sirannon_changes' })
    expect(status).toBe(403)
    expect(body.error.code).toBe('FORBIDDEN_SQL')
  })

  it('rejects writing to an internal table', async () => {
    const { status, body } = await post('/db/test/execute', { sql: 'DELETE FROM _sirannon_applied_changes' })
    expect(status).toBe(403)
    expect(body.error.code).toBe('FORBIDDEN_SQL')
  })

  it('rejects ATTACH', async () => {
    const { status, body } = await post('/db/test/execute', { sql: "ATTACH DATABASE 'secret.db' AS s" })
    expect(status).toBe(403)
    expect(body.error.code).toBe('FORBIDDEN_SQL')
  })

  it('rejects a reserved reference inside a transaction statement', async () => {
    const { status, body } = await post('/db/test/transaction', {
      statements: [{ sql: "INSERT INTO users (name) VALUES ('Alice')" }, { sql: 'DROP TABLE sqlite_sequence' }],
    })
    expect(status).toBe(403)
    expect(body.error.code).toBe('FORBIDDEN_SQL')
  })

  it('still serves ordinary user queries', async () => {
    const { status, body } = await post('/db/test/query', { sql: 'SELECT * FROM users' })
    expect(status).toBe(200)
    expect(body.rows).toEqual([])
  })

  it('allows reading the sqlite_ catalogue', async () => {
    const { status, body } = await post('/db/test/query', {
      sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
    })
    expect(status).toBe(200)
    expect(body.rows).toEqual([{ name: 'users' }])
  })

  it('rejects PRAGMA writable_schema', async () => {
    const { status, body } = await post('/db/test/execute', { sql: 'PRAGMA writable_schema = ON' })
    expect(status).toBe(403)
    expect(body.error.code).toBe('FORBIDDEN_SQL')
  })
})
