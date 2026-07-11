import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-ve-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER, payload BLOB)')

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function post(path: string, body: unknown): Promise<{ status: number; data: Record<string, unknown> }> {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: res.status, data: (await res.json()) as Record<string, unknown> }
}

describe('HTTP value encoding', () => {
  it('returns integers beyond 2^53 and BLOBs in query rows as tagged envelopes', async () => {
    const db = sirannon.get('test')
    await db?.execute("INSERT INTO ledgers VALUES (1, 9007199254740993, X'0001FFAB')")

    const { status, data } = await post('/db/test/query', { sql: 'SELECT * FROM ledgers' })
    expect(status).toBe(200)
    const rows = data.rows as Record<string, unknown>[]
    expect(rows[0].id).toBe(1)
    expect(rows[0].balance).toEqual({ __sirannon_int: '9007199254740993' })
    expect(rows[0].payload).toEqual({ __sirannon_blob: '0001FFAB' })
  })

  it('stores envelope-encoded params exactly across execute, transaction, and batch', async () => {
    const execute = await post('/db/test/execute', {
      sql: 'INSERT INTO ledgers (id, balance) VALUES (?, ?)',
      params: [1, { __sirannon_int: '9223372036854775807' }],
    })
    expect(execute.status).toBe(200)

    const transaction = await post('/db/test/transaction', {
      statements: [
        {
          sql: 'INSERT INTO ledgers (id, balance, payload) VALUES (?, ?, ?)',
          params: [2, { __sirannon_int: '-9223372036854775808' }, { __sirannon_blob: 'FF00' }],
        },
      ],
    })
    expect(transaction.status).toBe(200)

    const batch = await post('/db/test/batch', {
      sql: 'INSERT INTO ledgers (id, balance) VALUES (?, ?)',
      paramsBatch: [[3, { __sirannon_int: '9007199254740995' }]],
    })
    expect(batch.status).toBe(200)

    const { data } = await post('/db/test/query', {
      sql: 'SELECT CAST(balance AS TEXT) AS balance_text, hex(payload) AS payload_hex FROM ledgers ORDER BY id',
    })
    const rows = data.rows as { balance_text: string; payload_hex: string | null }[]
    expect(rows.map(r => r.balance_text)).toEqual(['9223372036854775807', '-9223372036854775808', '9007199254740995'])
    expect(rows[1].payload_hex).toBe('FF00')
  })

  it('rejects malformed integer envelopes in params with 400', async () => {
    const { status, data } = await post('/db/test/execute', {
      sql: 'INSERT INTO ledgers (id, balance) VALUES (?, ?)',
      params: [1, { __sirannon_int: '9'.repeat(40) }],
    })
    expect(status).toBe(400)
    expect((data.error as Record<string, unknown>).code).toBe('INVALID_REQUEST')
  })
})
