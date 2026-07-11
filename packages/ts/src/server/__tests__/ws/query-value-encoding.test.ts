import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { decodeTaggedValues } from '../../../core/cdc/encoding.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, parseMessages } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

async function flushAsync(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`flushAsync timed out after ${timeout}ms: condition never became true`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-qve-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler query value encoding', () => {
  it('returns integers beyond 2^53 and BLOBs in query rows as tagged envelopes', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'query.db'))
    await db.execute('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER, payload BLOB)')
    await db.execute("INSERT INTO ledgers VALUES (1, 9007199254740993, X'0001FFAB')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'q-1', type: 'query', sql: 'SELECT * FROM ledgers' }))

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'result'))

    const result = parseMessages(conn).find(m => m.type === 'result') as Record<string, unknown>
    const rows = (result.data as { rows: Record<string, unknown>[] }).rows
    expect(rows[0].id).toBe(1)
    expect(rows[0].balance).toEqual({ __sirannon_int: '9007199254740993' })
    expect(rows[0].payload).toEqual({ __sirannon_blob: '0001FFAB' })
    await handler.close()
  })

  it('stores envelope-encoded params exactly and rejects malformed envelopes', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'params.db'))
    await db.execute('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER, payload BLOB)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'e-1',
        type: 'execute',
        sql: 'INSERT INTO ledgers (id, balance, payload) VALUES (?, ?, ?)',
        params: [1, { __sirannon_int: '9223372036854775807' }, { __sirannon_blob: 'FF00' }],
      }),
    )
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'result'))

    const rows = await db.query<{ balance_text: string; payload_hex: string }>(
      'SELECT CAST(balance AS TEXT) AS balance_text, hex(payload) AS payload_hex FROM ledgers WHERE id = 1',
    )
    expect(rows[0].balance_text).toBe('9223372036854775807')
    expect(rows[0].payload_hex).toBe('FF00')

    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 'e-2',
        type: 'execute',
        sql: 'INSERT INTO ledgers (id, balance) VALUES (?, ?)',
        params: [2, { __sirannon_int: '12abc' }],
      }),
    )
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'error'))

    const error = parseMessages(conn).find(m => m.type === 'error') as Record<string, unknown>
    expect((error.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
    await handler.close()
  })

  it('delivers the same decoded value through a CDC change event and a query result', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'crosspath.db'))
    await db.execute('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'ledgers' }))
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))

    await db.execute('INSERT INTO ledgers (id, balance) VALUES (1, 9007199254740995)')
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

    handler.handleMessage(
      conn,
      JSON.stringify({ id: 'q-1', type: 'query', sql: 'SELECT balance FROM ledgers WHERE id = 1' }),
    )
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'result'))

    const messages = parseMessages(conn)
    const changeMsg = messages.find(m => m.type === 'change') as Record<string, unknown>
    const resultMsg = messages.find(m => m.type === 'result') as Record<string, unknown>

    const changeRow = decodeTaggedValues((changeMsg.event as Record<string, unknown>).row) as Record<string, unknown>
    const queryRow = decodeTaggedValues((resultMsg.data as { rows: unknown[] }).rows[0]) as Record<string, unknown>

    expect(changeRow.balance).toBe(9007199254740995n)
    expect(queryRow.balance).toBe(9007199254740995n)
    expect(changeRow.balance).toBe(queryRow.balance)
    await handler.close()
  })
})
