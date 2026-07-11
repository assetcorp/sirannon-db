import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
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
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-cve-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler change value encoding', () => {
  it('delivers a change on a row with an integer above 2^53 as a tagged envelope instead of losing it', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'bigint.db'))
    await db.execute('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER, payload BLOB)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'ledgers' }))
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))

    const big = 9007199254740993n
    const payload = Buffer.from([0x00, 0x01, 0xff, 0xab])
    await db.execute('INSERT INTO ledgers (id, balance, payload) VALUES (?, ?, ?)', [1, big, payload])

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'change'))

    expect(conn.closed).toBe(false)
    const changeMsg = parseMessages(conn).find(m => m.type === 'change') as Record<string, unknown>
    const event = changeMsg.event as Record<string, unknown>
    const row = event.row as Record<string, unknown>
    expect(row.id).toBe(1)
    expect(row.balance).toEqual({ __sirannon_int: '9007199254740993' })
    expect(row.payload).toEqual({ __sirannon_blob: '0001FFAB' })
    await handler.close()
  })

  it('encodes oldRow values on update changes the same way as row values', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'oldrow.db'))
    await db.execute('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'ledgers' }))
    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))

    await db.execute('INSERT INTO ledgers (id, balance) VALUES (?, ?)', [1, 9007199254740993n])
    await db.execute('UPDATE ledgers SET balance = ? WHERE id = ?', [9007199254740995n, 1])

    await flushAsync(() => parseMessages(conn).filter(m => m.type === 'change').length >= 2)

    const updateMsg = parseMessages(conn)
      .filter(m => m.type === 'change')
      .map(m => m.event as Record<string, unknown>)
      .find(e => e.type === 'update')
    expect(updateMsg).toBeDefined()
    expect((updateMsg?.row as Record<string, unknown>).balance).toEqual({ __sirannon_int: '9007199254740995' })
    expect((updateMsg?.oldRow as Record<string, unknown>).balance).toEqual({ __sirannon_int: '9007199254740993' })
    await handler.close()
  })
})
