import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { DEVICE_CURSORS_TABLE } from '../../../core/internal-tables.js'
import { Sirannon } from '../../../core/sirannon.js'
import { computeChecksum } from '../../../core/sync/checksum.js'
import { HLC } from '../../../core/sync/hlc.js'
import type { ReplicationChange } from '../../../core/sync/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, type WSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages } from '../helpers.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'

let tempDir: string
let sirannon: Sirannon
let handler: WSHandler
let db: Database
let conn: MockWSConnection

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-ack-'))
  sirannon = new Sirannon({ driver })
  handler = createWSHandler(sirannon)
  db = await sirannon.open('mydb', join(tempDir, 'ack.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
})

afterEach(async () => {
  await handler.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function until(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(r => setTimeout(r, 5))
  }
}

function messagesOf(type: string): Record<string, unknown>[] {
  return parseMessages(conn).filter(m => m.type === type)
}

describe('WS acks', () => {
  it('records a device cursor and confirms the ack', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'ack', id: 'a1', deviceId: DEVICE, seq: '7' }))
    await until(() => messagesOf('result').length >= 1)

    expect(messagesOf('result')[0].data).toEqual({ acked: true, seq: '7' })
    const inspect = await driver.open(join(tempDir, 'ack.db'))
    const stmt = await inspect.prepare(`SELECT device_id, acked_seq FROM ${DEVICE_CURSORS_TABLE}`)
    const rows = (await stmt.all()) as { device_id: string; acked_seq: number }[]
    await inspect.close()
    expect(rows).toHaveLength(1)
    expect(rows[0].device_id).toBe(DEVICE)
    expect(Number(rows[0].acked_seq)).toBe(7)
  })

  it('rejects a malformed device id', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'ack', id: 'a2', deviceId: 'short', seq: '1' }))
    await until(() => messagesOf('error').length >= 1)
    expect((messagesOf('error')[0].error as { code: string }).code).toBe('INVALID_MESSAGE')
  })
})

describe('WS echo suppression', () => {
  it('does not stream a change back to the device that originated it', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes', deviceId: DEVICE }))
    await until(() => messagesOf('subscribed').length >= 1)

    const hlc = new HLC(DEVICE).now()
    const changes: ReplicationChange[] = [
      {
        table: 'notes',
        operation: 'insert',
        rowId: '10',
        primaryKey: { id: 10 },
        hlc,
        txId: 'device-tx',
        nodeId: DEVICE,
        newData: { id: 10, body: 'own write' },
        oldData: null,
      },
    ]
    await db.applyChanges({
      sourceNodeId: DEVICE,
      batchId: `${DEVICE}-1-1`,
      fromSeq: 1n,
      toSeq: 1n,
      hlcRange: { min: hlc, max: hlc },
      changes,
      checksum: computeChecksum(changes),
    })
    await db.execute("INSERT INTO notes (id, body) VALUES (11, 'server write')")

    await until(() => messagesOf('change').length >= 1)
    await new Promise(r => setTimeout(r, 200))

    const received = messagesOf('change').map(m => (m.event as { row: { id: number } }).row.id)
    expect(received).toContain(11)
    expect(received).not.toContain(10)
  })
})
