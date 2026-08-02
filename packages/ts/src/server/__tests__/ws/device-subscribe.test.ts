import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { CHANGES_TABLE } from '../../../core/internal-tables.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, type WSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages } from '../helpers.js'

const DEVICE = 'eeee0000eeee0000eeee0000eeee0000'

let tempDir: string
let sirannon: Sirannon
let handler: WSHandler
let db: Database
let conn: MockWSConnection

const driver = betterSqlite3()

async function openHandler(maxUnacknowledgedChanges?: number): Promise<void> {
  handler = createWSHandler(sirannon, maxUnacknowledgedChanges === undefined ? undefined : { maxUnacknowledgedChanges })
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-device-'))
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('mydb', join(tempDir, 'device-subscribe.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  await openHandler()
})

afterEach(async () => {
  await handler.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function until(predicate: () => boolean, timeout = 3000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

function messagesOfType(type: string): Record<string, unknown>[] {
  return parseMessages(conn).filter(msg => msg.type === type)
}

describe('device subscription', () => {
  it('reports the delivery window so a device can acknowledge before it fills', async () => {
    await handler.close()
    await openHandler(7)

    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', tables: ['notes'], deviceId: DEVICE }))
    await until(() => messagesOfType('subscribed').length === 1)

    expect(messagesOfType('subscribed')[0]).toMatchObject({ maxUnacknowledgedChanges: 7 })
  })

  it('does not report a window to a plain subscriber', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    await until(() => messagesOfType('subscribed').length === 1)

    expect(messagesOfType('subscribed')[0]).not.toHaveProperty('maxUnacknowledgedChanges')
  })

  it('withholds an unstamped change from a device', async () => {
    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', tables: ['notes'], deviceId: DEVICE }))
    await until(() => messagesOfType('subscribed').length === 1)

    await db.runCdcMaintenance(async writer => {
      const stmt = await writer.prepare(
        `INSERT INTO ${CHANGES_TABLE} (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
         VALUES ('notes', 'INSERT', '1', '{"id":1,"body":"unstamped"}', '', '', '')`,
      )
      await stmt.run()
    })

    await new Promise(resolve => setTimeout(resolve, 150))
    await db.execute("INSERT INTO notes (id, body) VALUES (2, 'stamped')")

    await until(() => messagesOfType('change').length > 0)
    await new Promise(resolve => setTimeout(resolve, 150))

    const rowIds = messagesOfType('change').map(msg => (msg.event as { rowId: string }).rowId)
    expect(rowIds).not.toContain('1')
    expect(rowIds).toContain('2')
  })
})
