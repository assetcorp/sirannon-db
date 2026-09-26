import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { CDCMetrics } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, type WSHandler } from '../../ws-handler.js'
import { createMockConnection, parseMessages } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon
let handler: WSHandler
const events: CDCMetrics[] = []

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-cdc-metrics-'))
  events.length = 0
  sirannon = new Sirannon({ driver: betterSqlite3(), metrics: { onCDCEvent: m => events.push(m) } })
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

it('reports each change event that the server delivers to a WebSocket subscriber', async () => {
  const db = await sirannon.open('mydb', join(tempDir, 'cdc-metrics.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  handler = createWSHandler(sirannon, {})
  const conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb')
  handler.handleMessage(conn, JSON.stringify({ id: 'notes-feed', type: 'subscribe', table: 'notes' }))
  await until(() => parseMessages(conn).some(msg => msg.type === 'subscribed'))

  await db.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")
  await until(() => events.length > 0)

  expect(events[0]).toEqual({ databaseId: 'mydb', table: 'notes', operation: 'insert', subscriberCount: 1 })
})
