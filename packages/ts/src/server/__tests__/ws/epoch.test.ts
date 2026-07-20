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
      throw new Error(`flushAsync timed out after ${timeout}ms`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

function changeNames(conn: ReturnType<typeof createMockConnection>): string[] {
  return parseMessages(conn)
    .filter(m => m.type === 'change')
    .map(m => ((m.event as Record<string, unknown>).row as Record<string, string>).name)
}

function subscribedMessage(conn: ReturnType<typeof createMockConnection>): Record<string, unknown> {
  const found = parseMessages(conn).find(m => m.type === 'subscribed')
  if (!found) throw new Error('no subscribed message received')
  return found
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-epoch-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler subscription epoch', () => {
  it('reports a stable epoch string in the subscribed confirmation', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'epoch.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 's1', type: 'subscribe', table: 'users' }))

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))
    const epoch = subscribedMessage(conn).epoch
    expect(typeof epoch).toBe('string')
    expect((epoch as string).length).toBeGreaterThan(0)
    await handler.close()
  })

  it('replays normally when the resuming client echoes the matching epoch', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'match.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")

    const probe = createMockConnection()
    await handler.handleOpen(probe, 'mydb')
    handler.handleMessage(probe, JSON.stringify({ id: 's1', type: 'subscribe', table: 'users' }))
    await flushAsync(() => parseMessages(probe).some(m => m.type === 'subscribed'))
    const epoch = subscribedMessage(probe).epoch as string

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 's2', type: 'subscribe', table: 'users', sinceSeq: '0', epoch }))

    await flushAsync(() => changeNames(conn).length >= 2)
    expect(changeNames(conn)).toEqual(['Alice', 'Bob'])
    expect(subscribedMessage(conn).resync).toBeFalsy()
    await handler.close()
  })

  it('forces a resync without replay when the epoch belongs to another database', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'foreign.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(
      conn,
      JSON.stringify({
        id: 's1',
        type: 'subscribe',
        table: 'users',
        sinceSeq: '0',
        epoch: '00000000000000000000000000000000',
      }),
    )

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))
    expect(subscribedMessage(conn).resync).toBe(true)
    expect(changeNames(conn)).toHaveLength(0)
    await handler.close()
  })
})
