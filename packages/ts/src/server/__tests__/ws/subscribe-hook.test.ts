import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { HookDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { HookConfig } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createWSHandler, type WSHandler } from '../../ws-handler.js'
import { createMockConnection, type MockWSConnection, parseMessages, wait } from '../helpers.js'

const DEVICE = 'abcd0000abcd0000abcd0000abcd0000'
const IDENTITY = { tenant: 'acme' }

let tempDir: string
let sirannon: Sirannon
let handler: WSHandler
let db: Database
let conn: MockWSConnection

const driver = betterSqlite3()

async function open(hooks: HookConfig): Promise<void> {
  sirannon = new Sirannon({ driver, hooks })
  db = await sirannon.open('mydb', join(tempDir, 'subscribe-hook.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)')
  await db.watch('notes')
  await db.watch('ledger')
  handler = createWSHandler(sirannon, { acceptSql: true })
  conn = createMockConnection()
  await handler.handleOpen(conn, 'mydb', IDENTITY)
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-subscribe-hook-'))
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
    await wait(10)
  }
}

function messagesOfType(type: string): Record<string, unknown>[] {
  return parseMessages(conn).filter(msg => msg.type === type)
}

describe('beforeSubscribe hook', () => {
  it('receives the database, the table, the filter, and the identity of the subscriber', async () => {
    const seen: unknown[] = []
    await open({ onBeforeSubscribe: ctx => void seen.push({ ...ctx }) })

    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes', filter: { id: 1 } }))
    await until(() => messagesOfType('subscribed').length === 1)

    expect(seen).toEqual([{ databaseId: 'mydb', table: 'notes', filter: { id: 1 }, identity: IDENTITY }])
  })

  it('refuses the subscription the hook denies and delivers no change from that table', async () => {
    await open({
      onBeforeSubscribe: ctx => {
        if (ctx.table === 'ledger') throw new HookDeniedError('beforeSubscribe', 'the ledger is closed')
      },
    })

    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'ledger' }))
    await until(() => messagesOfType('error').length === 1)

    expect(messagesOfType('error')[0]).toMatchObject({
      id: 's1',
      error: { code: 'HOOK_DENIED' },
    })
    expect(messagesOfType('subscribed')).toHaveLength(0)

    await db.execute('INSERT INTO ledger (id, amount) VALUES (1, 500)')
    await wait(200)
    expect(messagesOfType('change')).toHaveLength(0)
  })

  it('runs for every table a device subscribes to and refuses the whole subscription on a denial', async () => {
    const seen: string[] = []
    await open({
      onBeforeSubscribe: ctx => {
        seen.push(ctx.table)
        if (ctx.table === 'ledger') throw new HookDeniedError('beforeSubscribe', 'the ledger is closed')
      },
    })

    handler.handleMessage(
      conn,
      JSON.stringify({ type: 'subscribe', id: 's1', tables: ['notes', 'ledger'], deviceId: DEVICE }),
    )
    await until(() => messagesOfType('error').length === 1)

    expect(seen).toEqual(['notes', 'ledger'])
    expect(messagesOfType('subscribed')).toHaveLength(0)

    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'kept private')")
    await wait(200)
    expect(messagesOfType('change')).toHaveLength(0)
  })

  it('waits for a hook that returns a promise before it opens the subscription', async () => {
    let resolved = false
    await open({
      onBeforeSubscribe: async () => {
        await wait(30)
        resolved = true
      },
    })

    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    await until(() => messagesOfType('subscribed').length === 1)

    expect(resolved).toBe(true)
  })

  it('reports the code a hook throwing its own error carries', async () => {
    await open({
      onBeforeSubscribe: () => {
        throw new Error('the operator refused')
      },
    })

    handler.handleMessage(conn, JSON.stringify({ type: 'subscribe', id: 's1', table: 'notes' }))
    await until(() => messagesOfType('error').length === 1)

    expect(messagesOfType('error')[0]).toMatchObject({ error: { code: 'INTERNAL_ERROR' } })
  })
})
