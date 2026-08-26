import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { HookDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { HookConfig } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server.js'
import { createServer } from '../../server.js'

const driver = betterSqlite3()
const READER_TOKEN = 'reader-token'

interface Reader {
  role: string
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer<Reader>
let baseUrl: string
let db: Database

async function start(hooks: HookConfig): Promise<void> {
  sirannon = new Sirannon({ driver, hooks })
  db = await sirannon.open('snapdb', join(tempDir, 'snap.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)')
  await db.execute("INSERT INTO notes (id, body) VALUES (1, 'public')")
  await db.execute('INSERT INTO ledger (id, amount) VALUES (1, 500)')
  server = createServer(sirannon, {
    port: 0,
    authenticate: ctx => ({ role: ctx.headers.authorization === `Bearer ${READER_TOKEN}` ? 'reader' : 'guest' }),
  })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-snapshot-hook-'))
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function post<T>(path: string, body: unknown, token?: string): Promise<{ status: number; data: T }> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token === undefined ? {} : { authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(body),
  })
  return { status: response.status, data: (await response.json()) as T }
}

describe('beforeSnapshot hook', () => {
  it('receives every table the manifest would list, with the identity of the caller', async () => {
    const seen: unknown[] = []
    await start({ onBeforeSnapshot: ctx => void seen.push({ ...ctx }) })

    const { status } = await post('/db/snapdb/snapshot', {}, READER_TOKEN)

    expect(status).toBe(200)
    expect(seen).toEqual([
      { databaseId: 'snapdb', table: 'notes', identity: { role: 'reader' } },
      { databaseId: 'snapdb', table: 'ledger', identity: { role: 'reader' } },
    ])
  })

  it('refuses the manifest when the hook denies one of its tables', async () => {
    await start({
      onBeforeSnapshot: ctx => {
        if (ctx.table === 'ledger') throw new HookDeniedError('beforeSnapshot', 'the ledger is closed')
      },
    })

    const { status, data } = await post<{ error?: { code?: string } }>('/db/snapdb/snapshot', {}, READER_TOKEN)

    expect(status).toBe(403)
    expect(data.error?.code).toBe('HOOK_DENIED')
  })

  it('refuses a page of a denied table, so a caller cannot skip the manifest', async () => {
    await start({
      onBeforeSnapshot: ctx => {
        if (ctx.table === 'ledger') throw new HookDeniedError('beforeSnapshot', 'the ledger is closed')
      },
    })

    const denied = await post<{ error?: { code?: string } }>(
      '/db/snapdb/snapshot/page',
      { table: 'ledger', limit: 10 },
      READER_TOKEN,
    )
    const allowed = await post<{ rows: unknown[] }>(
      '/db/snapdb/snapshot/page',
      { table: 'notes', limit: 10 },
      READER_TOKEN,
    )

    expect(denied.status).toBe(403)
    expect(denied.data.error?.code).toBe('HOOK_DENIED')
    expect(allowed.status).toBe(200)
    expect(allowed.data.rows).toHaveLength(1)
  })

  it('lets the hook decide on the identity alone', async () => {
    await start({
      onBeforeSnapshot: ctx => {
        const identity = ctx.identity as Reader
        if (identity.role !== 'reader') throw new HookDeniedError('beforeSnapshot', 'snapshots are for readers')
      },
    })

    const guest = await post<{ error?: { code?: string } }>('/db/snapdb/snapshot', {})
    const reader = await post('/db/snapdb/snapshot', {}, READER_TOKEN)

    expect(guest.status).toBe(403)
    expect(guest.data.error?.code).toBe('HOOK_DENIED')
    expect(reader.status).toBe(200)
  })

  it('serves the snapshot untouched when no hook is registered', async () => {
    await start({})

    const { status, data } = await post<{ tables: { name: string }[] }>('/db/snapdb/snapshot', {}, READER_TOKEN)

    expect(status).toBe(200)
    expect(data.tables.map(table => table.name)).toEqual(['notes', 'ledger'])
  })
})
