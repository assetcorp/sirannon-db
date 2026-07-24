import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { computeChecksum } from '../../../core/sync/checksum.js'
import { HLC } from '../../../core/sync/hlc.js'
import type { ReplicationChange } from '../../../core/sync/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'

const CREATE_NOTES_SQL = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'
const ADD_FLAG_SQL = 'ALTER TABLE notes ADD COLUMN flag INTEGER'

const registryMigrations: Migration[] = [
  { version: 1, name: 'create_notes', up: CREATE_NOTES_SQL },
  { version: 2, name: 'add_flag', up: ADD_FLAG_SQL },
]

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

const driver = betterSqlite3()

async function startServer(migrations: Migration[]): Promise<void> {
  sirannon = new Sirannon({ driver, migrations })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.watch('notes')
  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-migrations-'))
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function postJson(path: string, body: unknown): Promise<Response> {
  return fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

function wireBatch(): Record<string, unknown> {
  const changes: ReplicationChange[] = [
    {
      table: 'notes',
      operation: 'insert',
      rowId: '10',
      primaryKey: { id: 10 },
      hlc: new HLC(DEVICE).now(),
      txId: 'device-tx-1',
      nodeId: DEVICE,
      newData: { id: 10, body: 'pushed' },
      oldData: null,
    },
  ]
  return {
    sourceNodeId: DEVICE,
    batchId: `${DEVICE}-1-1`,
    fromSeq: '1',
    toSeq: '1',
    hlcRange: { min: changes[0].hlc, max: changes[0].hlc },
    changes,
    checksum: computeChecksum(changes),
  }
}

describe('POST /db/:id/migrations', () => {
  it('lists applied migrations and serves up SQL above the requested version', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/migrations', { after: 1 })
    expect(res.status).toBe(200)
    const body = (await res.json()) as {
      serverVersion: number
      migrations: { version: number; name: string; checksum: string | null; up?: string }[]
    }
    expect(body.serverVersion).toBe(2)
    expect(body.migrations.map(m => m.version)).toEqual([1, 2])
    expect(body.migrations[0].up).toBeUndefined()
    expect(body.migrations[1].up).toBe(ADD_FLAG_SQL)
    expect(body.migrations[1].checksum).toMatch(/^[0-9a-f]{16}$/)
  })

  it('serves every migration when the device has none', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/migrations', {})
    const body = (await res.json()) as { migrations: { up?: string }[] }
    expect(body.migrations.map(m => m.up)).toEqual([CREATE_NOTES_SQL, ADD_FLAG_SQL])
  })

  it('omits up SQL for function migrations', async () => {
    await startServer([
      registryMigrations[0],
      {
        version: 2,
        name: 'add_flag_fn',
        up: async tx => {
          await tx.execute(ADD_FLAG_SQL)
        },
      },
    ])
    const res = await postJson('/db/test/migrations', {})
    const body = (await res.json()) as { serverVersion: number; migrations: { checksum: string | null; up?: string }[] }
    expect(body.serverVersion).toBe(2)
    expect(body.migrations[1].up).toBeUndefined()
    expect(body.migrations[1].checksum).toBeNull()
  })

  it('rejects an invalid after value', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/migrations', { after: -1 })
    expect(res.status).toBe(400)
  })

  it('returns 404 for an unknown database', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/missing/migrations', {})
    expect(res.status).toBe(404)
  })
})

describe('POST /db/:id/changes schema version gate', () => {
  it('refuses a push from a device behind the server schema', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/changes', { schemaVersion: 1, batch: wireBatch() })
    expect(res.status).toBe(409)
    const body = (await res.json()) as { error: { code: string; details?: { serverVersion: number } } }
    expect(body.error.code).toBe('MIGRATION_REQUIRED')
    expect(body.error.details?.serverVersion).toBe(2)
  })

  it('treats a missing schema version as version zero', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/changes', { batch: wireBatch() })
    expect(res.status).toBe(409)
    const body = (await res.json()) as { error: { code: string } }
    expect(body.error.code).toBe('MIGRATION_REQUIRED')
  })

  it('refuses a push from a device ahead of the server schema', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/changes', { schemaVersion: 3, batch: wireBatch() })
    expect(res.status).toBe(409)
    const body = (await res.json()) as { error: { code: string } }
    expect(body.error.code).toBe('SCHEMA_AHEAD')
  })

  it('accepts a push at the server schema version', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/changes', { schemaVersion: 2, batch: wireBatch() })
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ applied: 1, skipped: 0, conflicts: 0 })
  })

  it('rejects a malformed schema version', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/changes', { schemaVersion: 1.5, batch: wireBatch() })
    expect(res.status).toBe(400)
  })
})

describe('snapshot manifest migration history', () => {
  it('carries the applied migration rows', async () => {
    await startServer(registryMigrations)
    const res = await postJson('/db/test/snapshot', {})
    expect(res.status).toBe(200)
    const body = (await res.json()) as { migrations: { version: number; name: string; checksum: string | null }[] }
    expect(body.migrations.map(m => ({ version: m.version, name: m.name }))).toEqual([
      { version: 1, name: 'create_notes' },
      { version: 2, name: 'add_flag' },
    ])
  })
})
