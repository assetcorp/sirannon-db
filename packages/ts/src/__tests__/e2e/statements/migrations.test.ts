import { createHash } from 'node:crypto'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import {
  errorCodeOf,
  postJson,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'
const CREATE_NOTES = '  CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)\n'
const ADD_FLAG = 'ALTER TABLE notes\r\n  ADD COLUMN flag INTEGER'
const ADD_PINNED = 'ALTER TABLE notes ADD COLUMN pinned INTEGER'
const FNV_OFFSET_BASIS = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n
const UINT64_MASK = 2n ** 64n - 1n
const HLC_WALL_DIGITS = 12

const SQL_MIGRATIONS: Migration[] = [
  { version: 1, name: 'create_notes', up: CREATE_NOTES },
  { version: 2, name: 'add_flag', up: ADD_FLAG },
]

interface ListedMigration {
  version: number
  name: string
  checksum: string | null
  up?: string
}

let path: string

function fnv1a64(sql: string): string {
  let hash = FNV_OFFSET_BASIS
  for (const byte of Buffer.from(sql.replace(/\r\n/g, '\n').trim(), 'utf8')) {
    hash = ((hash ^ BigInt(byte)) * FNV_PRIME) & UINT64_MASK
  }
  return hash.toString(16).padStart(16, '0')
}

function canonical(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'boolean') return String(value)
  if (typeof value === 'string') return JSON.stringify(value)
  if (typeof value === 'number') return Number.isInteger(value) ? `{"__sint":"${value}"}` : JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, member]) => member !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
  return `{${entries.map(([key, member]) => `${JSON.stringify(key)}:${canonical(member)}`).join(',')}}`
}

function pushedBatch(): Record<string, unknown> {
  const hlc = `${Date.now().toString(16).padStart(HLC_WALL_DIGITS, '0')}-0000-${DEVICE}`
  const changes = [
    {
      table: 'notes',
      operation: 'insert',
      rowId: '10',
      primaryKey: { id: 10 },
      hlc,
      txId: 'device-tx-1',
      nodeId: DEVICE,
      newData: { id: 10, body: 'pushed from a device' },
      oldData: null,
    },
  ]
  return {
    sourceNodeId: DEVICE,
    batchId: `${DEVICE}-1-1`,
    fromSeq: '1',
    toSeq: '1',
    hlcRange: { min: hlc, max: hlc },
    changes,
    checksum: createHash('sha256').update(canonical(changes)).digest('hex'),
  }
}

async function serveNotes(migrations: Migration[], acceptDeviceSync = true): Promise<ServedRegistry> {
  const sirannon = trackedRegistry({ migrations })
  const db = await sirannon.open('notes', path)
  await db.watch('notes')
  return serveRegistry(sirannon, {
    acceptSql: true,
    acceptDeviceSync,
    authenticate: () => ({ device: DEVICE }),
  })
}

function trackedMigrations(): ListedMigration[] {
  return readSqlite<{ version: bigint; name: string; checksum: string | null }>(
    path,
    'SELECT version, name, checksum FROM _sirannon_migrations ORDER BY version',
  ).map(row => ({ version: Number(row.version), name: row.name, checksum: row.checksum }))
}

beforeEach(() => {
  path = join(scratchDirectory('migrations-route'), 'notes.db')
})

afterEach(releaseServedRegistries)

describe('the migrations that a server reports to a device', () => {
  it('lists every applied migration with its checksum and serves the SQL of each one above the version that the device holds', async () => {
    const served = await serveNotes(SQL_MIGRATIONS)
    const tracked = trackedMigrations()

    const afterFirst = await postJson(served.base, '/db/notes/migrations', { after: 1 })
    const fromNothing = await postJson(served.base, '/db/notes/migrations', {})
    const upToDate = await postJson(served.base, '/db/notes/migrations', { after: 2 })
    const manifest = await postJson(served.base, '/db/notes/snapshot', {})

    expect(tracked).toEqual([
      { version: 1, name: 'create_notes', checksum: fnv1a64(CREATE_NOTES) },
      { version: 2, name: 'add_flag', checksum: fnv1a64(ADD_FLAG) },
    ])
    expect(afterFirst.body).toEqual({
      serverVersion: 2,
      migrations: [tracked[0], { ...tracked[1], up: ADD_FLAG }],
    })
    expect(fromNothing.body).toEqual({
      serverVersion: 2,
      migrations: [
        { ...tracked[0], up: CREATE_NOTES },
        { ...tracked[1], up: ADD_FLAG },
      ],
    })
    expect(upToDate.body).toEqual({ serverVersion: 2, migrations: tracked })
    expect((manifest.body as { migrations: ListedMigration[] }).migrations).toEqual(tracked)
  })

  it('lists a function migration without SQL or a checksum', async () => {
    const served = await serveNotes([
      SQL_MIGRATIONS[0] as Migration,
      {
        version: 2,
        name: 'add_pinned',
        up: async tx => {
          await tx.execute(ADD_PINNED)
        },
      },
    ])

    const listed = await postJson(served.base, '/db/notes/migrations', {})

    expect(listed.body).toEqual({
      serverVersion: 2,
      migrations: [
        { version: 1, name: 'create_notes', checksum: fnv1a64(CREATE_NOTES), up: CREATE_NOTES },
        { version: 2, name: 'add_pinned', checksum: null },
      ],
    })
    expect(trackedMigrations()[1]?.checksum).toBeNull()
  })

  it('refuses a malformed version, an unknown database, and a server that keeps device sync off', async () => {
    const served = await serveNotes(SQL_MIGRATIONS)
    for (const after of [-1, 1.5, '1', null, 2 ** 31, [1], { version: 1 }]) {
      const answer = await postJson(served.base, '/db/notes/migrations', { after })
      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(after)).toEqual([400, 'INVALID_REQUEST'])
    }
    const unknown = await postJson(served.base, '/db/drafts/migrations', {})
    expect([unknown.status, errorCodeOf(unknown.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    await served.server.close()

    path = join(scratchDirectory('migrations-route-off'), 'notes.db')
    const closed = await serveNotes(SQL_MIGRATIONS, false)
    const refused = await postJson(closed.base, '/db/notes/migrations', {})
    expect([refused.status, errorCodeOf(refused.body)]).toEqual([403, 'DEVICE_SYNC_NOT_ACCEPTED'])
  })

  it('refuses a push from a device whose schema differs from the server and applies one at the same version', async () => {
    const served = await serveNotes(SQL_MIGRATIONS)
    const pushes: Array<[schemaVersion: unknown, status: number, code: string | null]> = [
      [1, 409, 'MIGRATION_REQUIRED'],
      [undefined, 409, 'MIGRATION_REQUIRED'],
      [0, 409, 'MIGRATION_REQUIRED'],
      [3, 409, 'SCHEMA_AHEAD'],
      [1.5, 400, 'INVALID_REQUEST'],
      [-1, 400, 'INVALID_REQUEST'],
      ['2', 400, 'INVALID_REQUEST'],
    ]

    for (const [schemaVersion, status, code] of pushes) {
      const answer = await postJson(served.base, '/db/notes/changes', { schemaVersion, batch: pushedBatch() })
      expect([answer.status, errorCodeOf(answer.body)], String(schemaVersion)).toEqual([status, code])
      if (status === 409) {
        expect((answer.body as { error: { details?: unknown } }).error.details).toEqual({ serverVersion: 2 })
      }
    }
    expect(readSqlite(path, 'SELECT id FROM notes')).toEqual([])

    const accepted = await postJson(served.base, '/db/notes/changes', { schemaVersion: 2, batch: pushedBatch() })

    expect([accepted.status, accepted.body]).toEqual([200, { applied: 1, skipped: 0, conflicts: 0 }])
    expect(readSqlite(path, 'SELECT id, body, flag FROM notes')).toEqual([
      { id: 10n, body: 'pushed from a device', flag: null },
    ])
  })
})
