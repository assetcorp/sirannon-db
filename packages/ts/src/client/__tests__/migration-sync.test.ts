import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import type { Migration } from '../../core/migrations/types.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { syncDeviceMigrations } from '../migration-sync.js'
import { WebSocketTransport } from '../transport/ws.js'
import { RemoteError } from '../types.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'
const CREATE_NOTES_SQL = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'
const ADD_FLAG_SQL = 'ALTER TABLE notes ADD COLUMN flag INTEGER'

const serverMigrations: Migration[] = [
  { version: 1, name: 'create_notes', up: CREATE_NOTES_SQL },
  { version: 2, name: 'add_flag', up: ADD_FLAG_SQL },
]

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let deviceDb: Database

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-migration-sync-'))
  sirannon = new Sirannon({ driver, migrations: serverMigrations })
  await sirannon.open('appdb', join(tempDir, 'server.db'))
  deviceSirannon = new Sirannon({ driver })
  deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))
  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await deviceSirannon.shutdown()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function syncOptions() {
  return { url: baseUrl, databaseId: 'appdb' }
}

describe('syncDeviceMigrations', () => {
  it('applies every pending migration to a fresh device', async () => {
    const result = await syncDeviceMigrations(deviceDb, syncOptions())
    expect(result).toEqual({ status: 'migrated', schemaVersion: 2, serverVersion: 2 })
    const applied = await deviceDb.appliedMigrations()
    expect(applied.map(row => row.version)).toEqual([1, 2])
    await deviceDb.execute("INSERT INTO notes (id, body, flag) VALUES (1, 'x', 1)")
  })

  it('reports in-sync when the device already matches the server', async () => {
    await deviceDb.migrate(serverMigrations)
    const result = await syncDeviceMigrations(deviceDb, syncOptions())
    expect(result.status).toBe('in-sync')
    expect(result.schemaVersion).toBe(2)
  })

  it('applies only the migrations above the device version', async () => {
    await deviceDb.migrate([serverMigrations[0]])
    const result = await syncDeviceMigrations(deviceDb, syncOptions())
    expect(result.status).toBe('migrated')
    expect((await deviceDb.appliedMigrations()).map(row => row.version)).toEqual([1, 2])
  })

  it('reports ahead when the device has migrations the server lacks', async () => {
    await deviceDb.migrate([
      ...serverMigrations,
      { version: 3, name: 'add_extra', up: 'ALTER TABLE notes ADD COLUMN extra TEXT' },
    ])
    const result = await syncDeviceMigrations(deviceDb, syncOptions())
    expect(result).toEqual({ status: 'ahead', schemaVersion: 3, serverVersion: 2 })
  })

  it('requires a resync when an applied migration differs from the server history', async () => {
    await deviceDb.migrate([
      { version: 1, name: 'create_notes', up: 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body BLOB)' },
    ])
    const result = await syncDeviceMigrations(deviceDb, syncOptions())
    expect(result.status).toBe('resync-required')
  })
})

describe('WS subscribe schema version gate', () => {
  async function subscribeWith(schemaVersion: number | undefined): Promise<void> {
    const transport = new WebSocketTransport(`ws://127.0.0.1:${server.listeningPort}/db/appdb`, {})
    try {
      await transport.subscribe('notes', undefined, () => {}, { deviceId: DEVICE, schemaVersion })
    } finally {
      transport.close()
    }
  }

  it('refuses a device subscription behind the server schema', async () => {
    await expect(subscribeWith(0)).rejects.toMatchObject({ code: 'MIGRATION_REQUIRED' })
    await expect(subscribeWith(undefined)).rejects.toMatchObject({ code: 'MIGRATION_REQUIRED' })
  })

  it('refuses a device subscription ahead of the server schema', async () => {
    await expect(subscribeWith(5)).rejects.toBeInstanceOf(RemoteError)
    await expect(subscribeWith(5)).rejects.toMatchObject({ code: 'SCHEMA_AHEAD' })
  })

  it('accepts a device subscription at the server schema version', async () => {
    await expect(subscribeWith(2)).resolves.toBeUndefined()
  })
})
