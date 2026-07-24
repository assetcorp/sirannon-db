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
import { SyncController, type SyncControllerOptions } from '../sync-controller.js'

const CREATE_NOTES_SQL = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'
const ADD_FLAG_SQL = 'ALTER TABLE notes ADD COLUMN flag INTEGER'

const driver = betterSqlite3()

let tempDir: string
let serverMigrations: Migration[]
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let serverDb: Database
let deviceDb: Database
let controllers: SyncController[]

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-mig-ctl-'))
  controllers = []
  serverMigrations = [
    { version: 1, name: 'create_notes', up: CREATE_NOTES_SQL },
    { version: 2, name: 'add_flag', up: ADD_FLAG_SQL },
  ]
  sirannon = new Sirannon({ driver, migrations: serverMigrations })
  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.watch('notes')

  deviceSirannon = new Sirannon({ driver })
  deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  for (const controller of controllers) {
    await controller.stop()
  }
  await server.close()
  await deviceSirannon.shutdown()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function makeController(overrides?: Partial<SyncControllerOptions>): SyncController {
  const controller = new SyncController(deviceDb, {
    url: baseUrl,
    databaseId: 'appdb',
    tables: ['notes'],
    pushIntervalMs: 50,
    ackIntervalMs: 50,
    autoResync: false,
    ...overrides,
  })
  controllers.push(controller)
  return controller
}

async function until(predicate: () => boolean | Promise<boolean>, timeout = 5000): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

describe('SyncController migration handshake', () => {
  it('brings a stale device to the server schema on start and pushes under it', async () => {
    await deviceDb.migrate([serverMigrations[0]])
    await deviceDb.watch('notes')

    const controller = makeController()
    await controller.start()

    const status = await controller.status()
    expect(status.schemaVersion).toBe(2)
    expect((await deviceDb.appliedMigrations()).map(row => row.version)).toEqual([1, 2])

    await deviceDb.execute("INSERT INTO notes (id, body, flag) VALUES (7, 'stale device write', 1)")
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 7')).length === 1)
    const [row] = await serverDb.query<{ flag: number }>('SELECT flag FROM notes WHERE id = 7')
    expect(row.flag).toBe(1)
  })

  it('recovers mid-run when the server schema moves ahead', async () => {
    await deviceDb.migrate(serverMigrations)
    await deviceDb.watch('notes')
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    const addExtra: Migration = { version: 3, name: 'add_extra', up: 'ALTER TABLE notes ADD COLUMN extra TEXT' }
    serverMigrations.push(addExtra)
    await serverDb.migrate(serverMigrations)

    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (8, 'written while behind')")
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 8')).length === 1)
    expect((await controller.status()).schemaVersion).toBe(3)
    expect((await deviceDb.appliedMigrations()).map(row => row.version)).toEqual([1, 2, 3])
  })

  it('self-heals a divergent migration history through a snapshot resync', async () => {
    await deviceDb.migrate([
      { version: 1, name: 'create_notes', up: 'CREATE TABLE notes (id INTEGER PRIMARY KEY, other TEXT)' },
    ])
    await deviceDb.watch('notes')
    await serverDb.execute("INSERT INTO notes (id, body, flag) VALUES (1, 'server truth', 0)")

    let resyncSignalled = false
    const controller = makeController({
      autoResync: true,
      snapshotRetryDelayMs: 50,
      onResyncRequired: () => {
        resyncSignalled = true
      },
    })
    await controller.start()

    await until(async () => {
      const status = await controller.status()
      return status.state === 'running' && !status.resyncRequired
    })
    expect(resyncSignalled).toBe(true)

    const applied = await deviceDb.appliedMigrations()
    expect(applied.map(row => ({ version: row.version, name: row.name }))).toEqual([
      { version: 1, name: 'create_notes' },
      { version: 2, name: 'add_flag' },
    ])
    expect((await controller.status()).schemaVersion).toBe(2)
    expect(await deviceDb.query('SELECT id FROM notes WHERE id = 1')).toHaveLength(1)
  })
})
