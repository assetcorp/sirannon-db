import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import type { SnapshotProgress } from '../snapshot-loader.js'
import { SyncController, type SyncControllerOptions } from '../sync-controller.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let serverDb: Database
let deviceDb: Database
let controllers: SyncController[]

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-snap-ctl-'))
  controllers = []
  sirannon = new Sirannon({ driver })
  deviceSirannon = new Sirannon({ driver })

  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, weight INTEGER, art BLOB)')
  await serverDb.watch('notes')
  for (let i = 1; i <= 12; i++) {
    await serverDb.execute('INSERT INTO notes (id, body, weight, art) VALUES (?, ?, ?, ?)', [
      i,
      `server note ${i}`,
      9007199254740991n + BigInt(i),
      new Uint8Array([i]),
    ])
  }

  deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))
  await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, weight INTEGER, art BLOB)')
  await deviceDb.watch('notes')

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

describe('SyncController.downloadSnapshot', () => {
  it('replaces divergent local data, reports progress, and resumes syncing', async () => {
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (99, 'stale local')")
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    const progress: SnapshotProgress[] = []
    await controller.downloadSnapshot({ pageSize: 5, onProgress: event => progress.push(event) })

    const localRows = await deviceDb.query<{ id: number; weight: bigint; art: Uint8Array }>(
      'SELECT id, weight, art FROM notes ORDER BY id',
    )
    expect(localRows.map(row => row.id)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 99])
    expect(BigInt(localRows[11].weight)).toBe(9007199254740991n + 12n)
    expect(Array.from(localRows[4].art as Uint8Array)).toEqual([5])

    expect(progress.length).toBeGreaterThanOrEqual(3)
    expect(progress[progress.length - 1].loadedRows).toBe(13)
    expect(progress[progress.length - 1].totalRows).toBe(13)

    const status = await controller.status()
    expect(status.state).toBe('running')
    expect(status.resyncRequired).toBe(false)

    const pullState = await deviceDb.deviceSync().getPullState()
    expect(pullState).not.toBeNull()
    expect((pullState?.seq ?? 0n) > 0n).toBe(true)
    expect(pullState?.epoch).toMatch(/^[0-9a-f]{32}$/)

    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (200, 'post snapshot')")
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 200')).length === 1)
  })

  it('pushes pending local writes to the server before wiping', async () => {
    const controller = makeController()
    await controller.start()
    controller.pause()
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (300, 'must not be lost')")

    await controller.downloadSnapshot({ pageSize: 50 })

    expect(await serverDb.query('SELECT id FROM notes WHERE id = 300')).toHaveLength(1)
    expect(await deviceDb.query('SELECT id FROM notes WHERE id = 300')).toHaveLength(1)
  })

  it('marks resync required when a snapshot was interrupted before completion', async () => {
    const port = deviceDb.deviceSync()
    await port.beginSnapshotLoad(['notes'])
    await port.abortSnapshotLoad()

    const controller = makeController()
    await controller.start()
    expect((await controller.status()).resyncRequired).toBe(true)

    await controller.downloadSnapshot({ pageSize: 50 })
    expect((await controller.status()).resyncRequired).toBe(false)
    expect(await deviceDb.deviceSync().snapshotLoadPending()).toBe(false)
  })

  it('requires a started controller', async () => {
    const controller = makeController()
    await expect(controller.downloadSnapshot()).rejects.toThrow(/started sync controller/)
  })

  it('blocks reads and writes on the device while a snapshot load is incomplete', async () => {
    const port = deviceDb.deviceSync()
    await port.beginSnapshotLoad(['notes'])

    await expect(deviceDb.query('SELECT 1 FROM notes')).rejects.toThrow(/sync snapshot/)
    await expect(deviceDb.execute("INSERT INTO notes (id, body) VALUES (500, 'x')")).rejects.toThrow(/sync snapshot/)

    await port.loadSnapshotPage('notes', [{ id: 1, body: 'server' }])
    await port.endSnapshotLoad(['notes'])
    expect(await deviceDb.query('SELECT id FROM notes')).toHaveLength(1)
  })

  it('keeps the device blocked after reopening with an interrupted load marker', async () => {
    const port = deviceDb.deviceSync()
    await port.beginSnapshotLoad(['notes'])
    await port.abortSnapshotLoad()
    await deviceDb.close()

    deviceDb = await deviceSirannon.open('appdb2', join(tempDir, 'device.db'))
    await expect(deviceDb.query('SELECT 1 FROM notes')).rejects.toThrow(/sync snapshot/)
    expect(await deviceDb.deviceSync().snapshotLoadPending()).toBe(true)
  })

  it('automatically resyncs an interrupted snapshot load', async () => {
    const port = deviceDb.deviceSync()
    await port.beginSnapshotLoad(['notes'])
    await port.abortSnapshotLoad()

    const controller = makeController({ autoResync: true, snapshotRetryDelayMs: 50 })
    await controller.start()

    await until(async () => {
      const status = await controller.status()
      return !status.resyncRequired && status.state === 'running'
    })
    expect(await deviceDb.deviceSync().snapshotLoadPending()).toBe(false)
    expect(await deviceDb.query('SELECT id FROM notes')).toHaveLength(12)
  })
})
