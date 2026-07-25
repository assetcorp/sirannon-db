import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SyncController, type SyncControllerOptions } from '../sync-controller.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let serverPort: number
let baseUrl: string
let serverDb: Database
let deviceDb: Database
let controllers: SyncController[]
let serverOptions: { maxUnacknowledgedChanges?: number }

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sync-chaos-'))
  controllers = []
  serverOptions = {}
  sirannon = new Sirannon({ driver })
  deviceSirannon = new Sirannon({ driver })

  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await serverDb.watch('notes')

  deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))
  await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await deviceDb.watch('notes')

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  serverPort = server.listeningPort ?? 0
  baseUrl = `http://127.0.0.1:${serverPort}`
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
    ...overrides,
  })
  controllers.push(controller)
  return controller
}

async function restartServer(): Promise<void> {
  await server.close()
  server = createServer(sirannon, { port: serverPort, ...serverOptions })
  await server.listen()
  expect(server.listeningPort).toBe(serverPort)
}

async function writePair(first: number): Promise<void> {
  await serverDb.transaction(async tx => {
    await tx.execute(`INSERT INTO notes (id, body) VALUES (${first}, 'first')`)
    await tx.execute(`INSERT INTO notes (id, body) VALUES (${first + 1}, 'second')`)
  })
}

async function until(predicate: () => boolean | Promise<boolean>, timeout = 20_000): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(resolve => setTimeout(resolve, 20))
  }
}

async function deviceRowIds(): Promise<number[]> {
  const rows = (await deviceDb.query('SELECT id FROM notes ORDER BY id')) as { id: number }[]
  return rows.map(row => row.id)
}

async function transactionIdsByRow(): Promise<Map<string, string>> {
  const inspect = await driver.open(join(tempDir, 'device.db'))
  try {
    const stmt = await inspect.prepare(
      `SELECT row_id, tx_id FROM ${CHANGES_TABLE} WHERE table_name = 'notes' ORDER BY seq`,
    )
    const rows = (await stmt.all()) as { row_id: string; tx_id: string }[]
    const byRow = new Map<string, string>()
    for (const row of rows) {
      byRow.set(row.row_id, row.tx_id)
    }
    return byRow
  } finally {
    await inspect.close()
  }
}

async function expectPairsAppliedAtomically(firstIds: readonly number[]): Promise<void> {
  const byRow = await transactionIdsByRow()
  for (const first of firstIds) {
    const left = byRow.get(String(first))
    const right = byRow.get(String(first + 1))
    expect(left).toBeDefined()
    expect(left).toBe(right)
    expect(left).not.toBe('')
  }
}

describe('device sync under connection loss', () => {
  it('loses no transaction when the connection dies at different points in delivery', async () => {
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    const dropDelaysMs = [0, 5, 15, 40]
    const firstIds = [100, 200, 300, 400]

    for (const [index, delay] of dropDelaysMs.entries()) {
      await writePair(firstIds[index])
      await new Promise(resolve => setTimeout(resolve, delay))
      await restartServer()
    }

    await until(async () => (await deviceRowIds()).length === firstIds.length * 2)

    expect(await deviceRowIds()).toEqual([100, 101, 200, 201, 300, 301, 400, 401])
    await expectPairsAppliedAtomically(firstIds)

    const pullState = await deviceDb.deviceSync().getPullState()
    expect(pullState?.seq).toBe((await controller.status()).lastPulledSeq)
  }, 60_000)

  it('loses no transaction when the server closes an overloaded connection', async () => {
    serverOptions = { maxUnacknowledgedChanges: 1 }
    await restartServer()

    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    const firstIds = [500, 600, 700]
    for (const first of firstIds) {
      await writePair(first)
    }

    await until(async () => (await deviceRowIds()).length === firstIds.length * 2)

    expect(await deviceRowIds()).toEqual([500, 501, 600, 601, 700, 701])
    await expectPairsAppliedAtomically(firstIds)
  }, 60_000)

  it('loses no change when the connection dies inside a transaction larger than one poll batch', async () => {
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    const rowCount = 1500
    await serverDb.transaction(async tx => {
      for (let id = 1; id <= rowCount; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'bulk'])
      }
    })

    await new Promise(resolve => setTimeout(resolve, 60))
    await restartServer()

    await until(async () => (await deviceRowIds()).length === rowCount)

    const byRow = await transactionIdsByRow()
    expect(byRow.size).toBe(rowCount)
    const txIds = new Set(byRow.values())
    expect(txIds.size).toBe(1)
    expect(txIds.has('')).toBe(false)
  }, 60_000)

  it('keeps syncing every table after the connection comes back', async () => {
    await serverDb.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)')
    await serverDb.watch('tags')
    await deviceDb.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)')
    await deviceDb.watch('tags')

    const controller = makeController({ tables: ['notes', 'tags'] })
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await serverDb.execute("INSERT INTO notes (id, body) VALUES (1, 'before')")
    await until(async () => (await deviceRowIds()).includes(1))

    await restartServer()

    await serverDb.execute("INSERT INTO tags (id, label) VALUES (2, 'after')")
    await serverDb.execute("INSERT INTO notes (id, body) VALUES (3, 'after')")

    await until(async () => (await deviceDb.query('SELECT id FROM tags WHERE id = 2')).length === 1)
    await until(async () => (await deviceRowIds()).includes(3))
  }, 60_000)

  it('keeps device writes made while the connection is down', async () => {
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await server.close()
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (800, 'offline write')")
    await writePair(900)

    server = createServer(sirannon, { port: serverPort })
    await server.listen()

    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 800')).length === 1)
    await until(async () => (await deviceRowIds()).includes(901))

    expect(await deviceRowIds()).toEqual([800, 900, 901])
    await expectPairsAppliedAtomically([900])
  }, 60_000)
})
