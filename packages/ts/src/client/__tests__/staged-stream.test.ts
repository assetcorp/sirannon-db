import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import { STAGED_CHANGES_TABLE } from '../../core/internal-tables.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SyncController, type SyncControllerOptions } from '../sync-controller.js'

const driver = betterSqlite3()
const WINDOW = 20
const HUGE_ROWS = 200

let tempDir: string
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let serverDb: Database
let deviceDb: Database
let devicePath: string
let controllers: SyncController[]

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-staged-stream-'))
  controllers = []
  sirannon = new Sirannon({ driver })
  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await serverDb.watch('notes')

  deviceSirannon = new Sirannon({ driver })
  devicePath = join(tempDir, 'device.db')
  deviceDb = await deviceSirannon.open('appdb', devicePath)
  await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await deviceDb.watch('notes')

  server = createServer(sirannon, { acceptSql: true, port: 0, maxUnacknowledgedChanges: WINDOW })
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
    pushIntervalMs: 25,
    ackIntervalMs: 25,
    autoResync: false,
    ...overrides,
  })
  controllers.push(controller)
  return controller
}

async function until(predicate: () => boolean | Promise<boolean>, timeout = 15_000): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

async function deviceNoteCount(): Promise<number> {
  const rows = await deviceDb.query<{ n: number }>('SELECT COUNT(*) AS n FROM notes')
  return Number(rows[0].n)
}

async function deviceStagedCount(): Promise<number> {
  const inspect = await driver.open(devicePath)
  try {
    const stmt = await inspect.prepare(`SELECT COUNT(*) AS n FROM ${STAGED_CHANGES_TABLE}`)
    const row = (await stmt.get()) as { n: number | bigint } | undefined
    return row === undefined ? 0 : Number(row.n)
  } finally {
    await inspect.close()
  }
}

async function writeHugeServerTransaction(rows: number, offset = 0): Promise<void> {
  await serverDb.transaction(async tx => {
    for (let i = 1; i <= rows; i++) {
      await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [offset + i, `bulk row ${offset + i}`])
    }
  })
}

describe('staged device stream', () => {
  it('converges a transaction ten times the delivery window and leaves staging empty', async () => {
    const controller = makeController()
    await controller.start()

    await writeHugeServerTransaction(HUGE_ROWS)

    await until(async () => (await deviceNoteCount()) === HUGE_ROWS)
    await until(async () => (await deviceStagedCount()) === 0)
    const [edge] = await deviceDb.query<{ body: string }>('SELECT body FROM notes WHERE id = ?', [HUGE_ROWS])
    expect(edge.body).toBe(`bulk row ${HUGE_ROWS}`)
  })

  it('stages a paused transaction on disk without showing any of it, then a restart finishes it', async () => {
    const silent = makeController({ immediateAckAfterChanges: 1_000_000_000, ackIntervalMs: 600_000 })
    await silent.start()

    await writeHugeServerTransaction(HUGE_ROWS)

    await until(async () => (await deviceStagedCount()) >= 5)
    expect(await deviceStagedCount()).toBeLessThan(HUGE_ROWS)
    expect(await deviceNoteCount()).toBe(0)

    await silent.stop()
    expect(await deviceNoteCount()).toBe(0)

    const resumed = makeController()
    await resumed.start()

    await until(async () => (await deviceNoteCount()) === HUGE_ROWS)
    await until(async () => (await deviceStagedCount()) === 0)
  })

  it('keeps small transactions flowing around a huge one', async () => {
    const controller = makeController()
    await controller.start()

    await serverDb.execute("INSERT INTO notes (id, body) VALUES (1000, 'before the bulk write')")
    await writeHugeServerTransaction(HUGE_ROWS, 2000)
    await serverDb.execute("INSERT INTO notes (id, body) VALUES (3000, 'after the bulk write')")

    await until(async () => (await deviceNoteCount()) === HUGE_ROWS + 2)
    const ids = await deviceDb.query<{ id: number }>('SELECT id FROM notes WHERE id IN (1000, 3000) ORDER BY id')
    expect(ids.map(row => Number(row.id))).toEqual([1000, 3000])
  })

  it('accepts local writes while a pulled transaction is paused in staging', async () => {
    const silent = makeController({ immediateAckAfterChanges: 1_000_000_000, ackIntervalMs: 600_000 })
    await silent.start()

    await writeHugeServerTransaction(HUGE_ROWS)
    await until(async () => (await deviceStagedCount()) >= 5)

    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (5000, 'written on the device')")
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 5000')).length === 1)

    expect(await deviceNoteCount()).toBe(1)
  })
})
