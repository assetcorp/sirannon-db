import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../../core/internal-tables.js'
import { Sirannon } from '../../core/sirannon.js'
import type { ChangeEvent } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
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
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sync-ctl-'))
  controllers = []
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

describe('SyncController push', () => {
  it('drains local writes to the server and advances the durable push cursor', async () => {
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (1, 'offline one')")
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (2, 'offline two')")

    const controller = makeController()
    await controller.start()
    await until(async () => (await serverDb.query('SELECT id FROM notes ORDER BY id')).length === 2)

    const rows = await serverDb.query<{ id: number; body: string }>('SELECT id, body FROM notes ORDER BY id')
    expect(rows).toEqual([
      { id: 1, body: 'offline one' },
      { id: 2, body: 'offline two' },
    ])

    await until(async () => (await controller.status()).pendingPushCount === 0)
    const status = await controller.status()
    expect(status.pushCaughtUp).toBe(true)
    expect(status.lastPushedSeq > 0n).toBe(true)
    expect(status.lastError).toBeNull()

    const persisted = await deviceDb.deviceSync().getPushCursor()
    expect(persisted).toBe(status.lastPushedSeq)
  })

  it('pauses pushing and resumes where it left off', async () => {
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    controller.pause()
    expect((await controller.status()).state).toBe('paused')
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (3, 'while paused')")
    await new Promise(resolve => setTimeout(resolve, 200))
    expect(await serverDb.query('SELECT id FROM notes WHERE id = 3')).toHaveLength(0)

    await controller.resume()
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 3')).length === 1)
  })

  it('does not re-push already pushed changes after a restart', async () => {
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (1, 'once')")
    const first = makeController()
    await first.start()
    await until(async () => (await serverDb.query('SELECT id FROM notes')).length === 1)
    await first.stop()

    const second = makeController()
    await second.start()
    await until(async () => (await second.status()).pushCaughtUp)
    expect((await second.status()).pendingPushCount).toBe(0)
    expect(await serverDb.query('SELECT id FROM notes')).toHaveLength(1)
  })
})

describe('SyncController pull', () => {
  it('receives server changes, suppresses its own echoes, and acks its cursor', async () => {
    const received: ChangeEvent[] = []
    const controller = makeController({ onChange: event => received.push(event) })
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (10, 'device write')")
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 10')).length === 1)
    await serverDb.execute("INSERT INTO notes (id, body) VALUES (11, 'server write')")

    await until(() => received.some(event => event.row.id === 11))
    await new Promise(resolve => setTimeout(resolve, 250))
    expect(received.some(event => event.row.id === 10)).toBe(false)

    const status = await controller.status()
    expect(status.lastPulledSeq).not.toBeNull()

    const deviceId = status.deviceId
    expect(deviceId).not.toBeNull()
    await until(async () => {
      const inspect = await driver.open(join(tempDir, 'server.db'))
      try {
        const stmt = await inspect.prepare(`SELECT device_id FROM ${DEVICE_CURSORS_TABLE}`)
        const cursors = (await stmt.all()) as { device_id: string }[]
        return cursors.some(cursor => cursor.device_id === deviceId)
      } catch {
        return false
      } finally {
        await inspect.close()
      }
    })

    await controller.stop()
    const pullState = await deviceDb.deviceSync().getPullState()
    expect(pullState).not.toBeNull()
    expect(pullState?.seq).toBe(status.lastPulledSeq)
  })

  it('writes pulled changes into the local database and commits the cursor with them', async () => {
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await serverDb.execute("INSERT INTO notes (id, body) VALUES (21, 'from server')")

    await until(async () => (await deviceDb.query('SELECT body FROM notes WHERE id = 21')).length === 1)
    const rows = (await deviceDb.query('SELECT body FROM notes WHERE id = 21')) as { body: string }[]
    expect(rows[0].body).toBe('from server')

    const pullState = await deviceDb.deviceSync().getPullState()
    expect(pullState?.seq).toBe((await controller.status()).lastPulledSeq)
  })

  it('applies a server transaction as one local transaction', async () => {
    const controller = makeController()
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await serverDb.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (31, 'first')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (32, 'second')")
    })

    await until(async () => (await deviceDb.query('SELECT id FROM notes WHERE id IN (31, 32)')).length === 2)

    const inspect = await driver.open(join(tempDir, 'device.db'))
    try {
      const stmt = await inspect.prepare(
        `SELECT row_id, tx_id FROM ${CHANGES_TABLE} WHERE table_name = 'notes' AND row_id IN ('31', '32')`,
      )
      const changeRows = (await stmt.all()) as { row_id: string; tx_id: string }[]
      expect(changeRows).toHaveLength(2)
      expect(changeRows[0].tx_id).toBe(changeRows[1].tx_id)
      expect(changeRows[0].tx_id).not.toBe('')
    } finally {
      await inspect.close()
    }
  })

  it('remembers across a restart that it still owes a resync', async () => {
    await deviceDb.deviceSync().setResyncRequired(true)

    const controller = makeController({ autoResync: false })
    await controller.start()

    expect((await controller.status()).resyncRequired).toBe(true)
  })

  it('applies a transaction spanning tables as one local transaction', async () => {
    await serverDb.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)')
    await serverDb.watch('tags')
    await deviceDb.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)')
    await deviceDb.watch('tags')

    const controller = makeController({ tables: ['notes', 'tags'] })
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await serverDb.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (51, 'note')")
      await tx.execute("INSERT INTO tags (id, label) VALUES (51, 'tag')")
    })

    await until(async () => (await deviceDb.query('SELECT id FROM notes WHERE id = 51')).length === 1)
    await until(async () => (await deviceDb.query('SELECT id FROM tags WHERE id = 51')).length === 1)

    const inspect = await driver.open(join(tempDir, 'device.db'))
    try {
      const stmt = await inspect.prepare(
        `SELECT table_name, tx_id FROM ${CHANGES_TABLE} WHERE row_id = '51' ORDER BY seq`,
      )
      const changeRows = (await stmt.all()) as { table_name: string; tx_id: string }[]
      expect(changeRows.map(row => row.table_name)).toEqual(['notes', 'tags'])
      expect(changeRows[0].tx_id).toBe(changeRows[1].tx_id)
    } finally {
      await inspect.close()
    }
  })

  it('leaves the cursor where it was when applying a pulled change fails', async () => {
    const controller = makeController({
      resolver: {
        resolve: () => {
          throw new Error('resolver refused the change')
        },
      },
    })
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (41, 'device version')")
    await until(async () => (await serverDb.query('SELECT id FROM notes WHERE id = 41')).length === 1)

    const cursorBefore = (await deviceDb.deviceSync().getPullState())?.seq ?? null
    await serverDb.execute("UPDATE notes SET body = 'server version' WHERE id = 41")

    await until(async () => (await controller.status()).lastError !== null)

    const rows = (await deviceDb.query('SELECT body FROM notes WHERE id = 41')) as { body: string }[]
    expect(rows[0].body).toBe('device version')
    expect((await deviceDb.deviceSync().getPullState())?.seq ?? null).toBe(cursorBefore)
  })
})
