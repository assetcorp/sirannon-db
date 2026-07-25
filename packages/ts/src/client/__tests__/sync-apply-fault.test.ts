import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import type { OpenOptions, SQLiteConnection, SQLiteDriver } from '../../core/driver/types.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SyncController } from '../sync-controller.js'

interface WriteFault {
  armFor(sqlFragment: string, failOnCall: number): void
  armAlways(sqlFragment: string): void
  failures(): number
  disarm(): void
}

function faultInjectingDriver(base: SQLiteDriver): { driver: SQLiteDriver; fault: WriteFault } {
  let target: string | null = null
  let failOnCall = 0
  let calls = 0
  let failures = 0

  const wrapConnection = (conn: SQLiteConnection): SQLiteConnection => ({
    exec: sql => conn.exec(sql),
    close: () => conn.close(),
    transaction: fn => conn.transaction(inner => fn(wrapConnection(inner))),
    prepare: async sql => {
      const stmt = await conn.prepare(sql)
      if (target === null || !sql.includes(target)) return stmt
      return {
        ...stmt,
        run: async (...params: unknown[]) => {
          calls += 1
          if (calls === failOnCall || failOnCall === -1) {
            failures += 1
            throw new Error('simulated write failure')
          }
          return stmt.run(...params)
        },
      }
    },
  })

  return {
    driver: {
      capabilities: base.capabilities,
      open: (path: string, options?: OpenOptions) => base.open(path, options).then(wrapConnection),
    },
    fault: {
      armFor: (sqlFragment, call) => {
        target = sqlFragment
        failOnCall = call
        calls = 0
      },
      armAlways: sqlFragment => {
        target = sqlFragment
        failOnCall = -1
        calls = 0
      },
      failures: () => failures,
      disarm: () => {
        target = null
        failOnCall = 0
        calls = 0
      },
    },
  }
}

const serverDriver = betterSqlite3()
const { driver: deviceDriver, fault } = faultInjectingDriver(betterSqlite3())

let tempDir: string
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let serverDb: Database
let deviceDb: Database
let controller: SyncController

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-apply-fault-'))
  fault.disarm()
  sirannon = new Sirannon({ driver: serverDriver })
  deviceSirannon = new Sirannon({ driver: deviceDriver })

  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await serverDb.watch('notes')

  deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))
  await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await deviceDb.watch('notes')

  server = createServer(sirannon, { port: 0 })
  await server.listen()

  controller = new SyncController(deviceDb, {
    url: `http://127.0.0.1:${server.listeningPort}`,
    databaseId: 'appdb',
    tables: ['notes'],
    pushIntervalMs: 50,
    ackIntervalMs: 50,
    autoResync: false,
  })
})

afterEach(async () => {
  fault.disarm()
  await controller.stop()
  await server.close()
  await deviceSirannon.shutdown()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

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

describe('device sync when a local write fails part-way through a transaction', () => {
  it('commits none of the transaction, holds the cursor, and converges on retry', async () => {
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    const cursorBefore = (await deviceDb.deviceSync().getPullState())?.seq ?? null
    fault.armFor('INSERT INTO "notes"', 2)

    await serverDb.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (2, 'second')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (3, 'third')")
    })

    await until(async () => (await controller.status()).lastError !== null)

    expect(await deviceRowIds()).toEqual([])
    expect((await deviceDb.deviceSync().getPullState())?.seq ?? null).toBe(cursorBefore)

    fault.disarm()

    await until(async () => (await deviceRowIds()).length === 3)
    expect(await deviceRowIds()).toEqual([1, 2, 3])

    const status = await controller.status()
    expect((await deviceDb.deviceSync().getPullState())?.seq).toBe(status.lastPulledSeq)
  }, 60_000)

  it('backs off instead of retrying a failing change at full speed', async () => {
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    fault.armAlways('INSERT INTO "notes"')

    await serverDb.execute("INSERT INTO notes (id, body) VALUES (20, 'poison')")
    await until(async () => (await controller.status()).lastError !== null)

    const attemptsAtStart = fault.failures()
    await new Promise(resolve => setTimeout(resolve, 1500))
    const attemptsAdded = fault.failures() - attemptsAtStart

    expect(attemptsAdded).toBeLessThan(8)
  }, 60_000)

  it('survives a failure on the last write of a transaction', async () => {
    await controller.start()
    await until(async () => (await controller.status()).pushCaughtUp)

    fault.armFor('INSERT INTO "notes"', 3)

    await serverDb.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (10, 'first')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (11, 'second')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (12, 'third')")
    })

    await until(async () => (await controller.status()).lastError !== null)
    expect(await deviceRowIds()).toEqual([])

    fault.disarm()

    await until(async () => (await deviceRowIds()).length === 3)
    expect(await deviceRowIds()).toEqual([10, 11, 12])
  }, 60_000)
})
