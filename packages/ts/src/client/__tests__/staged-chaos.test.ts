import { mkdtempSync, rmSync } from 'node:fs'
import net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import { STAGED_CHANGES_TABLE } from '../../core/internal-tables.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SyncController } from '../sync-controller.js'

const driver = betterSqlite3()
const WINDOW = 20
const DEVICE_SEED_ROWS = 200
const SERVER_BULK_ROWS = 1_500
const STAGED_ROWS_BEFORE_FIRST_KILL = SERVER_BULK_ROWS / 4
const LATER_KILL_DELAYS_MS = [300, 150, 500, 300]

class ChaosProxy {
  private server: net.Server | null = null
  private readonly sockets = new Set<net.Socket>()
  port = 0

  constructor(private readonly targetPort: number) {}

  listen(): Promise<void> {
    const server = net.createServer(clientSocket => {
      const upstream = net.connect(this.targetPort, '127.0.0.1')
      this.sockets.add(clientSocket)
      this.sockets.add(upstream)
      clientSocket.pipe(upstream)
      upstream.pipe(clientSocket)
      const drop = () => {
        this.sockets.delete(clientSocket)
        this.sockets.delete(upstream)
        clientSocket.destroy()
        upstream.destroy()
      }
      clientSocket.on('close', drop)
      upstream.on('close', drop)
      clientSocket.on('error', () => {})
      upstream.on('error', () => {})
    })
    this.server = server
    return new Promise(resolve => {
      server.listen(0, '127.0.0.1', () => {
        const address = server.address()
        if (address !== null && typeof address === 'object') {
          this.port = address.port
        }
        resolve()
      })
    })
  }

  killAllConnections(): number {
    const killed = this.sockets.size
    for (const socket of [...this.sockets]) {
      socket.destroy()
    }
    this.sockets.clear()
    return killed
  }

  close(): Promise<void> {
    this.killAllConnections()
    const server = this.server
    if (server === null) return Promise.resolve()
    return new Promise(resolve => server.close(() => resolve()))
  }
}

let tempDir: string
let sirannon: Sirannon
let deviceSirannon: Sirannon
let server: SirannonServer
let proxy: ChaosProxy
let serverDb: Database
let deviceDb: Database
let devicePath: string
let controller: SyncController | null = null

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-staged-chaos-'))
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
  proxy = new ChaosProxy(server.listeningPort)
  await proxy.listen()
})

afterEach(async () => {
  await controller?.stop()
  controller = null
  await proxy.close()
  await server.close()
  await deviceSirannon.shutdown()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function until(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start >= timeoutMs) throw new Error('condition never became true')
    await sleep(25)
  }
}

async function untilBulkPartlyStaged(timeoutMs: number): Promise<void> {
  const start = Date.now()
  while ((await stagedCount()) < STAGED_ROWS_BEFORE_FIRST_KILL) {
    if (Date.now() - start >= timeoutMs) {
      throw new Error(
        `the bulk transaction never reached ${STAGED_ROWS_BEFORE_FIRST_KILL} staged rows, so no kill could land part-way through it`,
      )
    }
    await sleep(1)
  }
}

async function tableRows(db: Database): Promise<Array<{ id: number; body: string }>> {
  const rows = await db.query<{ id: number | bigint; body: string }>('SELECT id, body FROM notes ORDER BY id')
  return rows.map(row => ({ id: Number(row.id), body: row.body }))
}

async function stagedCount(): Promise<number> {
  const inspect = await driver.open(devicePath)
  try {
    const stmt = await inspect.prepare(`SELECT COUNT(*) AS n FROM ${STAGED_CHANGES_TABLE}`)
    const row = (await stmt.get()) as { n: number | bigint } | undefined
    return row === undefined ? 0 : Number(row.n)
  } finally {
    await inspect.close()
  }
}

describe('staged pull under connection chaos', () => {
  it('converges byte-identically across repeated connection kills inside a bulk transaction over an echo-heavy log', async () => {
    controller = new SyncController(deviceDb, {
      url: `http://127.0.0.1:${proxy.port}`,
      databaseId: 'appdb',
      tables: ['notes'],
      pushIntervalMs: 25,
      ackIntervalMs: 25,
      autoResync: false,
    })
    await controller.start()

    await deviceDb.transaction(async tx => {
      for (let i = 1; i <= DEVICE_SEED_ROWS; i++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [i, `device seed ${i}`])
      }
    })
    await until(async () => {
      const [row] = await serverDb.query<{ n: number | bigint }>('SELECT COUNT(*) AS n FROM notes')
      return Number(row.n) === DEVICE_SEED_ROWS
    }, 30_000)

    const body = 'y'.repeat(1_024)
    await serverDb.transaction(async tx => {
      for (let i = 1; i <= SERVER_BULK_ROWS; i++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [10_000 + i, body])
      }
    })

    await untilBulkPartlyStaged(30_000)
    proxy.killAllConnections()

    for (const [round, delay] of LATER_KILL_DELAYS_MS.entries()) {
      await sleep(delay)
      proxy.killAllConnections()
      if (round === 1) {
        await deviceDb.execute("INSERT INTO notes (id, body) VALUES (20001, 'written mid-chaos')")
      }
    }

    await until(async () => {
      const [serverRows, deviceRows] = await Promise.all([tableRows(serverDb), tableRows(deviceDb)])
      return (
        serverRows.length === DEVICE_SEED_ROWS + SERVER_BULK_ROWS + 1 &&
        deviceRows.length === serverRows.length &&
        JSON.stringify(serverRows) === JSON.stringify(deviceRows)
      )
    }, 60_000)

    await until(async () => (await stagedCount()) === 0, 30_000)

    const [serverRows, deviceRows] = await Promise.all([tableRows(serverDb), tableRows(deviceDb)])
    expect(deviceRows).toEqual(serverRows)
    expect(serverRows.length).toBe(DEVICE_SEED_ROWS + SERVER_BULK_ROWS + 1)
  }, 120_000)
})
