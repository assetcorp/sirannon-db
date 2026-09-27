import { mkdtempSync, rmSync } from 'node:fs'
import { createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../core/database.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server/server.js'
import { SyncController } from '../sync-controller.js'
import { jitteredBackoff } from '../sync-reconnect.js'

const driver = betterSqlite3()
const RETRY_CAP_MS = 200
const LONGER_THAN_SEVERAL_RETRIES_MS = RETRY_CAP_MS * 4

let tempDir: string
let serverSirannon: Sirannon
let deviceSirannon: Sirannon
let serverDb: Database
let deviceDb: Database
let server: SirannonServer | null
let controller: SyncController | null
let port: number

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createNetServer()
    probe.once('error', reject)
    probe.listen(0, '127.0.0.1', () => {
      const address = probe.address()
      if (address === null || typeof address === 'string') {
        reject(new Error('The probe server reported no port'))
        return
      }
      probe.close(() => resolve(address.port))
    })
  })
}

async function startServer(): Promise<void> {
  server = createServer(serverSirannon, {
    port,
    acceptDeviceSync: true,
    authenticate: (): unknown => undefined,
  })
  await server.listen()
}

async function until(predicate: () => Promise<boolean>, timeout = 10_000): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(resolve => setTimeout(resolve, 20))
  }
}

async function serverHasNote(id: number): Promise<boolean> {
  return (await serverDb.query('SELECT id FROM notes WHERE id = ?', [id])).length === 1
}

function startController(): Promise<void> {
  controller = new SyncController(deviceDb, {
    url: `http://127.0.0.1:${port}`,
    databaseId: 'appdb',
    tables: ['notes'],
    pushIntervalMs: 50,
    ackIntervalMs: 50,
    maxPushRetryDelayMs: RETRY_CAP_MS,
  })
  return controller.start()
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-offline-start-'))
  serverSirannon = new Sirannon({ driver })
  deviceSirannon = new Sirannon({ driver })
  serverDb = await serverSirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await serverDb.watch('notes')
  deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))
  await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await deviceDb.watch('notes')
  server = null
  controller = null
  port = await freePort()
})

afterEach(async () => {
  await controller?.stop()
  vi.unstubAllGlobals()
  await server?.close()
  await deviceSirannon.shutdown()
  await serverSirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('starting device sync with the server unreachable', () => {
  it('resolves, reports the failure, and syncs both ways once the server answers', async () => {
    await startController()

    const status = await controller?.status()
    expect(status?.state).toBe('running')
    expect(status?.lastError?.code).toBe('CONNECTION_ERROR')

    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (1, 'written offline')")
    await startServer()

    await until(() => serverHasNote(1))
    await serverDb.execute("INSERT INTO notes (id, body) VALUES (2, 'written on the server')")
    await until(async () => (await deviceDb.query('SELECT id FROM notes WHERE id = 2')).length === 1)
  })

  it('makes no attempt while the device reports no network, and retries at once when it returns', async () => {
    const onlineListeners: (() => void)[] = []
    const network = { onLine: false }
    vi.stubGlobal('navigator', network)
    vi.stubGlobal('addEventListener', (type: string, listener: () => void) => {
      if (type === 'online') onlineListeners.push(listener)
    })
    vi.stubGlobal('removeEventListener', (type: string, listener: () => void) => {
      const index = onlineListeners.indexOf(listener)
      if (type === 'online' && index !== -1) onlineListeners.splice(index, 1)
    })

    await startController()
    await deviceDb.execute("INSERT INTO notes (id, body) VALUES (1, 'written offline')")
    await startServer()
    await new Promise(resolve => setTimeout(resolve, LONGER_THAN_SEVERAL_RETRIES_MS))

    expect(await serverHasNote(1)).toBe(false)
    expect(onlineListeners).toHaveLength(1)

    network.onLine = true
    for (const listener of onlineListeners) listener()

    await until(() => serverHasNote(1), RETRY_CAP_MS * 2)
  })

  it('still rejects when the server refuses device sync', async () => {
    server = createServer(serverSirannon, { port })
    await server.listen()

    await expect(startController()).rejects.toMatchObject({ code: 'DEVICE_SYNC_NOT_ACCEPTED' })
    expect((await controller?.status())?.state).toBe('stopped')
  })
})

describe('jitteredBackoff', () => {
  it('keeps every wait between half the doubled wait and the doubled wait', () => {
    expect(jitteredBackoff(100, 2, 10_000, () => 0)).toBe(200)
    expect(jitteredBackoff(100, 2, 10_000, () => 0.999_999)).toBe(400)
  })

  it('never waits longer than the cap', () => {
    expect(jitteredBackoff(100, 20, 1_000, () => 0.999_999)).toBe(1_000)
    expect(jitteredBackoff(100, 20, 1_000, () => 0)).toBe(500)
  })
})
