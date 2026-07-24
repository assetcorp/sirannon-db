import { mkdtempSync, rmSync } from 'node:fs'
import type { Server } from 'node:http'
import { createServer as createHttpServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { SERVER_CAPABILITIES } from '../../server/capabilities.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { verifyDeviceSyncCapabilities } from '../sync-capabilities.js'
import { SyncController } from '../sync-controller.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-capabilities-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

function listenOnce(handler: Parameters<typeof createHttpServer>[1]): Promise<{ url: string; close: () => void }> {
  return new Promise(resolve => {
    const stub: Server = createHttpServer(handler)
    stub.listen(0, '127.0.0.1', () => {
      const { port } = stub.address() as AddressInfo
      resolve({ url: `http://127.0.0.1:${port}`, close: () => stub.close() })
    })
  })
}

describe('GET /capabilities', () => {
  it('announces every device sync capability', async () => {
    const res = await fetch(`${baseUrl}/capabilities`)
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ capabilities: [...SERVER_CAPABILITIES] })
  })
})

describe('verifyDeviceSyncCapabilities', () => {
  it('accepts a server announcing every required capability', async () => {
    const capabilities = await verifyDeviceSyncCapabilities({ url: baseUrl })
    expect(capabilities).toEqual([...SERVER_CAPABILITIES])
  })

  it('refuses a server that predates the capabilities route', async () => {
    const stub = await listenOnce((_req, res) => {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Route not found' } }))
    })
    try {
      await expect(verifyDeviceSyncCapabilities({ url: stub.url })).rejects.toMatchObject({
        code: 'SYNC_UNSUPPORTED',
      })
    } finally {
      stub.close()
    }
  })

  it('refuses a server missing a required capability and names it', async () => {
    const stub = await listenOnce((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ capabilities: ['sync.push'] }))
    })
    try {
      await expect(verifyDeviceSyncCapabilities({ url: stub.url })).rejects.toThrow(/sync\.echo-suppression/)
    } finally {
      stub.close()
    }
  })

  it('propagates an unreachable server as a connection error, not a refusal', async () => {
    await expect(
      verifyDeviceSyncCapabilities({ url: 'http://127.0.0.1:1', requestTimeoutMs: 500 }),
    ).rejects.toMatchObject({ code: 'CONNECTION_ERROR' })
  })
})

describe('SyncController capability handshake', () => {
  it('records the server capabilities on start', async () => {
    const deviceSirannon = new Sirannon({ driver })
    const deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device.db'))
    await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await deviceDb.watch('notes')
    const controller = new SyncController(deviceDb, {
      url: baseUrl,
      databaseId: 'appdb',
      tables: ['notes'],
      autoResync: false,
    })
    try {
      await controller.start()
      expect((await controller.status()).serverCapabilities).toEqual([...SERVER_CAPABILITIES])
    } finally {
      await controller.stop()
      await deviceSirannon.shutdown()
    }
  })

  it('refuses to start against a server without device sync', async () => {
    const stub = await listenOnce((_req, res) => {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Route not found' } }))
    })
    const deviceSirannon = new Sirannon({ driver })
    const deviceDb = await deviceSirannon.open('appdb', join(tempDir, 'device2.db'))
    await deviceDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    const controller = new SyncController(deviceDb, {
      url: stub.url,
      databaseId: 'appdb',
      tables: ['notes'],
      autoResync: false,
    })
    try {
      await expect(controller.start()).rejects.toMatchObject({ code: 'SYNC_UNSUPPORTED' })
      expect((await controller.status()).state).toBe('stopped')
    } finally {
      stub.close()
      await deviceSirannon.shutdown()
    }
  })
})
