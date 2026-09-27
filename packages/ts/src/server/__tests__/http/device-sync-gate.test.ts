import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

const DEVICE_SYNC_ROUTES = ['changes', 'migrations', 'snapshot', 'snapshot/page']

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer | null = null
let baseUrl: string

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-device-sync-gate-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('orders', join(tempDir, 'orders.db'))
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
})

afterEach(async () => {
  await server?.close()
  server = null
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function listen(acceptDeviceSync?: boolean): Promise<void> {
  const started = createServer<unknown>(sirannon, {
    port: 0,
    ...(acceptDeviceSync === undefined ? {} : { acceptDeviceSync, authenticate: () => ({ userId: 'u_amara' }) }),
  })
  server = started
  await started.listen()
  baseUrl = `http://127.0.0.1:${started.listeningPort}`
}

async function announcedCapabilities(): Promise<string[]> {
  const body = (await (await fetch(`${baseUrl}/capabilities`)).json()) as { capabilities: string[] }
  return body.capabilities
}

describe('device sync while the server leaves it off', () => {
  it('refuses every device-sync route with 403 DEVICE_SYNC_NOT_ACCEPTED', async () => {
    await listen()

    for (const route of DEVICE_SYNC_ROUTES) {
      const res = await fetch(`${baseUrl}/db/orders/${route}`, { method: 'POST', body: '{}' })
      expect(res.status, route).toBe(403)
      const body = (await res.json()) as { error: { code: string } }
      expect(body.error.code, route).toBe('DEVICE_SYNC_NOT_ACCEPTED')
    }
  })

  it('announces no device-sync capability', async () => {
    await listen()

    expect((await announcedCapabilities()).filter(name => name.startsWith('sync.'))).toEqual([])
  })
})

describe('device sync once the server turns it on', () => {
  it('announces the device-sync capabilities', async () => {
    await listen(true)

    expect(await announcedCapabilities()).toContain('sync.push')
  })

  it('refuses to start without an authenticate hook to name the caller', () => {
    let refusal: unknown
    try {
      createServer(sirannon, { acceptDeviceSync: true })
    } catch (err) {
      refusal = err
    }

    expect(refusal).toBeInstanceOf(SirannonError)
    expect((refusal as SirannonError).code).toBe('INVALID_DEVICE_SYNC')
  })
})
