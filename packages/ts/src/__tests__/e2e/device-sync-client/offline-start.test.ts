import { afterEach, describe, expect, it, vi } from 'vitest'
import { countOf, eventually, openDevice, releaseDeviceSync, sameRows, sleep, startSyncServer } from './harness.js'

const RETRY_CAP_MS = 200

afterEach(async () => {
  vi.unstubAllGlobals()
  await releaseDeviceSync()
})

describe('a device starting without the server', () => {
  it('starts while the server is down, reports why, and syncs both ways once the server answers', async () => {
    const server = await startSyncServer()
    await server.stop()
    const device = await openDevice(server.url)
    const controller = device.sync({ maxPushRetryDelayMs: RETRY_CAP_MS })

    await controller.start()
    const offline = await controller.status()
    expect([offline.state, offline.lastError?.code, offline.serverCapabilities]).toEqual([
      'running',
      'CONNECTION_ERROR',
      null,
    ])
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'written offline')")

    await server.start()
    await eventually(() => countOf(server.path, 'notes') === 1, 'the offline write reaching the server')
    await eventually(async () => (await controller.status()).lastPulledSeq !== null, 'the first subscription')
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'written after the device connected')")
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the server write arriving')
  })

  it('makes no attempt while the runtime reports no network and retries at once when it reports the network back', async () => {
    const onlineListeners: Array<() => void> = []
    const network = { onLine: false }
    vi.stubGlobal('navigator', network)
    vi.stubGlobal('addEventListener', (type: string, listener: () => void) => {
      if (type === 'online') onlineListeners.push(listener)
    })
    vi.stubGlobal('removeEventListener', (type: string, listener: () => void) => {
      const at = onlineListeners.indexOf(listener)
      if (type === 'online' && at !== -1) onlineListeners.splice(at, 1)
    })
    const server = await startSyncServer()
    await server.stop()
    const device = await openDevice(server.url)
    const controller = device.sync({ maxPushRetryDelayMs: RETRY_CAP_MS })
    await controller.start()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'written offline')")
    await server.start()
    await sleep(RETRY_CAP_MS * 4)

    expect(countOf(server.path, 'notes')).toBe(0)
    expect(onlineListeners).toHaveLength(1)

    network.onLine = true
    for (const listener of [...onlineListeners]) listener()
    await eventually(() => countOf(server.path, 'notes') === 1, 'the push once the network returns', RETRY_CAP_MS * 2)
    await eventually(
      async () => (await controller.status()).lastPulledSeq !== null,
      'the pull once the network returns',
    )
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'pulled once the network returns')")
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the pulled row')

    await controller.stop()
    expect(onlineListeners).toHaveLength(0)
  })
})
