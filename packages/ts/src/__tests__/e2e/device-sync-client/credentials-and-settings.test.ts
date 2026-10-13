import { afterEach, describe, expect, it, vi } from 'vitest'
import { RequestDeniedError } from '../../../core/index.js'
import { countOf, eventually, openDevice, releaseDeviceSync, sameRows, startSyncServer } from './harness.js'

const TOKEN = 'Bearer device-token-7f3a'
const TICKET = 'sirannon.device.ticket.7f3a'

afterEach(async () => {
  vi.unstubAllGlobals()
  await releaseDeviceSync()
})

interface SeenRequest {
  upgrade: boolean
  authorization: string | undefined
  protocols: string | undefined
}

function admitting(seen: SeenRequest[], admit: (request: SeenRequest) => boolean) {
  return ({ headers }: { headers: Record<string, string | undefined> }) => {
    const request: SeenRequest = {
      upgrade: headers['sec-websocket-key'] !== undefined,
      authorization: headers.authorization,
      protocols: headers['sec-websocket-protocol'],
    }
    seen.push(request)
    if (!admit(request)) throw new RequestDeniedError(401, 'UNAUTHORIZED', 'The device credential is missing')
    return { device: 'admitted' }
  }
}

describe('a device carrying credentials', () => {
  it('sends its headers on every request and on the pull upgrade, and syncs only while they admit it', async () => {
    const seen: SeenRequest[] = []
    const server = await startSyncServer({
      options: { authenticate: admitting(seen, request => request.authorization === TOKEN) },
    })
    const device = await openDevice(server.url)
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'from the device')")

    const admitted = device.sync({ headers: { Authorization: TOKEN } })
    await admitted.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'from the server')")
    await eventually(() => sameRows(server.path, device.path, 'notes') && countOf(device.path, 'notes') === 2, 'sync')

    expect(seen.some(request => request.upgrade)).toBe(true)
    expect(seen.filter(request => request.authorization !== TOKEN)).toEqual([])
    await admitted.stop()

    const refused = device.sync({ headers: { Authorization: 'Bearer stolen' } })
    await expect(refused.start()).rejects.toMatchObject({ code: 'UNAUTHORIZED' })
    expect((await refused.status()).state).toBe('stopped')
  })

  it('offers the Sirannon subprotocol ahead of its ticket and syncs on the ticket alone', async () => {
    const seen: SeenRequest[] = []
    const server = await startSyncServer({
      options: {
        authenticate: admitting(seen, request => !request.upgrade || request.protocols === `sirannon.v1, ${TICKET}`),
      },
    })
    const device = await openDevice(server.url)
    const controller = device.sync({ webSocketProtocols: [TICKET] })
    await controller.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'reached over the ticket')")

    await eventually(() => countOf(device.path, 'notes') === 1, 'the pulled row')
    expect(seen.filter(request => request.upgrade).map(request => request.protocols)).toContain(
      `sirannon.v1, ${TICKET}`,
    )
  })
})

describe('settings that a device refuses before it starts', () => {
  it('refuses a credential that the upgrade cannot carry or a setting that stops it syncing, naming the setting and never the credential', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const secret = 'sirannon device ticket 7f3a'

    const refusals: Array<[setting: string, build: () => unknown]> = [
      ['webSocketProtocols', () => device.sync({ webSocketProtocols: [secret] })],
      ['webSocketProtocols', () => device.sync({ webSocketProtocols: ['ticket,with-comma'] })],
      ['webSocketProtocols', () => device.sync({ webSocketProtocols: [''] })],
      ['webSocketProtocols', () => device.sync({ webSocketProtocols: [TICKET, TICKET] })],
      ['tables', () => device.sync({ tables: [] })],
      ['batchSize', () => device.sync({ batchSize: 0 })],
      ['batchSize', () => device.sync({ batchSize: -1 })],
      ['batchSize', () => device.sync({ batchSize: 1_001 })],
      ['batchSize', () => device.sync({ batchSize: 2.5 })],
      ['batchSize', () => device.sync({ batchSize: Number.NaN })],
      ['snapshotPageSize', () => device.sync({ snapshotPageSize: 0 })],
      ['snapshotPageSize', () => device.sync({ snapshotPageSize: 1_001 })],
    ]
    for (const [setting, build] of refusals) {
      let caught: unknown = null
      try {
        build()
      } catch (err) {
        caught = err
      }
      expect(caught, setting).toMatchObject({ code: 'INVALID_ARGUMENT', message: expect.stringContaining(setting) })
      expect(String((caught as Error).message)).not.toContain(secret)
    }

    vi.stubGlobal('window', {})
    expect(() => device.sync({ headers: { Authorization: TOKEN } })).toThrowError(/webSocketProtocols/)
    vi.unstubAllGlobals()

    const controller = device.sync({ batchSize: 1_000, snapshotPageSize: 1_000 })
    await controller.start()
    await expect(controller.downloadSnapshot({ pageSize: 1_001 })).rejects.toMatchObject({ code: 'INVALID_ARGUMENT' })
    expect((await controller.status()).state).toBe('running')
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'still syncing')")
    await eventually(() => countOf(server.path, 'notes') === 1, 'the write after the refusals')
  })

  it('syncs both ways with no time limit on a request when the limit is zero', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'from the device')")
    const controller = device.sync({ requestTimeout: 0 })
    await controller.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'from the server')")

    await eventually(() => countOf(server.path, 'notes') === 2 && countOf(device.path, 'notes') === 2, 'both rows')
    expect((await controller.status()).lastError).toBeNull()
  })
})
