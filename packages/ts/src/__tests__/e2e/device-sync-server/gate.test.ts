import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { SirannonError } from '../../../core/errors.js'
import { createServer } from '../../../server/server.js'
import {
  errorCodeOf,
  RawSocket,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'
import {
  DATABASE_ID,
  DEVICE,
  deviceChange,
  openNotesDatabase,
  postDeviceRoute,
  push,
  startDeviceServer,
  storedNotes,
  wireBatch,
} from './harness.js'

const DEVICE_SYNC_ROUTES = ['changes', 'migrations', 'snapshot', 'snapshot/page'] as const
const REQUIRED_CAPABILITIES = [
  'sync.push',
  'sync.echo-suppression',
  'sync.ack',
  'sync.resume',
  'sync.snapshot',
  'sync.migrations',
  'sync.schema-gate',
  'sync.stream-apply',
]

afterEach(releaseServedRegistries)

async function capabilitiesOf(base: string): Promise<string[]> {
  const answer = await sendRaw(base, 'GET', '/capabilities')
  return (answer.body as { capabilities: string[] }).capabilities
}

describe('device sync while the server leaves it off', () => {
  it('refuses every device route and device message, announces no sync capability, and still serves the rest', async () => {
    const path = join(scratchDirectory('device-sync-off'), `${DATABASE_ID}.db`)
    const sirannon = trackedRegistry()
    await openNotesDatabase(sirannon, path)
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const bodies: Record<(typeof DEVICE_SYNC_ROUTES)[number], unknown> = {
      changes: { batch: wireBatch([deviceChange({ id: 1 })]) },
      migrations: {},
      snapshot: {},
      'snapshot/page': { table: 'notes' },
    }

    for (const route of DEVICE_SYNC_ROUTES) {
      const answer = await postDeviceRoute(served.base, route, bodies[route])
      expect([answer.status, errorCodeOf(answer.body)], route).toEqual([403, 'DEVICE_SYNC_NOT_ACCEPTED'])
    }
    expect((await capabilitiesOf(served.base)).filter(name => name.startsWith('sync.'))).toEqual([])

    const ws = await RawSocket.open(`${served.wsBase}/db/${DATABASE_ID}`)
    const device = await ws.request({ type: 'subscribe', id: 'device', tables: ['notes'], deviceId: DEVICE })
    const ack = await ws.request({ type: 'ack', id: 'ack', deviceId: DEVICE, seq: '1' })
    const plain = await ws.request({ type: 'subscribe', id: 'plain', table: 'notes' })
    expect([device.error?.code, ack.error?.code, plain.type]).toEqual([
      'DEVICE_SYNC_NOT_ACCEPTED',
      'DEVICE_SYNC_NOT_ACCEPTED',
      'subscribed',
    ])
    expect(storedNotes(path)).toEqual([])
    expect(ws.open).toBe(true)
  })
})

describe('device sync once the server turns it on', () => {
  it('announces every capability that the spec requires, and the staged stream', async () => {
    const server = await startDeviceServer()

    const announced = await capabilitiesOf(server.base)

    expect(announced).toEqual(expect.arrayContaining([...REQUIRED_CAPABILITIES, 'sync.staged-stream']))
  })

  it('refuses to build a server that accepts device sync without an authenticate hook', () => {
    const sirannon = trackedRegistry()
    let refusal: unknown
    try {
      createServer(sirannon, { acceptDeviceSync: true })
    } catch (err) {
      refusal = err
    }

    expect(refusal).toBeInstanceOf(SirannonError)
    expect((refusal as SirannonError).code).toBe('INVALID_DEVICE_SYNC')
  })

  it('refuses a caller that the authenticate hook rejects on every device route, and writes nothing', async () => {
    const server = await startDeviceServer()

    for (const route of DEVICE_SYNC_ROUTES) {
      const answer = await postDeviceRoute(server.base, route, { table: 'notes' }, 'stolen-token')
      expect([answer.status, errorCodeOf(answer.body)], route).toEqual([401, 'UNAUTHORIZED'])
    }
    const accepted = await push(server.base, wireBatch([deviceChange({ id: 1 })]))
    expect(accepted.status).toBe(200)
    expect(storedNotes(server.path).map(note => note.id)).toEqual([1])
  })
})
