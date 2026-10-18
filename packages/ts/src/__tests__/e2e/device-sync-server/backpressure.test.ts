import { afterEach, describe, expect, it } from 'vitest'
import type { RawWireEvent } from '../lib/raw-device-client.js'
import { releaseServedRegistries, scratchDirectory } from '../lib/served-registry.js'
import { DEVICE, type DeviceServer, destroyClients, pause, startDeviceServer, strictlyRising } from './harness.js'

const CONGESTION_BODY_BYTES = 8_192
const KERNEL_AND_SOCKET_BUFFER_CEILING_BYTES = 17 * 1_048_576
const CONGESTED_SERVER = { maxBodyBytes: 65_536, maxWebSocketBackpressureBytes: 131_072 }

const servers: DeviceServer[] = []

afterEach(async () => {
  for (const server of servers.splice(0)) destroyClients(server)
  await releaseServedRegistries()
})

async function startStreamServer(
  window: number,
  options: { congested?: boolean; directory?: string } = {},
): Promise<DeviceServer> {
  const server = await startDeviceServer({
    directory: options.directory,
    server: {
      acceptSql: true,
      authenticate: () => ({ userId: 'u_amara' }),
      maxUnacknowledgedChanges: window,
      ...(options.congested === true ? CONGESTED_SERVER : {}),
    },
  })
  servers.push(server)
  return server
}

async function writeRows(server: DeviceServer, rows: number, bodyBytes: number, offset = 0): Promise<void> {
  const body = 'x'.repeat(bodyBytes)
  await server.db.transaction(async tx => {
    for (let id = 1; id <= rows; id++)
      await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [offset + id, body])
  })
}

function expectWholeTransaction(events: RawWireEvent[], rows: number): void {
  expect(events.length).toBe(rows)
  expect(strictlyRising(events.map(event => event.seq))).toBe(true)
  expect(new Set(events.map(event => Number(event.row.id))).size).toBe(rows)
  expect(events[events.length - 1].txEnd).toBe(true)
  expect(events.slice(0, -1).some(event => event.txEnd)).toBe(false)
}

describe('device streams over a socket that stops reading', () => {
  it('pauses a staged stream on real socket backpressure and resumes on the drain without loss or duplication', async () => {
    const rows = 3_000
    expect(rows * CONGESTION_BODY_BYTES).toBeGreaterThan(KERNEL_AND_SOCKET_BUFFER_CEILING_BYTES)
    const server = await startStreamServer(100_000, { congested: true })
    const client = await server.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, stagedStream: true })

    client.pauseReading()
    await writeRows(server, rows, CONGESTION_BODY_BYTES)
    await pause(2_000)
    expect(client.closeCode).toBeNull()
    client.resumeReading()
    await client.waitForEvents('s1', rows, 60_000)

    expectWholeTransaction(client.eventsFor('s1'), rows)
    expect(client.frames.every(frame => frame.kind === 'changes' && frame.bytes <= 66_000)).toBe(true)
    expect([client.closeCode, client.socketClosed]).toEqual([null, false])
  }, 90_000)

  it('delivers the tail of a staged stream through a reader that stalls and resumes again and again', async () => {
    const rows = 2_000
    const server = await startStreamServer(100_000, { congested: true })
    const client = await server.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, stagedStream: true })

    client.pauseReading()
    await writeRows(server, rows, CONGESTION_BODY_BYTES)
    await pause(300)
    client.resumeReading()
    const flap = setInterval(() => {
      client.pauseReading()
      setTimeout(() => client.resumeReading(), 3)
    }, 7)
    try {
      await client.waitForEvents('s1', rows, 30_000)
    } finally {
      clearInterval(flap)
      client.resumeReading()
    }

    expectWholeTransaction(client.eventsFor('s1'), rows)
    expect(client.closeCode).toBeNull()
  }, 60_000)

  it('streams a whole transaction to a per-transaction device through backpressure, with no acknowledgement and no overload close', async () => {
    const rows = 2_500
    expect(rows * CONGESTION_BODY_BYTES).toBeGreaterThan(KERNEL_AND_SOCKET_BUFFER_CEILING_BYTES)
    const server = await startStreamServer(40, { congested: true })
    const client = await server.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })

    client.pauseReading()
    await writeRows(server, rows, CONGESTION_BODY_BYTES)
    await pause(2_000)
    expect(client.closeCode).toBeNull()
    client.resumeReading()
    await client.waitForEvents('s1', rows, 60_000)

    const events = client.eventsFor('s1')
    expectWholeTransaction(events, rows)
    expect(client.frames.every(frame => frame.kind === 'change')).toBe(true)
    client.sendAck(DEVICE, events[events.length - 1].seq)
    await writeRows(server, 1, 8, rows)
    await client.waitForEvents('s1', rows + 1, 15_000)
    expect([client.eventsFor('s1')[rows].txEnd, client.closeCode]).toEqual([true, null])
  }, 90_000)

  it('advances two staged subscriptions of one device on one connection through the acknowledgements they share', async () => {
    const rows = 60
    const server = await startStreamServer(20)
    const client = await server.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, stagedStream: true })
    await client.subscribeDevice({ id: 's2', tables: ['notes'], deviceId: DEVICE, stagedStream: true })

    await writeRows(server, rows, 8)
    for (let round = 0; round < 200; round++) {
      if (client.eventsFor('s1').length >= rows && client.eventsFor('s2').length >= rows) break
      const highest = client.events.reduce((max, event) => (event.seq > max ? event.seq : max), 0n)
      if (highest > 0n) client.sendAck(DEVICE, highest)
      await pause(50)
    }

    for (const id of ['s1', 's2']) expectWholeTransaction(client.eventsFor(id), rows)
    expect(client.closeCode).toBeNull()
  }, 30_000)
})

describe('device streams interrupted part-way through a transaction', () => {
  it('resumes a staged stream from the position that the device staged, without resending the start of the transaction', async () => {
    const rows = 300
    const server = await startStreamServer(100_000)
    const first = await server.connect()
    const subscribed = await first.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      stagedStream: true,
    })

    await writeRows(server, rows, 2_048)
    await first.waitForEvents('s1', 30, 15_000)
    first.pauseReading()
    const before = first.eventsFor('s1')
    expect(before.length).toBeLessThan(rows)
    const watermark = before[before.length - 1].seq
    first.destroy()

    const second = await server.connect()
    const resumed = await second.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      stagedStream: true,
      sinceSeq: watermark,
      epoch: String(subscribed.epoch),
    })
    await second.waitForEvents('s1', rows - before.length, 15_000)

    const after = second.eventsFor('s1')
    expect(resumed.resync).toBeUndefined()
    expect(after.length).toBe(rows - before.length)
    expect(after[0].seq).toBe(watermark + 1n)
    expect(strictlyRising(after.map(event => event.seq))).toBe(true)
    expect(after[after.length - 1].txEnd).toBe(true)
    expect(new Set([...before, ...after].map(event => Number(event.row.id))).size).toBe(rows)
  }, 30_000)

  it('sends a per-transaction device the whole transaction again when it resumes from the last transaction it applied', async () => {
    const rows = 400
    const server = await startStreamServer(100_000)
    const first = await server.connect()
    const subscribed = await first.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })
    await writeRows(server, 1, 8, 10_000)
    await first.waitForEvents('s1', 1)
    const applied = first.eventsFor('s1')[0].seq

    await writeRows(server, rows, 2_048)
    await first.waitForEvents('s1', 20, 15_000)
    first.destroy()
    const second = await server.connect()
    await second.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      sinceSeq: applied,
      epoch: String(subscribed.epoch),
    })
    await second.waitForEvents('s1', rows, 15_000)

    expectWholeTransaction(second.eventsFor('s1'), rows)
    expect(second.eventsFor('s1')[0].seq).toBe(applied + 1n)
  }, 30_000)

  it('finishes a transaction after the server restarts in the middle of streaming it', async () => {
    const rows = 300
    const directory = scratchDirectory('device-stream-restart')
    const before = await startStreamServer(100_000, { directory })
    const first = await before.connect()
    const subscribed = await first.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      stagedStream: true,
    })
    await writeRows(before, rows, 2_048)
    await first.waitForEvents('s1', 30, 15_000)
    first.pauseReading()
    const received = first.eventsFor('s1')
    const watermark = received[received.length - 1].seq
    first.destroy()
    await before.server.close()
    await before.sirannon.shutdown()

    const after = await startStreamServer(100_000, { directory })
    const second = await after.connect()
    const resumed = await second.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      stagedStream: true,
      sinceSeq: watermark,
      epoch: String(subscribed.epoch),
    })
    await second.waitForEvents('s1', rows - received.length, 15_000)

    const rest = second.eventsFor('s1')
    expect([resumed.resync, resumed.epoch]).toEqual([undefined, subscribed.epoch])
    expect(rest[0].seq).toBe(watermark + 1n)
    expect(rest[rest.length - 1].txEnd).toBe(true)
    expect(new Set([...received, ...rest].map(event => Number(event.row.id))).size).toBe(rows)
    expect(received.length + rest.length).toBe(rows)
  }, 30_000)
})
