import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server.js'
import { createServer } from '../../server.js'
import { RawDeviceClient } from './raw-device-client.js'

const DEVICE_ID = 'c'.repeat(32)
const CONGESTION_BODY_BYTES = 8_192
const KERNEL_PLUS_UWS_CEILING_BYTES = 17 * 1_048_576

let tempDir: string
let sirannon: Sirannon
let serverDb: Database
let server: SirannonServer | null = null
let clients: RawDeviceClient[]

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-backpressure-'))
  clients = []
  sirannon = new Sirannon({ driver: betterSqlite3() })
  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await serverDb.watch('notes')
})

afterEach(async () => {
  for (const client of clients) {
    client.destroy()
  }
  await server?.close()
  server = null
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function listen(options: { window: number; congested?: boolean }): Promise<number> {
  server = createServer(sirannon, {
    acceptSql: true,
    port: 0,
    maxUnacknowledgedChanges: options.window,
    ...(options.congested === true ? { maxBodyBytes: 65_536, maxWebSocketBackpressureBytes: 131_072 } : {}),
  })
  await server.listen()
  return server.listeningPort
}

async function connect(port: number): Promise<RawDeviceClient> {
  const client = await RawDeviceClient.connect(port, 'appdb')
  clients.push(client)
  return client
}

async function writeRows(rows: number, bodyBytes: number, offset = 0): Promise<void> {
  const body = 'x'.repeat(bodyBytes)
  await serverDb.transaction(async tx => {
    for (let i = 1; i <= rows; i++) {
      await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [offset + i, body])
    }
  })
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function assertStrictlyIncreasing(seqs: bigint[]): void {
  for (let i = 1; i < seqs.length; i++) {
    expect(seqs[i] > seqs[i - 1], `seq ${seqs[i]} at index ${i} must exceed ${seqs[i - 1]}`).toBe(true)
  }
}

describe('device streams over a congested socket', () => {
  it('pauses a staged stream on real socket backpressure and resumes through the drain event without loss or duplication', async () => {
    const rows = 3_000
    expect(rows * CONGESTION_BODY_BYTES).toBeGreaterThan(KERNEL_PLUS_UWS_CEILING_BYTES)

    const port = await listen({ window: 100_000, congested: true })
    const client = await connect(port)
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE_ID, stagedStream: true })

    client.pauseReading()
    await writeRows(rows, CONGESTION_BODY_BYTES)
    await sleep(2_000)
    expect(client.closeCode).toBeNull()

    client.resumeReading()
    await client.waitForEvents('s1', rows, 60_000)

    const events = client.eventsFor('s1')
    expect(events.length).toBe(rows)
    assertStrictlyIncreasing(events.map(event => event.seq))
    expect(new Set(events.map(event => Number(event.row.id))).size).toBe(rows)
    expect(events[events.length - 1].txEnd).toBe(true)
    expect(events.slice(0, -1).every(event => !event.txEnd)).toBe(true)
    expect(client.frames.every(frame => frame.kind === 'changes')).toBe(true)
    expect(client.frames.every(frame => frame.bytes <= 66_000)).toBe(true)
    expect(client.closeCode).toBeNull()
    expect(client.socketClosed).toBe(false)
  }, 90_000)

  it('delivers the tail of a staged stream through repeated reader stalls', async () => {
    const rows = 2_000
    const port = await listen({ window: 100_000, congested: true })
    const client = await connect(port)
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE_ID, stagedStream: true })

    client.pauseReading()
    await writeRows(rows, CONGESTION_BODY_BYTES)
    await sleep(300)
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

    const events = client.eventsFor('s1')
    expect(events.length).toBe(rows)
    assertStrictlyIncreasing(events.map(event => event.seq))
    expect(new Set(events.map(event => Number(event.row.id))).size).toBe(rows)
    expect(events[events.length - 1].txEnd).toBe(true)
    expect(client.closeCode).toBeNull()
  }, 60_000)

  it('streams a per-transaction device through mid-transaction backpressure without acknowledgements and without an overload close', async () => {
    const rows = 2_500
    expect(rows * CONGESTION_BODY_BYTES).toBeGreaterThan(KERNEL_PLUS_UWS_CEILING_BYTES)

    const port = await listen({ window: 40, congested: true })
    const client = await connect(port)
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE_ID })

    client.pauseReading()
    await writeRows(rows, CONGESTION_BODY_BYTES)
    await sleep(2_000)
    expect(client.closeCode).toBeNull()

    client.resumeReading()
    await client.waitForEvents('s1', rows, 60_000)

    const events = client.eventsFor('s1')
    expect(events.length).toBe(rows)
    assertStrictlyIncreasing(events.map(event => event.seq))
    expect(events[events.length - 1].txEnd).toBe(true)
    expect(events.slice(0, -1).every(event => !event.txEnd)).toBe(true)
    expect(client.frames.every(frame => frame.kind === 'change')).toBe(true)
    expect(client.closeCode).toBeNull()

    client.sendAck(DEVICE_ID, events[events.length - 1].seq)
    await writeRows(1, 8, rows)
    await client.waitForEvents('s1', rows + 1, 15_000)
    expect(client.eventsFor('s1')[rows].txEnd).toBe(true)
  }, 90_000)

  it('holds the next transaction of a per-transaction device at a closed window until an acknowledgement reopens it', async () => {
    const port = await listen({ window: 40 })
    const client = await connect(port)
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE_ID })

    await writeRows(150, 8)
    await client.waitForEvents('s1', 150, 15_000)

    const delivered = client.eventsFor('s1')
    expect(delivered[149].txEnd).toBe(true)
    expect(delivered.slice(0, 149).every(event => !event.txEnd)).toBe(true)

    await writeRows(1, 8, 150)
    await sleep(500)
    expect(client.eventsFor('s1').length).toBe(150)

    client.sendAck(DEVICE_ID, delivered[149].seq)
    await client.waitForEvents('s1', 151, 15_000)

    const events = client.eventsFor('s1')
    expect(events.length).toBe(151)
    expect(events[150].seq > delivered[149].seq).toBe(true)
    expect(events[150].txEnd).toBe(true)
  }, 30_000)

  it('resumes a staged stream mid-transaction from the staged watermark without re-transferring the transaction start', async () => {
    const rows = 300
    const port = await listen({ window: 100_000 })
    const first = await connect(port)
    const subscribed = await first.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE_ID,
      stagedStream: true,
    })
    const epoch = String(subscribed.epoch)

    await writeRows(rows, 2_048)
    await first.waitForEvents('s1', 30, 15_000)
    first.pauseReading()
    const beforeCrash = first.eventsFor('s1')
    expect(beforeCrash.length).toBeLessThan(rows)
    const watermark = beforeCrash[beforeCrash.length - 1].seq
    first.destroy()

    const second = await connect(port)
    const resumed = await second.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE_ID,
      stagedStream: true,
      sinceSeq: watermark,
      epoch,
    })
    expect(resumed.resync).toBeUndefined()

    await second.waitForEvents('s1', rows - beforeCrash.length, 15_000)
    const afterCrash = second.eventsFor('s1')
    expect(afterCrash.length).toBe(rows - beforeCrash.length)
    expect(afterCrash.every(event => event.seq > watermark)).toBe(true)
    assertStrictlyIncreasing(afterCrash.map(event => event.seq))
    expect(afterCrash[0].seq).toBe(watermark + 1n)
    expect(afterCrash[afterCrash.length - 1].txEnd).toBe(true)

    const union = new Set([...beforeCrash, ...afterCrash].map(event => Number(event.row.id)))
    expect(union.size).toBe(rows)
  }, 30_000)

  it('advances two staged subscriptions of one device on a shared connection through shared acknowledgements', async () => {
    const rows = 60
    const port = await listen({ window: 20 })
    const client = await connect(port)
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE_ID, stagedStream: true })
    await client.subscribeDevice({ id: 's2', tables: ['notes'], deviceId: DEVICE_ID, stagedStream: true })

    await writeRows(rows, 8)

    for (let round = 0; round < 200; round++) {
      const s1 = client.eventsFor('s1')
      const s2 = client.eventsFor('s2')
      if (s1.length >= rows && s2.length >= rows) break
      const highest = client.events.reduce((max, event) => (event.seq > max ? event.seq : max), 0n)
      if (highest > 0n) {
        client.sendAck(DEVICE_ID, highest)
      }
      await sleep(50)
    }

    for (const id of ['s1', 's2']) {
      const events = client.eventsFor(id)
      expect(events.length).toBe(rows)
      assertStrictlyIncreasing(events.map(event => event.seq))
      expect(new Set(events.map(event => Number(event.row.id))).size).toBe(rows)
    }
    expect(client.closeCode).toBeNull()
  }, 30_000)
})
