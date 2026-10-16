import { afterEach, describe, expect, it } from 'vitest'
import { RawDeviceClient } from '../lib/raw-device-client.js'
import { postJson } from '../lib/served-registry.js'
import {
  changeLog,
  pause,
  readFile,
  releaseChangeStreams,
  type ServedDatabase,
  serveDatabase,
  WireSocket,
} from './harness.js'

const NOTES_SCHEMA = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)'
const SMALL_LIMITS = { maxBodyBytes: 65_536, maxWebSocketBackpressureBytes: 131_072 }
const FLOOD_ROWS = 400
const FLOOD_BODY_BYTES = 32_768
const PACE_MS = 8
const MAX_RESUME_ATTEMPTS = 100
const RESUME_POLL_MS = 20
const LARGE_REPLIES = 3
const LARGE_TRANSACTION_ROWS = 3_000
const EVENTS_BEFORE_THE_DROP = 100
const CLOSE_DEADLINE_MS = 20_000
const DELIVERY_DEADLINE_MS = 30_000
const WS_CLOSE_OVERLOADED = 4290
const OVERSIZED_REPLY_ROWS = 32
const PILED_UP_REPLIES = 40
const PILE_UP_MS = 500
const EVERY_NOTE = 'SELECT id, body FROM notes ORDER BY id'

const rawClients: RawDeviceClient[] = []

afterEach(async () => {
  for (const client of rawClients.splice(0)) client.destroy()
  await releaseChangeStreams()
})

async function connectRaw(served: ServedDatabase): Promise<RawDeviceClient> {
  const client = await RawDeviceClient.connect(served.served.port, 'notes')
  rawClients.push(client)
  return client
}

async function stalledReaders(served: ServedDatabase): Promise<{
  subscriber: RawDeviceClient
  requester: RawDeviceClient
  subscribed: Record<string, unknown>
}> {
  const subscriber = await connectRaw(served)
  const requester = await connectRaw(served)
  const subscribed = await subscriber.request({ type: 'subscribe', id: 'stalled', table: 'notes' })
  subscriber.pauseReading()
  requester.pauseReading()
  const body = 'x'.repeat(FLOOD_BODY_BYTES)
  for (let row = 0; row < FLOOD_ROWS; row++) {
    await served.db.execute('INSERT INTO notes (body) VALUES (?)', [body])
    await pause(PACE_MS)
  }
  for (let reply = 0; reply < LARGE_REPLIES; reply++) {
    requester.send({ type: 'query', id: `everything-${reply}`, sql: 'SELECT id, body FROM notes ORDER BY id' })
  }
  return { subscriber, requester, subscribed }
}

async function resumeUntilCaughtUp(
  served: ServedDatabase,
  epoch: string,
  cursor: string,
  missing: number,
): Promise<string[]> {
  const received: string[] = []
  let from = cursor
  for (let attempt = 0; attempt < MAX_RESUME_ATTEMPTS && received.length < missing; attempt++) {
    const socket = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    await socket.request({ id: 'resumed', type: 'subscribe', table: 'notes', sinceSeq: from, epoch })
    const closed = socket.closed.then(() => true)
    while (received.length + socket.events('resumed').length < missing) {
      if (await Promise.race([closed, pause(RESUME_POLL_MS).then(() => false)])) break
    }
    const seqs = socket.events('resumed').map(event => event.seq)
    socket.close()
    received.push(...seqs)
    from = seqs[seqs.length - 1] ?? from
  }
  return received
}

describe('a reader that stops reading', () => {
  it('loses its connection, whether its backlog is change events or replies, and resuming from its last change, as often as it takes, delivers the rest exactly once', async () => {
    const served = await serveDatabase({
      label: 'stalled-reader',
      databaseId: 'notes',
      schema: [NOTES_SCHEMA],
      server: SMALL_LIMITS,
    })
    const healthy = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    await healthy.request({ id: 'healthy', type: 'subscribe', table: 'notes' })
    const { subscriber, requester, subscribed } = await stalledReaders(served)
    await healthy.waitForEvents('healthy', FLOOD_ROWS, DELIVERY_DEADLINE_MS)

    subscriber.resumeReading()
    requester.resumeReading()
    await subscriber.waitForClose(CLOSE_DEADLINE_MS)
    await requester.waitForClose(CLOSE_DEADLINE_MS)
    const log = changeLog(served.path, 'notes').map(row => row.seq.toString())
    const before = subscriber.eventsFor('stalled').map(event => event.seq.toString())
    const after = await resumeUntilCaughtUp(
      served,
      String(subscribed.epoch),
      before[before.length - 1] ?? String(subscribed.seq),
      log.length - before.length,
    )
    const count = await healthy.request({ id: 'count', type: 'query', sql: 'SELECT count(*) AS n FROM notes' })

    expect(log).toHaveLength(FLOOD_ROWS)
    expect(before.length).toBeLessThan(FLOOD_ROWS)
    expect(before).toEqual(log.slice(0, before.length))
    expect([...before, ...after]).toEqual(log)
    expect(requester.messages.filter(message => message.type === 'result').length).toBeLessThan(LARGE_REPLIES)
    expect(healthy.events('healthy').map(event => event.seq)).toEqual(log)
    expect(healthy.isOpen).toBe(true)
    expect(count.data).toEqual({ rows: [{ n: FLOOD_ROWS }] })
  })

  it('is closed with code 4290 so that it can tell an overload from a network failure', async () => {
    const served = await serveDatabase({
      label: 'overload-close-code',
      databaseId: 'notes',
      schema: [NOTES_SCHEMA],
      server: SMALL_LIMITS,
    })
    const { subscriber, requester } = await stalledReaders(served)

    subscriber.resumeReading()
    requester.resumeReading()
    await subscriber.waitForClose(CLOSE_DEADLINE_MS)
    await requester.waitForClose(CLOSE_DEADLINE_MS)

    expect([subscriber.closeCode, requester.closeCode]).toEqual([WS_CLOSE_OVERLOADED, WS_CLOSE_OVERLOADED])
  })

  it('accepts a reply larger than the bound onto an empty buffer, and closes with 4290 only once replies pile up behind a reader that stops', async () => {
    const served = await serveDatabase({
      label: 'oversized-reply',
      databaseId: 'notes',
      schema: [NOTES_SCHEMA],
      server: SMALL_LIMITS,
    })
    const body = 'y'.repeat(FLOOD_BODY_BYTES)
    for (let row = 0; row < OVERSIZED_REPLY_ROWS; row++) {
      await served.db.execute('INSERT INTO notes (body) VALUES (?)', [body])
    }
    const expected = {
      rows: readFile<{ id: number; body: string }>(served.path, EVERY_NOTE).map(row => ({
        id: Number(row.id),
        body: row.body,
      })),
    }
    const replyBytes = expected.rows.reduce((total, row) => total + Buffer.byteLength(row.body), 0)
    const reading = await connectRaw(served)
    const stopped = await connectRaw(served)

    const first = await reading.request({ type: 'query', id: 'first', sql: EVERY_NOTE })
    const second = await reading.request({ type: 'query', id: 'second', sql: EVERY_NOTE })
    stopped.pauseReading()
    for (let reply = 0; reply < PILED_UP_REPLIES; reply++) {
      stopped.send({ type: 'query', id: `piled-${reply}`, sql: EVERY_NOTE })
    }
    await pause(PILE_UP_MS)
    stopped.resumeReading()
    await stopped.waitForClose(CLOSE_DEADLINE_MS)
    const delivered = stopped.messages.filter(message => message.type === 'result')

    expect(replyBytes).toBeGreaterThan(SMALL_LIMITS.maxWebSocketBackpressureBytes)
    expect([first.data, second.data]).toEqual([expected, expected])
    expect(reading.closeCode).toBeNull()
    expect(stopped.closeCode).toBe(WS_CLOSE_OVERLOADED)
    expect(delivered.length).toBeGreaterThan(0)
    expect(delivered.length).toBeLessThan(PILED_UP_REPLIES)
    for (const message of delivered) expect(message.data, String(message.id)).toEqual(expected)
  })

  it('leaves the other subscribers whole when one drops in the middle of a large transaction, and its resume delivers the rest of that transaction', async () => {
    const served = await serveDatabase({
      label: 'dropped-mid-transaction',
      databaseId: 'notes',
      schema: [NOTES_SCHEMA],
    })
    const healthy = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    await healthy.request({ id: 'healthy', type: 'subscribe', table: 'notes' })
    const leaving = await connectRaw(served)
    const subscribed = await leaving.request({ type: 'subscribe', id: 'leaving', table: 'notes' })
    leaving.pauseReading()

    const answer = await postJson(served.served.base, '/db/notes/batch', {
      sql: 'INSERT INTO notes (body) VALUES (?)',
      paramsBatch: Array.from({ length: LARGE_TRANSACTION_ROWS }, (_, row) => [`row ${row}`]),
    })
    leaving.resumeReading()
    await leaving.waitForEvents('leaving', EVENTS_BEFORE_THE_DROP)
    leaving.destroy()
    const kept = leaving.eventsFor('leaving')
    await healthy.waitForEvents('healthy', LARGE_TRANSACTION_ROWS, DELIVERY_DEADLINE_MS)
    const log = changeLog(served.path, 'notes').map(row => row.seq.toString())
    const resumed = await WireSocket.open(`${served.served.wsBase}/db/notes`)
    await resumed.request({
      id: 'resumed',
      type: 'subscribe',
      table: 'notes',
      sinceSeq: kept[kept.length - 1]?.seq.toString(),
      epoch: subscribed.epoch,
    })
    const rest = await resumed.waitForEvents('resumed', LARGE_TRANSACTION_ROWS - kept.length, DELIVERY_DEADLINE_MS)
    const healthyEvents = healthy.events('healthy')

    expect(answer.status, answer.text).toBe(200)
    expect(kept.length).toBeLessThan(LARGE_TRANSACTION_ROWS)
    expect(kept.some(event => event.txEnd)).toBe(false)
    expect([...kept.map(event => event.seq.toString()), ...rest.map(event => event.seq)]).toEqual(log)
    expect(rest.map(event => event.txEnd === true)).toEqual(rest.map((_, index) => index === rest.length - 1))
    expect(healthyEvents.map(event => event.seq)).toEqual(log)
    expect(healthyEvents.filter(event => event.txEnd === true).map(event => event.seq)).toEqual([log[log.length - 1]])
    expect(healthy.isOpen).toBe(true)
  })
})
