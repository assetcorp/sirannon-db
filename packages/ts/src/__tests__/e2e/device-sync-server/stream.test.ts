import { afterEach, describe, expect, it } from 'vitest'
import { decodeWireRow } from '../lib/device-sync-oracle.js'
import type { RawDeviceClient } from '../lib/raw-device-client.js'
import { readSqlite, releaseServedRegistries } from '../lib/served-registry.js'
import {
  DEVICE,
  type DeviceServer,
  destroyClients,
  deviceChange,
  deviceCursors,
  OTHER_DEVICE,
  pause,
  push,
  serverChangeLog,
  serverMeta,
  startDeviceServer,
  strictlyRising,
  wireBatch,
  writeAroundSirannon,
} from './harness.js'

const QUIET_MS = 400

let server: DeviceServer | undefined

afterEach(async () => {
  destroyClients(server)
  server = undefined
  await releaseServedRegistries()
})

async function startStreamServer(window?: number): Promise<DeviceServer> {
  server = await startDeviceServer({
    server: {
      acceptSql: true,
      authenticate: () => ({ userId: 'u_amara' }),
      ...(window === undefined ? {} : { maxUnacknowledgedChanges: window }),
    },
  })
  return server
}

function errorCode(message: Record<string, unknown>): unknown {
  return (message.error as { code?: string } | undefined)?.code
}

async function ack(client: RawDeviceClient, deviceId: string, seq: unknown): Promise<Record<string, unknown>> {
  return client.request({ type: 'ack', id: client.nextId(), deviceId, seq })
}

async function expectStagedTransactionPacedByWindow(window: number): Promise<void> {
  const rows = 20
  const served = await startStreamServer(window)
  const client = await served.connect()
  await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, stagedStream: true })

  await served.db.executeBatch(
    'INSERT INTO notes (id, body) VALUES (?, ?)',
    Array.from({ length: rows }, (_, at) => [at + 1, 'x']),
  )
  await client.waitForEvents('s1', window + 1)
  await pause(QUIET_MS)
  expect(client.eventsFor('s1'), `window ${window}`).toHaveLength(window + 1)

  for (let round = 0; round < 100 && client.eventsFor('s1').length < rows; round++) {
    const events = client.eventsFor('s1')
    await ack(client, DEVICE, String(events[events.length - 1].seq))
    await pause(20)
  }
  const events = client.eventsFor('s1')
  expect(
    events.map(event => Number(event.row.id)),
    `window ${window}`,
  ).toEqual(Array.from({ length: rows }, (_, at) => at + 1))
  expect(strictlyRising(events.map(event => event.seq))).toBe(true)
  expect(events.map(event => event.txEnd).indexOf(true)).toBe(rows - 1)
  destroyClients(served)
  await releaseServedRegistries()
}

describe('the device change stream', () => {
  it('delivers every server write in log order, with origin, clock, and transaction, and marks the last change of each transaction', async () => {
    const served = await startStreamServer()
    await served.db.execute('CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT, icon BLOB, weight INTEGER)')
    await served.db.watch('tags')
    const perTransaction = await served.connect()
    const staged = await served.connect()
    const subscribed = await perTransaction.subscribeDevice({ id: 's1', tables: ['notes', 'tags'], deviceId: DEVICE })
    await staged.subscribeDevice({ id: 's2', tables: ['notes', 'tags'], deviceId: DEVICE, stagedStream: true })

    await served.db.execute("INSERT INTO notes (id, body) VALUES (1, 'alone')")
    await served.db.executeTransaction([
      { sql: "INSERT INTO notes (id, body) VALUES (2, 'first of three')" },
      {
        sql: 'INSERT INTO tags (id, label, icon, weight) VALUES (1, ?, ?, ?)',
        params: ['t', new Uint8Array([9, 8]), 2n ** 60n],
      },
      { sql: "UPDATE notes SET body = 'third of three' WHERE id = 2" },
    ])
    await served.db.executeBatch('INSERT INTO notes (id, body) VALUES (?, ?)', [
      [3, 'a'],
      [4, 'b'],
    ])
    await perTransaction.waitForEvents('s1', 6)
    await staged.waitForEvents('s2', 6)

    const logged = serverChangeLog(served.path).filter(row => row.seq > BigInt(String(subscribed.seq)))
    const expectedEnds = logged.map((row, at) => at === logged.length - 1 || logged[at + 1].tx_id !== row.tx_id)
    const serverNode = serverMeta(served.path, 'node_id')
    for (const [client, id] of [
      [perTransaction, 's1'],
      [staged, 's2'],
    ] as const) {
      const events = client.eventsFor(id)
      expect(events.map(event => [event.seq, event.txId, event.txEnd])).toEqual(
        logged.map((row, at) => [row.seq, row.tx_id, expectedEnds[at]]),
      )
    }
    expect(perTransaction.frames.every(frame => frame.kind === 'change')).toBe(true)
    expect(staged.frames.every(frame => frame.kind === 'changes')).toBe(true)
    const wire = perTransaction.messages
      .filter(message => message.type === 'change')
      .map(message => message.event as Record<string, unknown>)
    expect(wire.map(event => [event.origin, event.hlc, event.rowId])).toEqual(
      logged.map(row => [serverNode, row.hlc, row.row_id]),
    )
    expect(decodeWireRow(wire[2].row as Record<string, unknown>)).toEqual({
      id: 1,
      label: 't',
      icon: new Uint8Array([9, 8]),
      weight: 2n ** 60n,
    })
  })

  it('withholds the writes that the device pushed and every unstamped change, live and on replay', async () => {
    const served = await startStreamServer()
    const device = await served.connect()
    const control = await served.connect()
    const subscribed = await device.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })
    await control.request({ type: 'subscribe', id: 'plain', table: 'notes' })

    await push(served.base, wireBatch([deviceChange({ id: 1 })]))
    await push(served.base, wireBatch([deviceChange({ id: 2, nodeId: OTHER_DEVICE })]))
    await served.db.execute("INSERT INTO notes (id, body) VALUES (3, 'server')")
    writeAroundSirannon(served.path, "INSERT INTO notes (id, body) VALUES (4, 'written around Sirannon')")
    await control.waitForEvents('plain', 4)
    await pause(QUIET_MS)

    expect(control.eventsFor('plain').map(event => Number(event.row.id))).toEqual([1, 2, 3, 4])
    const origins = control.messages
      .filter(message => message.type === 'change')
      .map(message => (message.event as { origin?: string }).origin)
    expect(origins).toEqual([DEVICE, OTHER_DEVICE, serverMeta(served.path, 'node_id'), undefined])
    expect(device.eventsFor('s1').map(event => Number(event.row.id))).toEqual([2, 3])
    const replay = await served.connect()
    await replay.subscribeDevice({
      id: 'r1',
      tables: ['notes'],
      deviceId: DEVICE,
      sinceSeq: BigInt(String(subscribed.seq)),
      epoch: String(subscribed.epoch),
    })
    await replay.waitForEvents('r1', 2)
    await pause(QUIET_MS)
    expect(replay.eventsFor('r1').map(event => Number(event.row.id))).toEqual([2, 3])
  })

  it('reports the delivery window to a device and to no other subscriber', async () => {
    const configured = await startStreamServer(7)
    const client = await configured.connect()

    const device = await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })
    const plain = await client.request({ type: 'subscribe', id: 'plain', table: 'notes' })
    destroyClients(configured)
    await releaseServedRegistries()
    const defaulted = await startStreamServer()
    const defaultedDevice = await (await defaulted.connect()).subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
    })

    expect(device.maxUnacknowledgedChanges).toBe(7)
    expect([plain.type, Object.hasOwn(plain, 'maxUnacknowledgedChanges')]).toEqual(['subscribed', false])
    expect(defaultedDevice.maxUnacknowledgedChanges).toBe(1_000)
  })

  it('refuses every malformed or mismatched device subscription with the code that names the fault', async () => {
    const served = await startStreamServer()
    await served.db.migrate([{ version: 1, name: 'tags', up: 'CREATE TABLE tags (id INTEGER PRIMARY KEY)' }])
    const client = await served.connect()
    const base = { type: 'subscribe', tables: ['notes'], deviceId: DEVICE, schemaVersion: 1 }
    const refusals: Array<[Record<string, unknown>, string]> = [
      [{ ...base, deviceId: 'short' }, 'INVALID_MESSAGE'],
      [{ ...base, deviceId: DEVICE.toUpperCase() }, 'INVALID_MESSAGE'],
      [{ ...base, stagedStream: 'yes' }, 'INVALID_MESSAGE'],
      [{ ...base, tables: [] }, 'INVALID_MESSAGE'],
      [{ ...base, tables: Array.from({ length: 501 }, () => 'notes') }, 'INVALID_MESSAGE'],
      [{ ...base, tables: [''] }, 'INVALID_MESSAGE'],
      [{ ...base, sinceSeq: '-1' }, 'INVALID_MESSAGE'],
      [{ ...base, sinceSeq: 5 }, 'INVALID_MESSAGE'],
      [{ ...base, epoch: 5 }, 'INVALID_MESSAGE'],
      [{ ...base, schemaVersion: -1 }, 'INVALID_MESSAGE'],
      [{ ...base, schemaVersion: undefined }, 'MIGRATION_REQUIRED'],
      [{ ...base, schemaVersion: 2 }, 'SCHEMA_AHEAD'],
      [{ type: 'subscribe', tables: ['notes', 'tags'] }, 'INVALID_MESSAGE'],
      [{ type: 'subscribe', table: 'notes', stagedStream: true }, 'INVALID_MESSAGE'],
    ]

    for (const [message, code] of refusals) {
      const reply = await client.request({ ...message, id: client.nextId() })
      expect([reply.type, errorCode(reply)], JSON.stringify(message).slice(0, 120)).toEqual(['error', code])
    }
    const accepted = await client.request({ ...base, id: 'kept' })
    client.send({ ...base, id: 'kept' })
    const duplicate = await client.waitForMessage(message => message.id === 'kept' && message.type === 'error')
    expect([accepted.type, errorCode(duplicate)]).toEqual(['subscribed', 'DUPLICATE_SUBSCRIPTION'])
    expect(client.socketClosed).toBe(false)
  })
})

describe('acknowledgements', () => {
  it('bind to the device that subscribed on the same connection, and move the cursor forward only', async () => {
    const served = await startStreamServer()
    const subscriber = await served.connect()
    const stranger = await served.connect()

    const early = await ack(subscriber, DEVICE, '7')
    await subscriber.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })
    const otherDevice = await ack(subscriber, OTHER_DEVICE, '7')
    const otherConnection = await ack(stranger, DEVICE, '7')
    expect([errorCode(early), errorCode(otherDevice), errorCode(otherConnection)]).toEqual([
      'DEVICE_NOT_SUBSCRIBED',
      'DEVICE_NOT_SUBSCRIBED',
      'DEVICE_NOT_SUBSCRIBED',
    ])
    expect(deviceCursors(served.path)).toEqual([])

    const accepted = await ack(subscriber, DEVICE, '7')
    const lower = await ack(subscriber, DEVICE, '3')
    expect([accepted.type, accepted.data, lower.type]).toEqual(['result', { acked: true, seq: '7' }, 'result'])
    expect(deviceCursors(served.path)).toEqual([{ device_id: DEVICE, acked_seq: 7n }])

    const malformed: unknown[][] = [
      ['short', '9'],
      [DEVICE, 9],
      [DEVICE, '-1'],
      [DEVICE, '1.5'],
      [DEVICE, '1'.repeat(20)],
      [DEVICE, ''],
    ]
    for (const [deviceId, seq] of malformed) {
      expect(errorCode(await ack(subscriber, deviceId as string, seq)), `${deviceId} ${seq}`).toBe('INVALID_MESSAGE')
    }
    await subscriber.request({ type: 'unsubscribe', id: 's1' })
    const afterUnsubscribe = await ack(subscriber, DEVICE, '9')
    expect(errorCode(afterUnsubscribe)).toBe('DEVICE_NOT_SUBSCRIBED')
    expect(deviceCursors(served.path)).toEqual([{ device_id: DEVICE, acked_seq: 7n }])
  })
})

describe('the delivery window', () => {
  it('holds single-change transactions once the window is exceeded and releases them on each acknowledgement', async () => {
    const window = 5
    const served = await startStreamServer(window)
    const client = await served.connect()
    const subscribed = await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })
    const baseline = BigInt(String(subscribed.seq))

    for (let id = 1; id <= 12; id++) await served.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'x'])
    await client.waitForEvents('s1', window + 1)
    await pause(QUIET_MS)
    const first = client.eventsFor('s1')
    expect(first.map(event => event.seq - baseline)).toEqual([1n, 2n, 3n, 4n, 5n, 6n])

    await ack(client, DEVICE, String(first[2].seq))
    await client.waitForEvents('s1', 9)
    await pause(QUIET_MS)
    expect(client.eventsFor('s1').map(event => event.seq - baseline)).toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n])

    await ack(client, DEVICE, String(client.eventsFor('s1')[8].seq))
    await client.waitForEvents('s1', 12)
    const all = client.eventsFor('s1')
    expect(all.map(event => Number(event.row.id))).toEqual(Array.from({ length: 12 }, (_, at) => at + 1))
    expect(all.every(event => event.txEnd)).toBe(true)
  })

  it('delivers a transaction larger than the window whole, then holds the next transaction until an acknowledgement', async () => {
    const served = await startStreamServer(40)
    const client = await served.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })

    await served.db.executeBatch(
      'INSERT INTO notes (id, body) VALUES (?, ?)',
      Array.from({ length: 150 }, (_, at) => [at + 1, 'x']),
    )
    await client.waitForEvents('s1', 150)
    await served.db.execute("INSERT INTO notes (id, body) VALUES (151, 'next transaction')")
    await pause(QUIET_MS)
    const held = client.eventsFor('s1')
    expect([held.length, held[149].txEnd, held.slice(0, 149).some(event => event.txEnd)]).toEqual([150, true, false])

    await ack(client, DEVICE, String(held[149].seq))
    await client.waitForEvents('s1', 151)
    expect(client.eventsFor('s1')[150]).toMatchObject({ txEnd: true, row: { id: 151 } })
  })

  it('pauses a staged stream inside a transaction at the window and loses nothing as the device acknowledges', async () => {
    for (const window of [1, 3]) await expectStagedTransactionPacedByWindow(window)
  })

  it('paces a staged stream with a window of zero one change at a time through the last change of a transaction', async () => {
    await expectStagedTransactionPacedByWindow(0)
  })
})

describe('resuming a device stream', () => {
  it('replays exactly the changes after its position, and asks for a resync when the position comes from another file', async () => {
    const served = await startStreamServer()
    const first = await served.connect()
    const subscribed = await first.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE })
    await served.db.executeBatch('INSERT INTO notes (id, body) VALUES (?, ?)', [
      [1, 'seen'],
      [2, 'seen'],
    ])
    await first.waitForEvents('s1', 2)
    const position = first.eventsFor('s1')[1].seq
    first.destroy()
    await served.db.execute("INSERT INTO notes (id, body) VALUES (3, 'while away')")
    await served.db.executeTransaction([
      { sql: "INSERT INTO notes (id, body) VALUES (4, 'while away')" },
      { sql: "INSERT INTO notes (id, body) VALUES (5, 'while away')" },
    ])

    const resumed = await served.connect()
    const again = await resumed.subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      sinceSeq: position,
      epoch: String(subscribed.epoch),
    })
    await resumed.waitForEvents('s1', 3)
    await pause(QUIET_MS)
    const foreign = await (await served.connect()).subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: DEVICE,
      sinceSeq: 1n,
      epoch: 'f'.repeat(32),
    })

    const missed = readSqlite<{ seq: bigint }>(
      served.path,
      'SELECT seq FROM _sirannon_changes WHERE seq > ? ORDER BY seq',
      [position],
    )
    expect(again.resync).toBeUndefined()
    expect(resumed.eventsFor('s1').map(event => [event.seq, event.txEnd])).toEqual([
      [missed[0].seq, true],
      [missed[1].seq, false],
      [missed[2].seq, true],
    ])
    expect([foreign.resync, foreign.seq]).toEqual([true, String(missed[2].seq)])
  })
})
