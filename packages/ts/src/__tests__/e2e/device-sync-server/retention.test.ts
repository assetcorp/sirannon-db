import { afterEach, describe, expect, it } from 'vitest'
import type { RawDeviceClient } from '../lib/raw-device-client.js'
import { releaseServedRegistries } from '../lib/served-registry.js'
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
  startDeviceServer,
  wireBatch,
} from './harness.js'

const HOUR_MS = 3_600_000
const PRUNING_DEADLINE_MS = 20_000
const THIRD_DEVICE = 'c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0'

let server: DeviceServer | undefined

afterEach(async () => {
  destroyClients(server)
  server = undefined
  await releaseServedRegistries()
})

async function startRetentionServer(database: Record<string, number>, window?: number): Promise<DeviceServer> {
  server = await startDeviceServer({
    database: { cdcRetention: -HOUR_MS, ...database },
    server: {
      authenticate: () => ({ userId: 'u_amara' }),
      ...(window === undefined ? {} : { maxUnacknowledgedChanges: window }),
    },
  })
  return server
}

async function eventually(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + PRUNING_DEADLINE_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await pause(50)
  }
}

async function subscribe(
  served: DeviceServer,
  deviceId: string,
): Promise<{ client: RawDeviceClient; seq: bigint; epoch: string }> {
  const client = await served.connect()
  const subscribed = await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId })
  return { client, seq: BigInt(String(subscribed.seq)), epoch: String(subscribed.epoch) }
}

async function acknowledge(client: RawDeviceClient, deviceId: string, seq: bigint): Promise<void> {
  const reply = await client.request({ type: 'ack', id: client.nextId(), deviceId, seq: seq.toString() })
  expect(reply.type).toBe('result')
}

function loggedSeqs(path: string): bigint[] {
  return serverChangeLog(path).map(row => row.seq)
}

describe('change-log retention held for devices', () => {
  it('prunes what every device acknowledged, ignores the writes a device made itself, and holds at the lowest device', async () => {
    const served = await startRetentionServer({})
    const a = await subscribe(served, DEVICE)
    await served.db.execute("INSERT INTO notes (id, body) VALUES (1, 'server')")
    await a.client.waitForEvents('s1', 1)
    await acknowledge(a.client, DEVICE, a.client.eventsFor('s1')[0].seq)
    await push(served.base, wireBatch([deviceChange({ id: 2 }), deviceChange({ id: 3 })]))

    await eventually(() => loggedSeqs(served.path).length === 0, 'the writes of the only device to leave the log')

    const b = await subscribe(served, OTHER_DEVICE)
    for (let id = 4; id <= 6; id++)
      await served.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'server'])
    await a.client.waitForEvents('s1', 4)
    await b.client.waitForEvents('s1', 3)
    const [s4, s5, s6] = b.client.eventsFor('s1').map(event => event.seq)
    await acknowledge(a.client, DEVICE, s6)
    await acknowledge(b.client, OTHER_DEVICE, s4)

    await eventually(
      () => !loggedSeqs(served.path).includes(s4),
      'the change both devices acknowledged to leave the log',
    )
    expect(loggedSeqs(served.path)).toEqual([s5, s6])
    expect(deviceCursors(served.path)).toEqual([
      { device_id: DEVICE, acked_seq: s6 },
      { device_id: OTHER_DEVICE, acked_seq: s4 },
    ])
  }, 60_000)

  it('drops the cursor of a silent device and of a device whose oldest held change outlives the window, and makes each resync', async () => {
    const windowMs = 5_500
    const served = await startRetentionServer({ deviceCursorRetention: windowMs })
    const late = await subscribe(served, DEVICE)
    const silent = await subscribe(served, OTHER_DEVICE)
    const keeper = await subscribe(served, THIRD_DEVICE)
    await served.db.execute("INSERT INTO notes (id, body) VALUES (1, 'server')")
    await keeper.client.waitForEvents('s1', 1)
    const s1 = keeper.client.eventsFor('s1')[0].seq
    await acknowledge(silent.client, OTHER_DEVICE, s1)
    const refresh = setInterval(() => keeper.client.sendAck(THIRD_DEVICE, s1), 500)

    try {
      await pause(windowMs + 500)
      await acknowledge(late.client, DEVICE, late.seq)
      await eventually(
        () => deviceCursors(served.path).every(cursor => cursor.device_id === THIRD_DEVICE),
        'the expired cursors to be dropped',
      )
      await eventually(() => loggedSeqs(served.path).length === 0, 'the log to be pruned')
    } finally {
      clearInterval(refresh)
    }

    expect(deviceCursors(served.path)).toEqual([{ device_id: THIRD_DEVICE, acked_seq: s1 }])
    for (const [deviceId, position] of [
      [DEVICE, late],
      [OTHER_DEVICE, silent],
    ] as const) {
      const resumed = await (await served.connect()).subscribeDevice({
        id: 's1',
        tables: ['notes'],
        deviceId,
        sinceSeq: position.seq,
        epoch: position.epoch,
      })
      expect(resumed.resync, deviceId).toBe(true)
    }
  }, 60_000)

  it('drops the cursor of a device that holds back more changes than the cap, and keeps one at the cap', async () => {
    const served = await startRetentionServer({ maxChangesHeldForDevice: 2 })
    const atCap = await subscribe(served, DEVICE)
    const overCap = await subscribe(served, OTHER_DEVICE)
    const keeper = await subscribe(served, THIRD_DEVICE)
    await acknowledge(overCap.client, OTHER_DEVICE, overCap.seq)
    for (let id = 1; id <= 3; id++)
      await served.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'server'])
    await keeper.client.waitForEvents('s1', 3)
    const [s1, s2, s3] = keeper.client.eventsFor('s1').map(event => event.seq)
    await acknowledge(atCap.client, DEVICE, s1)
    await acknowledge(keeper.client, THIRD_DEVICE, s3)

    await eventually(
      () => !deviceCursors(served.path).some(cursor => cursor.device_id === OTHER_DEVICE),
      'the cursor over the cap to be dropped',
    )
    await eventually(() => !loggedSeqs(served.path).includes(s1), 'the log to be pruned to the device at the cap')

    expect(deviceCursors(served.path).map(cursor => cursor.device_id)).toEqual([THIRD_DEVICE, DEVICE])
    expect(loggedSeqs(served.path)).toEqual([s2, s3])
    const resumed = await (await served.connect()).subscribeDevice({
      id: 's1',
      tables: ['notes'],
      deviceId: OTHER_DEVICE,
      sinceSeq: overCap.seq,
      epoch: overCap.epoch,
    })
    expect(resumed.resync).toBe(true)
  }, 60_000)

  it('tells a connected device whose cursor the server dropped to resync, and delivers no change past the pruned ones', async () => {
    const served = await startRetentionServer({ maxChangesHeldForDevice: 2 }, 1)
    const slow = await subscribe(served, DEVICE)
    const keeper = await subscribe(served, THIRD_DEVICE)
    await acknowledge(slow.client, DEVICE, slow.seq)
    for (let id = 1; id <= 5; id++)
      await served.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'server'])
    await slow.client.waitForEvents('s1', 2)
    const refresh = setInterval(() => {
      const events = keeper.client.eventsFor('s1')
      if (events.length > 0) keeper.client.sendAck(THIRD_DEVICE, events[events.length - 1].seq)
    }, 100)

    try {
      await eventually(() => keeper.client.eventsFor('s1').length === 5, 'the keeper to receive every change')
      await eventually(
        () => !deviceCursors(served.path).some(cursor => cursor.device_id === DEVICE),
        'the cursor over the cap to be dropped',
      )
      await eventually(() => loggedSeqs(served.path).length < 3, 'the log to be pruned')
    } finally {
      clearInterval(refresh)
    }
    const received = slow.client.eventsFor('s1')
    slow.client.sendAck(DEVICE, received[received.length - 1].seq)
    await pause(1_000)

    const delivered = slow.client.eventsFor('s1').map(event => Number(event.row.id))
    const told = slow.client.socketClosed || slow.client.messages.some(message => message.type === 'error')
    expect(told || delivered.join() === '1,2,3,4,5', `delivered ${delivered.join()}`).toBe(true)
    expect(delivered, 'the changes delivered before the device was told').toEqual(
      Array.from({ length: delivered.length }, (_, at) => at + 1),
    )
    expect(deviceCursors(served.path).map(cursor => cursor.device_id)).not.toContain(DEVICE)
  }, 60_000)
})
