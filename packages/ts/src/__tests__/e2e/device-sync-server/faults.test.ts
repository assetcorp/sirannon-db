import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { failingChangeLogReadDriver } from '../../helpers/fault-drivers/failing-change-log-read-driver.js'
import { markedReadDriver } from '../../helpers/fault-drivers/marked-read-driver.js'
import { releaseServedRegistries } from '../lib/served-registry.js'
import { DEVICE, type DeviceServer, destroyClients, pause, startDeviceServer } from './harness.js'

const QUIET_MS = 400
const WRITES = 5

let server: DeviceServer | undefined

afterEach(async () => {
  destroyClients(server)
  server = undefined
  await releaseServedRegistries()
})

async function startFaultyServer(driver: SQLiteDriver, window?: number): Promise<DeviceServer> {
  server = await startDeviceServer({
    driver,
    server: {
      authenticate: () => ({ userId: 'u_amara' }),
      ...(window === undefined ? {} : { maxUnacknowledgedChanges: window }),
    },
  })
  return server
}

async function backlogFrom(served: DeviceServer): Promise<{ baseline: bigint; epoch: string }> {
  const first = await served.connect()
  const subscribed = await first.subscribeDevice({ id: 'probe', tables: ['notes'], deviceId: DEVICE })
  first.destroy()
  for (let id = 1; id <= WRITES; id++) await served.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'x'])
  return { baseline: BigInt(String(subscribed.seq)), epoch: String(subscribed.epoch) }
}

function untilHeld(held: () => number): Promise<void> {
  return (async () => {
    for (let round = 0; round < 200 && held() === 0; round++) await pause(10)
    expect(held()).toBeGreaterThan(0)
  })()
}

describe('device streams when reading the change log fails or lags', () => {
  it('closes only the connection whose replay read fails, with 1011, and serves the replay once reads recover', async () => {
    const faults = failingChangeLogReadDriver()
    const served = await startFaultyServer(faults.driver)
    const { baseline, epoch } = await backlogFrom(served)
    const bystander = await served.connect()
    await bystander.subscribeDevice({ id: 'live', tables: ['notes'], deviceId: DEVICE })

    faults.arm()
    const failing = await served.connect()
    await failing.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, sinceSeq: baseline, epoch })
    await failing.waitForClose()
    faults.disarm()
    await served.db.execute("INSERT INTO notes (id, body) VALUES (100, 'after the fault')")
    await bystander.waitForEvents('live', 1)
    const recovered = await served.connect()
    await recovered.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, sinceSeq: baseline, epoch })
    await recovered.waitForEvents('s1', WRITES + 1)

    expect([failing.closeCode, failing.eventsFor('s1').length, faults.failedReads() > 0]).toEqual([1011, 0, true])
    expect(bystander.socketClosed).toBe(false)
    expect(recovered.eventsFor('s1').map(event => Number(event.row.id))).toEqual([1, 2, 3, 4, 5, 100])
  })

  it('sends nothing from a replay read that finishes after the device unsubscribed, and keeps the connection usable', async () => {
    const reads = markedReadDriver()
    const served = await startFaultyServer(reads.driver)
    const { baseline, epoch } = await backlogFrom(served)
    let release: () => void = () => {}
    reads.holdChangeLogRangeReads(new Promise<void>(resolve => (release = resolve)))

    const client = await served.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, sinceSeq: baseline, epoch })
    await untilHeld(reads.heldChangeLogRangeReads)
    client.send({ type: 'unsubscribe', id: 's1' })
    const unsubscribed = await client.waitForMessage(message => message.type === 'unsubscribed')
    release()
    await pause(QUIET_MS)
    expect([client.closeCode, client.socketClosed]).toEqual([null, false])
    await client.subscribeDevice({ id: 's2', tables: ['notes'], deviceId: DEVICE })
    await served.db.execute("INSERT INTO notes (id, body) VALUES (100, 'live')")
    await client.waitForEvents('s2', 1)

    expect([unsubscribed.type, client.eventsFor('s1').length]).toEqual(['unsubscribed', 0])
    expect(client.eventsFor('s2').map(event => Number(event.row.id))).toEqual([100])
    expect(client.socketClosed).toBe(false)
  })

  it('applies an acknowledgement that arrives while a replay read is still running', async () => {
    const reads = markedReadDriver()
    const served = await startFaultyServer(reads.driver, 1)
    const { baseline, epoch } = await backlogFrom(served)
    let release: () => void = () => {}
    reads.holdChangeLogRangeReads(new Promise<void>(resolve => (release = resolve)))

    const client = await served.connect()
    await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: DEVICE, sinceSeq: baseline, epoch })
    await untilHeld(reads.heldChangeLogRangeReads)
    const acknowledged = await client.request({
      type: 'ack',
      id: client.nextId(),
      deviceId: DEVICE,
      seq: (baseline + BigInt(WRITES - 2)).toString(),
    })
    release()
    await client.waitForEvents('s1', WRITES)
    await pause(QUIET_MS)

    expect(acknowledged.type).toBe('result')
    expect(client.eventsFor('s1').map(event => Number(event.row.id))).toEqual([1, 2, 3, 4, 5])
  })
})
