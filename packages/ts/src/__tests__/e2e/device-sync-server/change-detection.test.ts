import { execFileSync } from 'node:child_process'
import { afterEach, describe, expect, it } from 'vitest'
import { releaseServedRegistries } from '../lib/served-registry.js'
import {
  type DeviceServer,
  destroyClients,
  deviceCursors,
  pause,
  startDeviceServer,
  writeAroundSirannon,
} from './harness.js'

const WS_CLOSE_DEVICE_CURSOR_DROPPED = 4410
const QUIET_MS = 400
const EXTERNAL_WRITE_DEADLINE_MS = 2_000
const DEVICE_COUNT = 40

const WRITE_FROM_ANOTHER_PROCESS = `
const Database = require('better-sqlite3')
const db = new Database(process.argv[1], { fileMustExist: true })
db.pragma('busy_timeout = 5000')
db.prepare('INSERT INTO notes (id, body) VALUES (?, ?)').run(Number(process.argv[2]), 'from another process')
db.close()
`

let server: DeviceServer | undefined

afterEach(async () => {
  destroyClients(server)
  server = undefined
  await releaseServedRegistries()
})

function deviceIdAt(index: number): string {
  return index.toString(16).padStart(32, 'a')
}

function writeFromAnotherProcess(path: string, id: number): void {
  execFileSync(process.execPath, ['-e', WRITE_FROM_ANOTHER_PROCESS, path, String(id)], { cwd: process.cwd() })
}

describe('the server change feed', () => {
  it('delivers a change that another process commits to an idle database without waiting for a fallback read', async () => {
    server = await startDeviceServer({ server: { acceptSql: true, authenticate: () => ({ userId: 'u_amara' }) } })
    const device = await server.connect()
    const client = await server.connect()
    await device.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: deviceIdAt(1) })
    await client.request({ type: 'subscribe', id: 'plain', table: 'notes' })
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'server')")
    await client.waitForEvents('plain', 1)
    await device.waitForEvents('s1', 1)
    await pause(QUIET_MS)

    for (const id of [2, 3]) {
      const writtenAt = Date.now()
      writeFromAnotherProcess(server.path, id)
      await client.waitForEvents('plain', id, EXTERNAL_WRITE_DEADLINE_MS)
      expect(Date.now() - writtenAt).toBeLessThan(EXTERNAL_WRITE_DEADLINE_MS)
      await pause(QUIET_MS)
    }

    expect(client.eventsFor('plain').map(event => Number(event.row.id))).toEqual([1, 2, 3])
  })

  it('stores every acknowledgement that many devices send at once, and closes only the device whose cursor is gone', async () => {
    server = await startDeviceServer({ server: { authenticate: () => ({ userId: 'u_amara' }) } })
    const served = server
    const devices = await Promise.all(
      Array.from({ length: DEVICE_COUNT }, async (_, index) => {
        const client = await served.connect()
        await client.subscribeDevice({ id: 's1', tables: ['notes'], deviceId: deviceIdAt(index) })
        return { client, deviceId: deviceIdAt(index) }
      }),
    )
    for (let id = 1; id <= 3; id++) await served.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'x'])
    await Promise.all(devices.map(({ client }) => client.waitForEvents('s1', 3)))
    const seqs = devices[0].client.eventsFor('s1').map(event => event.seq)

    const firstRound = await Promise.all(
      devices.map(({ client, deviceId }, index) =>
        client.request({ type: 'ack', id: client.nextId(), deviceId, seq: seqs[index % 2].toString() }),
      ),
    )
    expect(firstRound.map(reply => reply.type)).toEqual(devices.map(() => 'result'))
    expect(deviceCursors(served.path)).toEqual(
      devices
        .map(({ deviceId }, index) => ({ device_id: deviceId, acked_seq: seqs[index % 2] }))
        .sort((a, b) => a.device_id.localeCompare(b.device_id)),
    )

    const victim = devices[7]
    writeAroundSirannon(served.path, 'DELETE FROM _sirannon_device_cursors WHERE device_id = ?', [victim.deviceId])
    const survivors = devices.filter(device => device !== victim)
    victim.client.sendAck(victim.deviceId, seqs[2])
    const secondRound = await Promise.all(
      survivors.map(({ client, deviceId }) =>
        client.request({ type: 'ack', id: client.nextId(), deviceId, seq: seqs[2].toString() }),
      ),
    )
    await victim.client.waitForClose()

    expect(victim.client.closeCode).toBe(WS_CLOSE_DEVICE_CURSOR_DROPPED)
    expect(secondRound.map(reply => [reply.type, (reply.data as { seq?: string } | undefined)?.seq])).toEqual(
      survivors.map(() => ['result', seqs[2].toString()]),
    )
    expect(deviceCursors(served.path)).toEqual(
      survivors
        .map(({ deviceId }) => ({ device_id: deviceId, acked_seq: seqs[2] }))
        .sort((a, b) => a.device_id.localeCompare(b.device_id)),
    )
  })
})
