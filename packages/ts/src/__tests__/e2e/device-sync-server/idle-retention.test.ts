import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { releaseServedRegistries, waitUntil } from '../lib/served-registry.js'
import {
  DEVICE,
  type DeviceServer,
  destroyClients,
  deviceCursors,
  serverChangeLog,
  startDeviceServer,
} from './harness.js'

const servers: DeviceServer[] = []

afterEach(async () => {
  for (const server of servers.splice(0)) destroyClients(server)
  await releaseServedRegistries()
})

async function openServer(directory: string, cdcRetention: number) {
  const served = await startDeviceServer({
    directory,
    database: { cdcPollInterval: 5, cdcRetention },
    server: { authenticate: () => ({ userId: 'subscriber' }) },
  })
  servers.push(served)
  const client = await served.connect()
  await client.subscribeDevice({ id: 'notes', tables: ['notes'], deviceId: DEVICE })
  return { served, client }
}

describe('retention without later application writes', () => {
  it('prunes held changes after an existing device cursor advances beyond them', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'sirannon-ack-retention-'))
    const { served, client } = await openServer(directory, 0)
    await served.db.execute("INSERT INTO notes VALUES (1, 'first'), (2, 'second')")
    await client.waitForEvents('notes', 2)
    const [first, second] = client.eventsFor('notes')
    const ack = (seq: bigint) =>
      client.request({ type: 'ack', id: client.nextId(), deviceId: DEVICE, seq: seq.toString() })
    expect((await ack(first.seq)).type).toBe('result')
    await waitUntil(
      () => serverChangeLog(served.path).length === 1,
      'the first prune to retain only the unacknowledged change',
    )
    expect(serverChangeLog(served.path).map(row => row.seq)).toEqual([second.seq])
    expect((await ack(second.seq)).type).toBe('result')
    expect(deviceCursors(served.path)).toEqual([{ device_id: DEVICE, acked_seq: second.seq }])
    await waitUntil(() => serverChangeLog(served.path).length === 0, 'the acknowledged change to expire')
  })

  it('prunes expired changes after reopening when the subscriber starts at the end of the log', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'sirannon-reopened-retention-'))
    const first = await openServer(directory, 3_600_000)
    await first.served.db.execute("INSERT INTO notes VALUES (1, 'retained before reopening')")
    await first.client.waitForEvents('notes', 1)
    const seq = first.client.eventsFor('notes')[0].seq
    expect(
      (await first.client.request({ type: 'ack', id: first.client.nextId(), deviceId: DEVICE, seq: seq.toString() }))
        .type,
    ).toBe('result')
    destroyClients(first.served)
    await first.served.server.close()
    await first.served.sirannon.shutdown()
    expect(serverChangeLog(first.served.path).map(row => row.seq)).toEqual([seq])

    const reopened = await openServer(directory, 0)
    await waitUntil(() => serverChangeLog(reopened.served.path).length === 0, 'the retained change to expire')
    expect(reopened.client.eventsFor('notes')).toEqual([])
  })
})
