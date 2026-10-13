import { afterEach, describe, expect, it } from 'vitest'
import {
  countOf,
  eventually,
  NOTES_TABLE,
  openDevice,
  releaseDeviceSync,
  sameRows,
  sampleCounts,
  sleep,
  stagedCount,
  startSyncServer,
  TAGS_TABLE,
} from './harness.js'
import { forwardEverything, startHostileServer } from './hostile-server.js'

const WINDOW = 20
const SILENT = { immediateAckAfterChanges: 1_000_000_000, ackIntervalMs: 600_000 }

afterEach(releaseDeviceSync)

async function serverTransaction(
  server: Awaited<ReturnType<typeof startSyncServer>>,
  firstId: number,
  rows: number,
): Promise<void> {
  await server.db.transaction(async tx => {
    for (let id = firstId; id < firstId + rows; id++) {
      await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `row ${id}`])
    }
  })
}

describe('a device pulling transactions around the delivery window', () => {
  it('applies transactions at, around, and ten times the window whole, never showing part of one', async () => {
    const server = await startSyncServer({ options: { maxUnacknowledgedChanges: WINDOW } })
    const device = await openDevice(server.url)
    const controller = device.sync()
    await controller.start()

    const sizes = [1, WINDOW - 1, WINDOW, WINDOW + 1, WINDOW * 2 + 1, WINDOW * 10]
    const prefixSums = [0]
    for (const size of sizes) prefixSums.push((prefixSums.at(-1) ?? 0) + size)
    const sampler = sampleCounts(device.path, 'SELECT count(*) AS n FROM notes')
    let firstId = 1
    for (const size of sizes) {
      await serverTransaction(server, firstId, size)
      firstId += size
    }
    const total = prefixSums.at(-1) ?? 0

    await eventually(() => countOf(device.path, 'notes') === total, 'every transaction arriving')
    await eventually(() => stagedCount(device.path) === 0, 'the staging table emptying')
    const samples = sampler.stop()

    expect(samples.filter(count => !prefixSums.includes(count))).toEqual([])
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  })

  it('keeps a transaction staged on disk through a stop and a reopen, showing none of it until the rest arrives', async () => {
    const server = await startSyncServer({ options: { maxUnacknowledgedChanges: WINDOW } })
    const device = await openDevice(server.url)
    const silent = device.sync(SILENT)
    await silent.start()

    await serverTransaction(server, 1, WINDOW * 10)
    await eventually(() => stagedCount(device.path) >= 5, 'part of the transaction reaching staging')
    await sleep(200)
    const stagedBeforeStop = stagedCount(device.path)
    expect(stagedBeforeStop).toBeLessThan(WINDOW * 10)
    expect(countOf(device.path, 'notes')).toBe(0)

    await device.db.execute("INSERT INTO notes (id, body) VALUES (5000, 'written while the pull waits')")
    await eventually(() => countOf(server.path, 'notes') === WINDOW * 10 + 1, 'the device write reaching the server')
    expect(countOf(device.path, 'notes')).toBe(1)

    await silent.stop()
    await device.reopen()
    expect([countOf(device.path, 'notes'), stagedCount(device.path)]).toEqual([1, stagedBeforeStop])

    const resumed = device.sync()
    await resumed.start()
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the transaction finishing after the reopen')
    await eventually(() => stagedCount(device.path) === 0, 'the staging table emptying')
  })

  it('subscribes again with every table and its staged declaration after the connection dies', async () => {
    const server = await startSyncServer({
      schema: [NOTES_TABLE, TAGS_TABLE],
      watch: ['notes', 'tags'],
      options: { maxUnacknowledgedChanges: WINDOW },
    })
    const hostile = await startHostileServer(() => server.directPort, forwardEverything)
    const device = await openDevice(hostile.url, { schema: [NOTES_TABLE, TAGS_TABLE], watch: ['notes', 'tags'] })
    const controller = device.sync({ tables: ['notes', 'tags'] })
    await controller.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'before the drop')")
    await eventually(() => countOf(device.path, 'notes') === 1, 'the first row')

    const upgradesBefore = hostile.upgradeCount()
    hostile.dropUpgrades()
    const framesBefore = hostile.downstreamFrames().length
    await server.db.transaction(async tx => {
      for (let id = 2; id < 2 + WINDOW * 3; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `note ${id}`])
        await tx.execute('INSERT INTO tags (id, label) VALUES (?, ?)', [id, `tag ${id}`])
      }
    })

    await eventually(
      () => sameRows(server.path, device.path, 'notes') && sameRows(server.path, device.path, 'tags'),
      'both tables after the reconnect',
    )
    expect(hostile.upgradeCount()).toBeGreaterThan(upgradesBefore)
    const afterReconnect = hostile.downstreamFrames().slice(framesBefore)
    expect(afterReconnect).toContain('"type":"changes"')
    expect(afterReconnect).not.toMatch(/"type":"change"[,}]/)
  })
})
