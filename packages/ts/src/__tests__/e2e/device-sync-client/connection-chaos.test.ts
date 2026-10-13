import { afterEach, describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/index.js'
import { readSqlite } from '../lib/served-registry.js'
import {
  countOf,
  eventually,
  metaValue,
  openDevice,
  releaseDeviceSync,
  sameRows,
  sampleCounts,
  sleep,
  stagedCount,
  startSyncServer,
} from './harness.js'

const WINDOW = 20
const BULK_ROWS = 1_500
const DEVICE_SEED_ROWS = 200
const PUSH_BACKLOG = 1_001

afterEach(releaseDeviceSync)

type Server = Awaited<ReturnType<typeof startSyncServer>>

async function writePair(server: Server, first: number): Promise<void> {
  await server.db.transaction(async tx => {
    await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [first, 'first of the pair'])
    await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [first + 1, 'second of the pair'])
  })
}

function pairsWhole(samples: readonly number[][]): boolean {
  return samples.every(pairCounts => pairCounts.every(count => count === 0 || count === 2))
}

function samplePairs(path: string, firsts: readonly number[]): { stop(): number[][] } {
  const samplers = firsts.map(first =>
    sampleCounts(path, 'SELECT count(*) AS n FROM notes WHERE id IN (?, ?)', [first, first + 1]),
  )
  return { stop: () => samplers.map(sampler => sampler.stop()) }
}

describe('a device syncing through dying connections', () => {
  it('loses no transaction when connections die or the server restarts at each point of delivery', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const controller = device.sync()
    await controller.start()

    const firsts = [100, 200, 300, 400, 500, 600, 700, 800]
    const pairs = samplePairs(device.path, firsts)
    for (const [round, first] of firsts.entries()) {
      await writePair(server, first)
      await sleep([0, 5, 15, 40][round % 4] ?? 0)
      if (round % 2 === 0) server.proxy.killAllConnections()
      else await server.restart()
    }

    await eventually(() => countOf(device.path, 'notes') === firsts.length * 2, 'every pair arriving')
    expect(pairsWhole(pairs.stop())).toBe(true)
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    await eventually(
      async () => metaValue(device.path, 'device_sync_pull_seq') === String((await controller.status()).lastPulledSeq),
      'the durable cursor matching the applied one',
    )
  }, 60_000)

  it('loses no transaction when the server holds delivery at a window of one change', async () => {
    const server = await startSyncServer({ options: { maxUnacknowledgedChanges: 1 } })
    const device = await openDevice(server.url)
    const controller = device.sync()
    await controller.start()

    const firsts = [500, 600, 700, 800, 900]
    const pairs = samplePairs(device.path, firsts)
    for (const first of firsts) await writePair(server, first)

    await eventually(() => countOf(device.path, 'notes') === firsts.length * 2, 'every pair arriving')
    expect(pairsWhole(pairs.stop())).toBe(true)
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  }, 60_000)

  it('ends with the same bytes as the server when connections die inside a bulk load over a log full of its own echoes', async () => {
    const server = await startSyncServer({ options: { maxUnacknowledgedChanges: WINDOW } })
    const device = await openDevice(server.url)
    const controller = device.sync({ autoResync: false })
    await controller.start()

    await device.db.transaction(async tx => {
      for (let id = 1; id <= DEVICE_SEED_ROWS; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `device seed ${id}`])
      }
    })
    await eventually(() => countOf(server.path, 'notes') === DEVICE_SEED_ROWS, 'the device seed reaching the server')

    const sampler = sampleCounts(device.path, 'SELECT count(*) AS n FROM notes WHERE id > 10000 AND id < 20000')
    const body = 'y'.repeat(1_024)
    await server.db.bulkLoad(
      'INSERT INTO notes (id, body) VALUES (?, ?)',
      Array.from({ length: BULK_ROWS }, (_, at) => [10_001 + at, body]),
    )
    await eventually(() => stagedCount(device.path) >= BULK_ROWS / 4, 'a quarter of the load reaching staging')
    server.proxy.killAllConnections()
    for (const [round, delay] of [300, 150, 500, 300].entries()) {
      await sleep(delay)
      server.proxy.killAllConnections()
      if (round === 1) await device.db.execute("INSERT INTO notes (id, body) VALUES (20001, 'written mid-chaos')")
    }

    await eventually(
      () =>
        countOf(server.path, 'notes') === DEVICE_SEED_ROWS + BULK_ROWS + 1 &&
        sameRows(server.path, device.path, 'notes'),
      'the device and the server holding the same bytes',
      60_000,
    )
    await eventually(() => stagedCount(device.path) === 0, 'the staging table emptying')
    expect(sampler.stop().filter(count => count !== 0 && count !== BULK_ROWS)).toEqual([])
  }, 120_000)

  it('keeps device writes made while the server is down and receives the server writes made meanwhile', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const controller = device.sync()
    await controller.start()

    await server.stop()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (800, 'written offline')")
    await writePair(server, 900)
    await sleep(300)
    expect((await controller.status()).state).toBe('running')
    await server.start()

    await eventually(() => countOf(server.path, 'notes') === 3 && sameRows(server.path, device.path, 'notes'), 'sync')
  }, 30_000)

  it('pushes a backlog once through connections that die while it pushes, with no change applied twice', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    await device.db.transaction(async tx => {
      for (let id = 1; id <= PUSH_BACKLOG; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `backlog ${id}`])
      }
    })
    const controller = device.sync({ batchSize: 10, pushIntervalMs: 5 })
    await controller.start()

    let killsMidPush = 0
    while (countOf(server.path, 'notes') < PUSH_BACKLOG) {
      if (server.proxy.killAllConnections() > 0) killsMidPush += 1
      await sleep(7)
    }
    expect(killsMidPush).toBeGreaterThanOrEqual(3)

    await eventually(async () => (await controller.status()).pushCaughtUp, 'the outbox draining')
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    const [logged] = readSqlite<{ n: bigint }>(
      server.path,
      "SELECT count(*) AS n FROM _sirannon_changes WHERE table_name = 'notes'",
    )
    expect(Number(logged?.n)).toBe(PUSH_BACKLOG)
  }, 60_000)

  it('finishes a transaction it was part-way through receiving when its own process stopped', async () => {
    const server = await startSyncServer({ options: { maxUnacknowledgedChanges: WINDOW } })
    const device = await openDevice(server.url)
    const seen: string[] = []
    const record = (event: ChangeEvent) => seen.push(String(event.row.id))
    const first = device.sync({ onChange: record })
    await first.start()

    await server.db.transaction(async tx => {
      for (let id = 1; id <= WINDOW * 15; id++)
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'b'])
    })
    await eventually(() => stagedCount(device.path) >= WINDOW, 'part of the transaction reaching staging')
    await device.reopen()
    expect(countOf(device.path, 'notes')).toBe(0)

    const second = device.sync({ onChange: record })
    await second.start()
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the transaction finishing')
    await sleep(200)
    expect([seen.length, new Set(seen).size]).toEqual([WINDOW * 15, WINDOW * 15])
  }, 30_000)
})
