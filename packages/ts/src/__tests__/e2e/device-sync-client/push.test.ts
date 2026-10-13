import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { HookDeniedError } from '../../../core/index.js'
import { readSqlite, scratchDirectory } from '../lib/served-registry.js'
import {
  countOf,
  eventually,
  metaValue,
  openDevice,
  releaseDeviceSync,
  rowsOf,
  sameRows,
  sampleCounts,
  sleep,
  startSyncServer,
  writeSqlite,
} from './harness.js'

const LEDGER_TABLE =
  'CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER, payload BLOB, memo TEXT, ratio REAL, note TEXT)'
const INSERT_LEDGER = 'INSERT INTO ledger (id, amount, payload, memo, ratio, note) VALUES (?, ?, ?, ?, ?, ?)'
const SERVER_BATCH_LIMIT = 1_000
const FIVE_MINUTES_MS = 300_000
const DEVICE_NODE_ID = 'c0ffee00c0ffee00c0ffee00c0ffee00'
const REQUIRED_CAPABILITIES = [
  'sync.push',
  'sync.echo-suppression',
  'sync.ack',
  'sync.resume',
  'sync.snapshot',
  'sync.migrations',
  'sync.schema-gate',
  'sync.stream-apply',
]

afterEach(releaseDeviceSync)

function ownWritesThrough(path: string): bigint {
  const [row] = readSqlite<{ top: bigint | null }>(
    path,
    "SELECT max(seq) AS top FROM _sirannon_changes WHERE node_id = (SELECT value FROM _sirannon_meta WHERE key = 'node_id')",
  )
  return row?.top ?? 0n
}

function hlcAhead(aheadMs: number, nodeId: string): string {
  return `${(Date.now() + aheadMs).toString(16).padStart(12, '0')}-0000-${nodeId}`
}

describe('a device pushing its own writes', () => {
  it('pushes each write once and keeps its durable cursor through a pause, a stop, and a reopen', async () => {
    const pushedTables: string[] = []
    const server = await startSyncServer({ hooks: { onBeforePush: ({ table }) => void pushedTables.push(table) } })
    const device = await openDevice(server.url)
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'written before the first start')")
    await device.db.execute("INSERT INTO notes (id, body) VALUES (2, 'also offline')")

    const first = device.sync({ pushIntervalMs: 60_000 })
    await first.start()
    await eventually(() => countOf(server.path, 'notes') === 2, 'the offline writes reaching the server')
    await eventually(async () => (await first.status()).pushCaughtUp, 'the outbox draining')
    expect((await first.status()).serverCapabilities).toEqual(expect.arrayContaining(REQUIRED_CAPABILITIES))

    await device.db.execute("INSERT INTO notes (id, body) VALUES (3, 'typed while the interval is long')")
    const waiting = await first.status()
    expect([waiting.pendingPushCount, waiting.pushCaughtUp]).toEqual([1, false])
    first.triggerPush()
    await eventually(async () => (await first.status()).pushCaughtUp, 'the triggered push draining')

    first.pause()
    expect((await first.status()).state).toBe('paused')
    await device.db.execute("INSERT INTO notes (id, body) VALUES (4, 'typed while paused')")
    first.triggerPush()
    await sleep(300)
    expect(countOf(server.path, 'notes')).toBe(3)
    await first.resume()
    await eventually(() => countOf(server.path, 'notes') === 4, 'the paused write reaching the server')
    await eventually(async () => (await first.status()).pushCaughtUp, 'the outbox draining after the resume')
    await first.stop()

    const pushesBeforeReopen = pushedTables.length
    expect(metaValue(device.path, 'device_sync_pushed_seq')).toBe(ownWritesThrough(device.path).toString())

    await device.reopen()
    const second = device.sync()
    await second.start()
    const reopened = await second.status()
    await sleep(300)

    expect([reopened.pendingPushCount, reopened.lastPushedSeq]).toEqual([0, ownWritesThrough(device.path)])
    expect(pushedTables.length).toBe(pushesBeforeReopen)
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  })

  it('delivers integers past the safe range, blobs, nulls, reals, and text in any script with every byte intact', async () => {
    const server = await startSyncServer({ schema: [LEDGER_TABLE], watch: ['ledger'] })
    const writer = await openDevice(server.url, { schema: [LEDGER_TABLE], watch: ['ledger'] })
    const reader = await openDevice(server.url, { schema: [LEDGER_TABLE], watch: ['ledger'] })
    const bigBlob = Buffer.alloc(256 * 1024)
    for (let at = 0; at < bigBlob.length; at++) bigBlob[at] = (at * 31 + 7) % 256
    const rows: unknown[][] = [
      [1, 9_223_372_036_854_775_807n, Buffer.from([0, 255, 1, 254]), 'Ελληνικά 漢字 🧪 עברית', 0.1, null],
      [2, -9_223_372_036_854_775_808n, Buffer.alloc(0), '', 1e308, 'quote " and \\ and \n newline'],
      [3, 9_007_199_254_740_993n, bigBlob, '{"__sirannon_int":"5"}', -2.5e-300, '{"__sirannon_blob":"00"}'],
      [4, null, null, null, null, null],
      [5, 0, Buffer.from('plain text bytes'), 'x'.repeat(100_000), 3, '  '],
    ]

    const pushing = writer.sync({ tables: ['ledger'] })
    const pulling = reader.sync({ tables: ['ledger'] })
    await pushing.start()
    await pulling.start()
    await writer.db.transaction(async tx => {
      for (const row of rows) await tx.execute(INSERT_LEDGER, row)
    })

    await eventually(() => sameRows(writer.path, server.path, 'ledger'), 'the server holding the pushed rows')
    await eventually(() => sameRows(server.path, reader.path, 'ledger'), 'the second device holding the pulled rows')

    const types = 'SELECT typeof(amount) AS a, typeof(payload) AS p, typeof(memo) AS m, typeof(ratio) AS r FROM ledger'
    expect(readSqlite(server.path, types)).toEqual(readSqlite(writer.path, types))
    expect(readSqlite(reader.path, types)).toEqual(readSqlite(writer.path, types))
    expect(rowsOf(server.path, 'ledger')[0]?.amount).toBe(9_223_372_036_854_775_807n)
    expect(Buffer.compare(rowsOf(reader.path, 'ledger')[2]?.payload as Buffer, bigBlob)).toBe(0)
  })

  it('drains a backlog larger than one batch at the smallest and the largest batch size that the server accepts', async () => {
    const server = await startSyncServer()
    const backlog = SERVER_BATCH_LIMIT + 1
    for (const batchSize of [1, SERVER_BATCH_LIMIT]) {
      const device = await openDevice(server.url)
      const offset = batchSize * 10_000
      await device.db.transaction(async tx => {
        for (let id = 1; id <= backlog; id++) {
          await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [offset + id, `row ${id}`])
        }
      })
      const controller = device.sync({ batchSize, pushIntervalMs: 10 })
      await controller.start()
      await eventually(
        () =>
          Number(
            readSqlite<{ n: bigint }>(server.path, 'SELECT count(*) AS n FROM notes WHERE id > ? AND id <= ?', [
              offset,
              offset + backlog,
            ])[0]?.n,
          ) === backlog,
        `a backlog of ${backlog} pushed ${batchSize} at a time`,
        60_000,
      )
      await eventually(async () => (await controller.status()).pushCaughtUp, `the outbox of batch size ${batchSize}`)
      expect((await controller.status()).lastError).toBeNull()
    }
  }, 120_000)

  it('applies a transaction larger than one push batch on the server all at once, never in parts', async () => {
    const rowsInTransaction = 23
    const countsBeforeEachBatch: number[] = []
    let serverPath = ''
    const server = await startSyncServer({
      hooks: {
        onBeforePush: async () => {
          countsBeforeEachBatch.push(countOf(serverPath, 'notes'))
          await sleep(30)
        },
      },
    })
    serverPath = server.path
    const device = await openDevice(server.url)
    await device.db.transaction(async tx => {
      for (let id = 1; id <= rowsInTransaction; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `row ${id}`])
      }
    })
    await device.db.execute("INSERT INTO notes (id, body) VALUES (100, 'the next transaction')")
    const sampler = sampleCounts(server.path, 'SELECT count(*) AS n FROM notes WHERE id <= ?', [rowsInTransaction])

    const controller = device.sync({ batchSize: 5, pushIntervalMs: 10 })
    await controller.start()
    await eventually(() => countOf(server.path, 'notes') === rowsInTransaction + 1, 'every row reaching the server')
    await eventually(async () => (await controller.status()).pushCaughtUp, 'the outbox draining')
    const samples = sampler.stop()

    expect(countsBeforeEachBatch).toEqual([0, 0, 0, 0, 0])
    expect(samples.filter(count => count !== 0 && count !== rowsInTransaction)).toEqual([])
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    expect((await controller.status()).lastError).toBeNull()
  })

  it('keeps writes stamped by a clock running too far ahead until the server clock catches up', async () => {
    const server = await startSyncServer()
    const aheadPath = join(scratchDirectory('device-sync-clock'), 'device.db')
    const aheadMs = FIVE_MINUTES_MS + 3_000
    writeSqlite(aheadPath, [
      ['CREATE TABLE _sirannon_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)'],
      ['INSERT INTO _sirannon_meta (key, value) VALUES (?, ?)', ['node_id', DEVICE_NODE_ID]],
      ['INSERT INTO _sirannon_meta (key, value) VALUES (?, ?)', ['hlc_clock', hlcAhead(aheadMs, DEVICE_NODE_ID)]],
    ])
    const fast = await openDevice(server.url, { path: aheadPath })
    const nearlyPath = join(scratchDirectory('device-sync-clock'), 'device.db')
    const nearlyOtherNode = 'f00df00df00df00df00df00df00df00d'
    writeSqlite(nearlyPath, [
      ['CREATE TABLE _sirannon_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)'],
      ['INSERT INTO _sirannon_meta (key, value) VALUES (?, ?)', ['node_id', nearlyOtherNode]],
      [
        'INSERT INTO _sirannon_meta (key, value) VALUES (?, ?)',
        ['hlc_clock', hlcAhead(FIVE_MINUTES_MS - 30_000, nearlyOtherNode)],
      ],
    ])
    const nearly = await openDevice(server.url, { path: nearlyPath })
    await fast.db.execute("INSERT INTO notes (id, body) VALUES (1, 'stamped from the future')")
    await nearly.db.execute("INSERT INTO notes (id, body) VALUES (2, 'stamped just inside the limit')")

    const fastSync = fast.sync()
    const nearlySync = nearly.sync()
    await fastSync.start()
    await nearlySync.start()

    await eventually(async () => (await fastSync.status()).lastError?.code === 'DEVICE_CLOCK_AHEAD', 'the refusal')
    await eventually(() => countOf(server.path, 'notes') === 1, 'the write inside the limit arriving')
    const refused = await fastSync.status()
    expect([refused.pendingPushCount, refused.pushCaughtUp, refused.lastPushedSeq]).toEqual([1, false, 0n])
    expect(readSqlite(server.path, 'SELECT id FROM notes')).toEqual([{ id: 2n }])

    await eventually(
      () => countOf(server.path, 'notes') === 2,
      'the future write arriving once the clocks agree',
      20_000,
    )
    await eventually(async () => (await fastSync.status()).lastError === null, 'the refusal clearing')
    expect(readSqlite(server.path, 'SELECT body FROM notes WHERE id = 1')).toEqual([
      { body: 'stamped from the future' },
    ])
  }, 40_000)

  it('keeps the writes that the server refuses and pushes them once the server accepts them', async () => {
    let refusing = true
    const server = await startSyncServer({
      hooks: {
        onBeforePush: () => {
          if (refusing) throw new HookDeniedError('beforePush', 'This device is not yet registered')
        },
      },
    })
    const device = await openDevice(server.url)
    const controller = device.sync()
    await controller.start()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (1, 'held back')")

    await eventually(async () => (await controller.status()).lastError?.code === 'HOOK_DENIED', 'the refusal')
    await sleep(300)
    expect(countOf(server.path, 'notes')).toBe(0)
    expect((await controller.status()).pendingPushCount).toBe(1)

    refusing = false
    await eventually(() => countOf(server.path, 'notes') === 1, 'the held write arriving')
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  })
})
