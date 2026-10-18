import BetterSqlite3 from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { Params } from '../../../core/types.js'
import { checkpointObservingDriver } from '../../helpers/fault-drivers/checkpoint-observing-driver.js'
import { countSqlite, readSqlite } from '../lib/served-registry.js'
import { DATABASE_ID, middlebox, recordingProxy, releaseClientServers, type ServedShop, servedShop } from './harness.js'

const INSERT_READING = 'INSERT INTO readings (id, value) VALUES (?, ?)'
const LOAD_ROUTE = /^\/db\/[^/]+\/load$/

interface LoadBody {
  paramsBatch: Params[]
  durability?: string
  checkpoint?: boolean
}

afterEach(releaseClientServers)

function readingRows(count: number, firstId: number): Array<[number, number]> {
  return Array.from({ length: count }, (_, index) => [firstId + index, (firstId + index) / 4])
}

async function* streamed(rows: Array<[number, number]>): AsyncGenerator<[number, number]> {
  for (const row of rows) yield row
}

function expectedBatchSizes(rows: number, batchSize: number): number[] {
  const sizes: number[] = []
  for (let sent = 0; sent < rows; sent += batchSize) sizes.push(Math.min(batchSize, rows - sent))
  return sizes
}

function storedReadings(path: string): Array<[number, number]> {
  return readSqlite<{ id: bigint; value: number }>(path, 'SELECT id, value FROM readings ORDER BY id').map(row => [
    Number(row.id),
    row.value,
  ])
}

async function readingsShop(rowsAtCheckpoint: number[], maxBodyBytes?: number): Promise<ServedShop> {
  let path = ''
  const shop = await servedShop({
    customers: [],
    registry: {
      driver: checkpointObservingDriver(() => {
        const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
        try {
          rowsAtCheckpoint.push((raw.prepare('SELECT count(*) AS n FROM readings').get() as { n: number }).n)
        } finally {
          raw.close()
        }
      }),
    },
    server: maxBodyBytes === undefined ? {} : { maxBodyBytes },
  })
  path = shop.path
  await shop.database.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL NOT NULL)')
  return shop
}

describe('loading a large dataset through the published client', () => {
  it('sends one request per batch, asks for the checkpoint on the last one only, and carries the durability on each', async () => {
    const rowsAtCheckpoint: number[] = []
    const shop = await readingsShop(rowsAtCheckpoint)
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, { transport: 'http' })
    const db = client.database(DATABASE_ID)
    const loads: Array<[label: string, rows: Array<[number, number]>, batchSize: number, asStream: boolean]> = [
      ['a dataset with a partial last batch', readingRows(2_500, 1), 1_000, false],
      ['a dataset that fills its last batch', readingRows(2_000, 10_001), 1_000, false],
      ['a dataset smaller than one batch', readingRows(10, 20_001), 1_000, false],
      ['a streamed dataset', readingRows(2_500, 30_001), 1_000, true],
      ['a batch of one row', readingRows(3, 40_001), 1, false],
      ['an empty dataset', [], 1_000, false],
    ]
    const expectedRows: Array<[number, number]> = []

    for (const [label, rows, batchSize, asStream] of loads) {
      const sentBefore = proxy.requestsTo('POST', LOAD_ROUTE).length
      const checkpointsBefore = rowsAtCheckpoint.length
      const summary = await db.loadAll(INSERT_READING, asStream ? streamed(rows) : rows, {
        batchSize,
        durability: 'normal',
      })
      const bodies = proxy
        .requestsTo('POST', LOAD_ROUTE)
        .slice(sentBefore)
        .map(request => request.body as LoadBody)
      expectedRows.push(...rows)

      expect(summary, label).toEqual({ rowsLoaded: rows.length, changes: rows.length })
      expect(
        bodies.map(body => body.paramsBatch.length),
        label,
      ).toEqual(expectedBatchSizes(rows.length, batchSize))
      expect(
        bodies.map(body => body.checkpoint),
        label,
      ).toEqual(bodies.map((_, index) => index === bodies.length - 1))
      expect(
        bodies.map(body => body.durability),
        label,
      ).toEqual(bodies.map(() => 'normal'))
      expect(
        bodies.flatMap(body => body.paramsBatch),
        label,
      ).toEqual(rows)
      expect(rowsAtCheckpoint.slice(checkpointsBefore), label).toEqual(rows.length === 0 ? [] : [expectedRows.length])
    }
    client.close()

    expect(storedReadings(shop.path)).toEqual(expectedRows)
  })

  it('checkpoints once, after every row is stored, when it loads over WebSocket', async () => {
    const rowsAtCheckpoint: number[] = []
    const shop = await readingsShop(rowsAtCheckpoint)
    const box = await middlebox(shop.served.port)
    const client = new SirannonClient(box.base, { transport: 'websocket', autoReconnect: false })
    const rows = readingRows(4_500, 1)

    const summary = await client.database(DATABASE_ID).loadAll(INSERT_READING, streamed(rows), { batchSize: 1_000 })
    const frames = box.messages('client', 'load')
    client.close()

    expect(summary).toEqual({ rowsLoaded: 4_500, changes: 4_500 })
    expect(rowsAtCheckpoint).toEqual([4_500])
    expect(frames.map(frame => (frame.paramsBatch as unknown[]).length)).toEqual([1_000, 1_000, 1_000, 1_000, 500])
    expect(frames.map(frame => frame.checkpoint)).toEqual([false, false, false, false, true])
    expect(storedReadings(shop.path)).toEqual(rows)
  })

  it('refuses a batch size that is not a positive whole number before it sends anything', async () => {
    const shop = await readingsShop([])
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, { transport: 'http' })

    for (const batchSize of [0, -0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, -Number.MAX_SAFE_INTEGER]) {
      const failure = await client
        .database(DATABASE_ID)
        .loadAll(INSERT_READING, readingRows(10, 1), { batchSize })
        .then(
          () => null,
          (err: { code?: string }) => err.code,
        )
      expect(failure, String(batchSize)).toBe('INVALID_ARGUMENT')
    }
    client.close()

    expect(proxy.requests).toEqual([])
    expect(countSqlite(shop.path, 'readings')).toBe(0)
  })

  it('keeps every batch the server committed when a later batch is too large or the source fails', async () => {
    const shop = await readingsShop([], 16_384)
    const proxy = await recordingProxy(shop.served.port)
    const box = await middlebox(shop.served.port)
    const oversizedRow: [number, string] = [110, 'x'.repeat(20_000)]
    const withOversizedRow: Array<[number, number | string]> = readingRows(120, 1).map(row =>
      row[0] === 110 ? oversizedRow : row,
    )
    const sourceError = new Error('The sensor export stopped at row 2500')
    async function* failingSource(): AsyncGenerator<[number, number]> {
      for (const row of readingRows(2_499, 50_001)) yield row
      throw sourceError
    }

    const overHttp = new SirannonClient(proxy.base, { transport: 'http' })
    const httpFailure = await overHttp
      .database(DATABASE_ID)
      .loadAll(INSERT_READING, withOversizedRow, { batchSize: 50 })
      .then(
        () => null,
        (err: { code?: string }) => err.code,
      )
    const httpCommitted = countSqlite(shop.path, 'readings')
    const overWebSocket = new SirannonClient(box.base, { transport: 'websocket', autoReconnect: false })
    const wsFailure = await overWebSocket
      .database(DATABASE_ID)
      .loadAll(
        INSERT_READING,
        withOversizedRow.map(([id, value]) => [id + 1_000, value]),
        { batchSize: 50 },
      )
      .then(
        () => null,
        (err: { code?: string }) => err.code,
      )
    const wsCommitted = countSqlite(shop.path, 'readings') - httpCommitted
    const sentBeforeSourceFailure = proxy.requestsTo('POST', LOAD_ROUTE).length
    const thrown = await overHttp
      .database(DATABASE_ID)
      .loadAll(INSERT_READING, failingSource(), { batchSize: 1_000 })
      .then(
        () => null,
        (err: unknown) => err,
      )
    const sourceBodies = proxy
      .requestsTo('POST', LOAD_ROUTE)
      .slice(sentBeforeSourceFailure)
      .map(request => request.body as LoadBody)
    overHttp.close()
    overWebSocket.close()

    expect(httpFailure).toBe('PAYLOAD_TOO_LARGE')
    expect(httpCommitted).toBe(100)
    expect(wsFailure).toBe('CONNECTION_ERROR')
    expect(wsCommitted).toBe(100)
    expect(thrown).toBe(sourceError)
    expect(sourceBodies.every(body => body.checkpoint === false)).toBe(true)
    expect(
      readSqlite<{ id: bigint }>(shop.path, 'SELECT id FROM readings WHERE id > 50000 ORDER BY id').map(row =>
        Number(row.id),
      ),
    ).toEqual(sourceBodies.flatMap(body => body.paramsBatch.map(row => Number((row as unknown[])[0]))))
    expect(sourceBodies.flatMap(body => body.paramsBatch).length).toBeLessThan(2_499)
  })
})
