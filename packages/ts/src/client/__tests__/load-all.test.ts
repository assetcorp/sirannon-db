import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import type { BulkLoadDurability, Params } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { LoadResponse } from '../../server/protocol.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SirannonClient } from '../client.js'
import { RemoteDatabase } from '../database-proxy.js'
import type { RemoteSubscription, Transport } from '../types.js'

interface RecordedLoad {
  rowCount: number
  durability: BulkLoadDurability | undefined
  checkpoint: boolean | undefined
}

class RecordingTransport implements Transport {
  readonly loads: RecordedLoad[] = []

  async query(): Promise<never> {
    throw new Error('not used')
  }
  async execute(): Promise<never> {
    throw new Error('not used')
  }
  async transaction(): Promise<never> {
    throw new Error('not used')
  }
  async batch(): Promise<never> {
    throw new Error('not used')
  }
  async load(
    _sql: string,
    paramsBatch: Params[],
    durability?: BulkLoadDurability,
    checkpoint?: boolean,
  ): Promise<LoadResponse> {
    this.loads.push({ rowCount: paramsBatch.length, durability, checkpoint })
    return { rowsLoaded: paramsBatch.length, changes: paramsBatch.length }
  }
  async subscribe(): Promise<RemoteSubscription> {
    throw new Error('not used')
  }
  close(): void {}
}

function rowsUpTo(n: number): Params[] {
  return Array.from({ length: n }, (_, i) => [i + 1, `row${i + 1}`])
}

async function* asyncRows(n: number): AsyncGenerator<Params> {
  for (let i = 0; i < n; i++) {
    yield [i + 1, `row${i + 1}`]
  }
}

const INSERT = 'INSERT INTO t (id, name) VALUES (?, ?)'

describe('RemoteDatabase.loadAll batching', () => {
  it('batches a dataset and checkpoints only on the final batch', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    const summary = await db.loadAll(INSERT, rowsUpTo(2500), { batchSize: 1000 })

    expect(transport.loads.map(l => l.rowCount)).toEqual([1000, 1000, 500])
    expect(transport.loads.map(l => l.checkpoint)).toEqual([false, false, true])
    expect(summary).toEqual({ rowsLoaded: 2500, changes: 2500 })
  })

  it('checkpoints exactly once when the row count is a whole multiple of the batch size', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    await db.loadAll(INSERT, rowsUpTo(2000), { batchSize: 1000 })

    expect(transport.loads.map(l => l.rowCount)).toEqual([1000, 1000])
    expect(transport.loads.map(l => l.checkpoint)).toEqual([false, true])
    expect(transport.loads.filter(l => l.checkpoint === true)).toHaveLength(1)
  })

  it('sends one checkpointing batch when the dataset is smaller than a batch', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    await db.loadAll(INSERT, rowsUpTo(10), { batchSize: 1000 })

    expect(transport.loads).toEqual([{ rowCount: 10, durability: undefined, checkpoint: true }])
  })

  it('sends nothing and reports zero for an empty dataset', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    const summary = await db.loadAll(INSERT, [], { batchSize: 1000 })

    expect(transport.loads).toHaveLength(0)
    expect(summary).toEqual({ rowsLoaded: 0, changes: 0 })
  })

  it('never sends an empty batch to the transport', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    await db.loadAll(INSERT, rowsUpTo(3000), { batchSize: 1000 })

    expect(transport.loads.every(l => l.rowCount > 0)).toBe(true)
  })

  it('passes the requested durability to every batch', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    await db.loadAll(INSERT, rowsUpTo(2500), { batchSize: 1000, durability: 'normal' })

    expect(transport.loads.every(l => l.durability === 'normal')).toBe(true)
  })

  it('consumes an async iterable and still checkpoints only once, at the end', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    const summary = await db.loadAll(INSERT, asyncRows(2500), { batchSize: 1000 })

    expect(transport.loads.map(l => l.rowCount)).toEqual([1000, 1000, 500])
    expect(transport.loads.map(l => l.checkpoint)).toEqual([false, false, true])
    expect(summary).toEqual({ rowsLoaded: 2500, changes: 2500 })
  })

  it('rejects a non-positive or non-integer batch size before touching the transport', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('t', transport)

    for (const bad of [0, -1, 1.5]) {
      await expect(db.loadAll(INSERT, rowsUpTo(10), { batchSize: bad })).rejects.toMatchObject({
        code: 'INVALID_ARGUMENT',
      })
    }
    expect(transport.loads).toHaveLength(0)
  })
})

describe('RemoteDatabase.loadAll over a real server', () => {
  const driver = betterSqlite3()
  let tempDir: string
  let sirannon: Sirannon
  let server: SirannonServer
  let client: SirannonClient

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-load-all-'))
    sirannon = new Sirannon({ driver })
    const db = await sirannon.open('bench', join(tempDir, 'bench.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')

    server = createServer(sirannon, { port: 0 })
    await server.listen()
    client = new SirannonClient(`http://127.0.0.1:${server.listeningPort}`, { transport: 'http' })
  })

  afterEach(async () => {
    client.close()
    await server.close()
    await sirannon.shutdown()
    rmSync(tempDir, { recursive: true, force: true })
  })

  it('loads every row across batches and leaves measured writes at the configured durability', async () => {
    const remote = client.database('bench')

    const summary = await remote.loadAll(INSERT, rowsUpTo(4500), { batchSize: 1000, durability: 'off' })
    expect(summary.rowsLoaded).toBe(4500)

    const rows = await remote.query<{ count: number }>('SELECT COUNT(*) AS count FROM t')
    expect(rows[0].count).toBe(4500)

    const local = sirannon.get('bench')
    const level = await local?.transaction(async tx => {
      const pragma = await tx.query<{ synchronous: number }>('PRAGMA synchronous')
      return pragma[0].synchronous
    })
    expect(level).toBe(2)
  })
})
