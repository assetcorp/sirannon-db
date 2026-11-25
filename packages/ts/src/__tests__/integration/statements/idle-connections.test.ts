import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import {
  type ConnectionCountingDriver,
  connectionCountingDriver,
  heldReadSql,
} from '../../helpers/fault-drivers/connection-counting-driver.js'
import { createWorkspace, readRaw, type Workspace } from './harness.js'

const IDLE_TIMEOUT_MS = 250
const WRITE_GAP_MS = 25
const CLOSE_DEADLINE_MS = 5_000
const HELD_READS = 3
const READERS_AFTER_HELD_READS = 2
const SEEDED_ORDERS = 5

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('idle-connections')
})

afterEach(async () => {
  await workspace.dispose()
})

function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function openOrders(counting: ConnectionCountingDriver, name: string): Promise<Database> {
  const sirannon = workspace.registry({ driver: counting.driver })
  const db = await sirannon.open(name, workspace.path(name), {
    connectionIdleTimeout: IDLE_TIMEOUT_MS,
    readPoolSize: 4,
  })
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
  for (let id = 1; id <= SEEDED_ORDERS; id++) {
    await db.execute('INSERT INTO orders (id, total) VALUES (?, ?)', [id, id * 10])
  }
  return db
}

function ordersInFile(path: string): number[][] {
  return readRaw<{ id: bigint; total: bigint }>(path, 'SELECT id, total FROM orders ORDER BY id').map(row => [
    Number(row.id),
    Number(row.total),
  ])
}

function ordersRead(rows: readonly { id: number; total: number }[]): number[][] {
  return rows.map(row => [Number(row.id), Number(row.total)])
}

async function insertOrder(db: Database, id: number): Promise<void> {
  await db.execute('INSERT INTO orders (id, total) VALUES (?, ?)', [id, id * 10])
}

describe('idle connections of a database that keeps writing', () => {
  it('closes every reader once the idle timeout passes without a read, keeps the writer open while writes continue, and closes it once they stop', async () => {
    const counting = connectionCountingDriver()
    const db = await openOrders(counting, 'write-only')
    const held = counting.holdReads()
    const reads = Array.from({ length: HELD_READS }, () =>
      db.query<{ id: number; total: number }>(heldReadSql('SELECT id, total FROM orders ORDER BY id')),
    )
    await held.reached
    await vi.waitFor(() => expect(counting.openReaders()).toBe(READERS_AFTER_HELD_READS), {
      timeout: CLOSE_DEADLINE_MS,
      interval: 10,
    })
    held.release()
    for (const rows of await Promise.all(reads)) {
      expect(ordersRead(rows)).toEqual(ordersInFile(workspace.path('write-only')))
    }

    const deadline = performance.now() + CLOSE_DEADLINE_MS
    let nextId = SEEDED_ORDERS + 1
    while (counting.openReaders() > 0) {
      if (performance.now() > deadline) throw new Error('the database kept its readers open through every write')
      await insertOrder(db, nextId)
      nextId++
      await pause(WRITE_GAP_MS)
    }
    expect(counting.openWriters()).toBe(1)
    expect(counting.writerOpens()).toBe(1)

    await vi.waitFor(() => expect(counting.openWriters()).toBe(0), { timeout: CLOSE_DEADLINE_MS, interval: 25 })
    expect(counting.openReaders()).toBe(0)

    const rows = await db.query<{ id: number; total: number }>('SELECT id, total FROM orders ORDER BY id')
    const committed = ordersInFile(workspace.path('write-only'))
    expect(committed).toHaveLength(nextId - 1)
    expect(ordersRead(rows)).toEqual(committed)
    await insertOrder(db, nextId)
    expect(ordersInFile(workspace.path('write-only'))).toHaveLength(nextId)
  })

  it('keeps the reader under a read that outlasts the idle timeout while writes continue, and the read returns every row', async () => {
    const counting = connectionCountingDriver()
    const db = await openOrders(counting, 'long-read')
    const held = counting.holdReads()
    const read = db.query<{ id: number; total: number }>(heldReadSql('SELECT id, total FROM orders ORDER BY id'))
    await held.reached
    expect(counting.openReaders()).toBe(1)

    const writesEndAt = performance.now() + IDLE_TIMEOUT_MS * 3
    let nextId = SEEDED_ORDERS + 1
    while (performance.now() < writesEndAt) {
      await insertOrder(db, nextId)
      nextId++
      await pause(WRITE_GAP_MS)
    }
    expect(counting.openReaders()).toBe(1)

    held.release()
    const committed = ordersInFile(workspace.path('long-read'))
    expect(committed).toHaveLength(nextId - 1)
    expect(ordersRead(await read)).toEqual(committed)
  })
})
