import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { pause, waitFor } from './harness.js'

const registries: Sirannon[] = []

afterEach(async () => {
  for (const registry of registries.splice(0)) await registry.shutdown()
})

async function openOrders(driver: () => SQLiteDriver) {
  const path = join(mkdtempSync(join(tmpdir(), 'sirannon-callback-writes-')), 'orders.db')
  const registry = new Sirannon({ driver: driver() })
  registries.push(registry)
  const db = await registry.open('orders', path, { connectionIdleTimeout: 40, cdcPollInterval: 5 })
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
  await db.execute('CREATE TABLE receipts (id INTEGER PRIMARY KEY)')
  await db.watch('orders')
  return { db, path }
}

describe.each([
  { label: 'better-sqlite3', driver: betterSqlite3 },
  { label: 'node:sqlite', driver: nodeSqlite },
])('subscriber writes with $label', ({ driver }) => {
  it('reopens the writer when a subscription created inside a transaction receives an external write after idle', async () => {
    const { db, path } = await openOrders(driver)
    const failures: Error[] = []
    const completed: number[] = []
    await db.transaction(async () => {
      db.on('orders').subscribe(
        async event => {
          await db.execute('INSERT INTO receipts VALUES (?)', [event.row.id])
          completed.push(Number(event.row.id))
        },
        { onError: error => failures.push(error) },
      )
    })
    await pause(160)
    const oracle = new BetterSqlite3(path)
    try {
      oracle.prepare('INSERT INTO orders VALUES (1)').run()
      await waitFor(() => completed.length + failures.length > 0, 'the subscriber write to settle')
      expect(failures).toEqual([])
      expect(completed).toEqual([1])
      expect(oracle.prepare('SELECT id FROM receipts').all()).toEqual([{ id: 1 }])
    } finally {
      oracle.close()
    }
  })

  it('commits a subscriber write separately when the transaction that created the subscription rolls back', async () => {
    const { db, path } = await openOrders(driver)
    await db.execute('INSERT INTO orders VALUES (1)')
    let called = false
    let completed = false
    const failures: Error[] = []
    await expect(
      db.transaction(async () => {
        db.on('orders').subscribe(
          async event => {
            called = true
            await db.execute('INSERT INTO receipts VALUES (?)', [event.row.id])
            completed = true
          },
          { onError: error => failures.push(error) },
        )
        await waitFor(() => called, 'the subscriber callback to start')
        throw new Error('Cancel the application transaction')
      }),
    ).rejects.toThrow('Cancel the application transaction')
    await waitFor(() => completed || failures.length > 0, 'the subscriber write to settle')
    expect(failures).toEqual([])
    const oracle = new BetterSqlite3(path, { readonly: true })
    try {
      expect(oracle.prepare('SELECT id FROM receipts').all()).toEqual([{ id: 1 }])
    } finally {
      oracle.close()
    }
  })

  it('commits a subscriber write to another database separately from the transaction that subscribed', async () => {
    const source = await openOrders(driver)
    const target = await openOrders(driver === betterSqlite3 ? nodeSqlite : betterSqlite3)
    await source.db.execute('INSERT INTO orders VALUES (1)')
    let called = false
    let completed = false
    const failures: Error[] = []
    await expect(
      target.db.transaction(async () => {
        source.db.on('orders').subscribe(
          async event => {
            called = true
            await target.db.execute('INSERT INTO receipts VALUES (?)', [event.row.id])
            completed = true
          },
          { onError: error => failures.push(error) },
        )
        await waitFor(() => called, 'the subscriber callback to start')
        throw new Error('Cancel the target transaction')
      }),
    ).rejects.toThrow('Cancel the target transaction')
    await waitFor(() => completed || failures.length > 0, 'the subscriber write to settle')
    expect(failures).toEqual([])
    const oracle = new BetterSqlite3(target.path, { readonly: true })
    try {
      expect(oracle.prepare('SELECT id FROM receipts').all()).toEqual([{ id: 1 }])
    } finally {
      oracle.close()
    }
  })

  it('preserves both writer contexts for explicit nested transaction callbacks across databases', async () => {
    const outer = await openOrders(driver)
    const inner = await openOrders(driver === betterSqlite3 ? nodeSqlite : betterSqlite3)
    await expect(
      outer.db.transaction(async () => {
        await inner.db.transaction(async () => {
          await outer.db.execute('INSERT INTO receipts VALUES (1)')
          await inner.db.execute('INSERT INTO receipts VALUES (2)')
        })
        throw new Error('Cancel only the outer transaction')
      }),
    ).rejects.toThrow('Cancel only the outer transaction')
    for (const [path, expected] of [
      [outer.path, []],
      [inner.path, [{ id: 2 }]],
    ] as const) {
      const oracle = new BetterSqlite3(path, { readonly: true })
      try {
        expect(oracle.prepare('SELECT id FROM receipts').all()).toEqual(expected)
      } finally {
        oracle.close()
      }
    }
  })
})
