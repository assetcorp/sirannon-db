import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../sirannon.js'
import type { CDCMetrics, QueryMetrics } from '../../types.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-metrics-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

async function waitFor(condition: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before the deadline')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

describe('metrics callbacks', () => {
  it('reports each change event that reaches a database subscriber', async () => {
    const events: CDCMetrics[] = []
    const sirannon = new Sirannon({ driver: testDriver, metrics: { onCDCEvent: m => events.push(m) } })
    const db = await sirannon.open('main', join(tempDir, 'cdc.db'))
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
    await db.watch('orders')
    const subscription = db.on('orders').subscribe(() => {})

    await db.execute('INSERT INTO orders (id, total) VALUES (1, 4999)')
    await waitFor(() => events.length > 0)

    expect(events[0]).toEqual({ databaseId: 'main', table: 'orders', operation: 'insert', subscriberCount: 1 })
    subscription.unsubscribe()
    await sirannon.shutdown()
  })

  it('reports the rows that a read returns and the rows that a write changes', async () => {
    const queries: QueryMetrics[] = []
    const sirannon = new Sirannon({ driver: testDriver, metrics: { onQueryComplete: m => queries.push(m) } })
    const db = await sirannon.open('main', join(tempDir, 'query.db'))
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')

    await db.executeBatch('INSERT INTO orders (id, total) VALUES (?, ?)', [
      [1, 10],
      [2, 20],
      [3, 30],
    ])
    await db.execute('UPDATE orders SET total = total + 1 WHERE id < 3')
    await db.query('SELECT * FROM orders')
    await db.queryOne('SELECT * FROM orders WHERE id = 99')

    const bySql = (sql: string) => queries.find(m => m.sql === sql)
    expect(bySql('INSERT INTO orders (id, total) VALUES (?, ?)')?.changes).toBe(3)
    expect(bySql('UPDATE orders SET total = total + 1 WHERE id < 3')?.changes).toBe(2)
    expect(bySql('SELECT * FROM orders')?.rowsReturned).toBe(3)
    expect(bySql('SELECT * FROM orders WHERE id = 99')?.rowsReturned).toBe(0)
    await sirannon.shutdown()
  })
})
