import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { Database } from '../../database.js'
import type { LiveQuery, LiveQueryState } from '../../live/live-query.js'
import { Sirannon } from '../../sirannon.js'

let tempDir: string
let sirannon: Sirannon
let db: Database

interface OrderRow {
  id: number
  status: string
  total: number
}

function nextState<T>(live: LiveQuery<T>, matches: (state: LiveQueryState<T>) => boolean): Promise<LiveQueryState<T>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      unsubscribe()
      reject(new Error(`live query never reached the expected state; last was ${JSON.stringify(live.getState())}`))
    }, 5000)
    const settle = () => {
      const state = live.getState()
      if (!matches(state)) return
      clearTimeout(timer)
      unsubscribe()
      resolve(state)
    }
    const unsubscribe = live.subscribe(settle)
    settle()
  })
}

function readyRows<T>(live: LiveQuery<T>, count: number): Promise<readonly T[]> {
  return nextState(live, state => state.status === 'ready' && state.rows.length === count).then(state => {
    if (state.status !== 'ready') throw new Error('expected a ready state')
    return state.rows
  })
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-live-query-'))
  sirannon = new Sirannon({ driver: betterSqlite3() })
  db = await sirannon.open('shop', join(tempDir, 'shop.db'), { cdcPollInterval: 10 })
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total INTEGER)')
  await db.watch('orders')
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Database.live', () => {
  it('starts pending and reports the rows the query already matched', async () => {
    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")

    const live = db.live<OrderRow>('SELECT * FROM orders ORDER BY id')
    expect(live.getState()).toEqual({ status: 'pending' })

    const rows = await readyRows(live, 1)

    expect(rows).toEqual([{ id: 1, status: 'open', total: 100 }])
    live.close()
  })

  it('picks up a row inserted after it opened', async () => {
    const live = db.live<OrderRow>('SELECT * FROM orders ORDER BY id')
    await readyRows(live, 0)

    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")

    const rows = await readyRows(live, 1)
    expect(rows.map(row => row.id)).toEqual([1])
    live.close()
  })

  it('drops a row that no longer matches the statement', async () => {
    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")
    await db.execute("INSERT INTO orders (id, status, total) VALUES (2, 'open', 200)")

    const live = db.live<OrderRow>("SELECT * FROM orders WHERE status = 'open' ORDER BY id")
    await readyRows(live, 2)

    await db.execute("UPDATE orders SET status = 'shipped' WHERE id = 1")

    const rows = await readyRows(live, 1)
    expect(rows.map(row => row.id)).toEqual([2])
    live.close()
  })

  it('keeps an ordered and limited statement correct as rows change', async () => {
    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")
    await db.execute("INSERT INTO orders (id, status, total) VALUES (2, 'open', 200)")

    const live = db.live<OrderRow>('SELECT * FROM orders ORDER BY total DESC LIMIT 1')
    const first = await readyRows(live, 1)
    expect(first.map(row => row.id)).toEqual([2])

    await db.execute("INSERT INTO orders (id, status, total) VALUES (3, 'open', 900)")

    const second = await nextState(live, state => state.status === 'ready' && state.rows[0]?.id === 3)
    expect(second.status).toBe('ready')
    live.close()
  })

  it('serves an aggregate that no row-level filter could express', async () => {
    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")

    const live = db.live<{ total: number }>('SELECT SUM(total) AS total FROM orders')
    await nextState(live, state => state.status === 'ready' && state.rows[0]?.total === 100)

    await db.execute("INSERT INTO orders (id, status, total) VALUES (2, 'open', 50)")

    const state = await nextState(live, s => s.status === 'ready' && s.rows[0]?.total === 150)
    expect(state.status).toBe('ready')
    live.close()
  })

  it('reports a failing statement as an error state rather than throwing', async () => {
    const live = db.live('SELECT * FROM missing_table')

    const state = await nextState(live, s => s.status === 'error')
    if (state.status !== 'error') throw new Error('expected an error state')
    expect(state.error.message).toContain('missing_table')
    live.close()
  })

  it('retries a failed statement when a watched table changes', async () => {
    const live = db.live<{ id: number }>('SELECT orders.id FROM orders JOIN invoices ON invoices.id = orders.id')
    await nextState(live, s => s.status === 'error')

    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY)')
    await db.execute('INSERT INTO invoices (id) VALUES (1)')
    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")

    const rows = await readyRows(live, 1)
    expect(rows.map(row => row.id)).toEqual([1])
    live.close()
  })

  it('refuses a table it was given that carries no change triggers', async () => {
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY)')

    expect(() => db.live('SELECT * FROM invoices', undefined, { tables: ['invoices'] })).toThrow(
      'carries no change triggers',
    )
  })

  it('stops reporting once closed', async () => {
    const live = db.live<OrderRow>('SELECT * FROM orders ORDER BY id')
    await readyRows(live, 0)

    let notifications = 0
    live.subscribe(() => {
      notifications++
    })
    live.close()

    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(notifications).toBe(0)
    expect(live.getState().status).toBe('ready')
  })

  it('refuses a live query when no table is watched and none is named', async () => {
    const bare = await sirannon.open('bare', join(tempDir, 'bare.db'))
    await bare.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')

    expect(() => bare.live('SELECT * FROM orders')).toThrow('watch the tables it reads')
  })

  it('watches only the tables it was given', async () => {
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY)')
    await db.watch('invoices')

    const live = db.live<OrderRow>('SELECT * FROM orders ORDER BY id', undefined, { tables: ['orders'] })
    await readyRows(live, 0)

    let notifications = 0
    live.subscribe(() => {
      notifications++
    })

    await db.execute('INSERT INTO invoices (id) VALUES (1)')
    await new Promise(resolve => setTimeout(resolve, 120))
    expect(notifications).toBe(0)

    await db.execute("INSERT INTO orders (id, status, total) VALUES (1, 'open', 100)")
    await readyRows(live, 1)
    live.close()
  })
})
