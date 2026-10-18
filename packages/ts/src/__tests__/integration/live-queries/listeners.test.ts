import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery, LiveUpdate } from '../../../core/live/types.js'
import {
  createWorkspace,
  pause,
  QUIET_PERIOD_MS,
  type RejectionWatch,
  readyRows,
  settleCallbacks,
  settlesToQuery,
  settleToQueries,
  type Workspace,
  waitFor,
  watchUnhandledRejections,
} from './harness.js'

const ORDERS = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL, total INTEGER NOT NULL)'
const ALL_ORDERS = 'SELECT id, status, total FROM orders ORDER BY id'

let workspace: Workspace
let rejections: RejectionWatch
const opened: LiveQuery<unknown>[] = []

beforeEach(() => {
  workspace = createWorkspace('listeners')
  rejections = watchUnhandledRejections()
})

afterEach(async () => {
  rejections.stop()
  for (const query of opened.splice(0)) await query.close().catch(() => {})
  await workspace.dispose()
})

async function openOrders(name: string): Promise<Database> {
  const db = await workspace.open(name)
  await db.execute(ORDERS)
  return db
}

function track<T>(query: LiveQuery<T>): LiveQuery<T> {
  opened.push(query as LiveQuery<unknown>)
  return query
}

describe('listeners of a live query', () => {
  it('keep receiving updates when another listener throws or rejects, and each failure reaches onError without an unhandled rejection', async () => {
    const db = await openOrders('failing')
    const reported: [boolean, string][] = []
    const query = track(
      await db.live(ALL_ORDERS, undefined, {
        onError: error => {
          reported.push([error instanceof Error, error.message])
          if (error.message === 'reporter trips too') throw new Error('onError failed')
        },
      }),
    )
    const silent = track(await db.live(ALL_ORDERS))
    const first: LiveUpdate<unknown>['kind'][] = []
    const last: LiveUpdate<unknown>['kind'][] = []
    const stalled: LiveUpdate<unknown>['kind'][] = []

    query.subscribe(update => first.push(update.kind))
    query.subscribe(() => {
      throw new Error('listener failed')
    })
    query.subscribe(async () => Promise.reject(new Error('async listener failed')))
    query.subscribe(() => {
      throw 'thrown text'
    })
    query.subscribe(() => {
      throw new Error('reporter trips too')
    })
    query.subscribe(update => {
      stalled.push(update.kind)
      return new Promise(() => {})
    })
    query.subscribe(update => last.push(update.kind))
    silent.subscribe(() => {
      throw new Error('nobody listens')
    })
    silent.subscribe(async () => Promise.reject(new Error('nobody listens either')))

    for (const id of [1, 2, 3]) await db.execute('INSERT INTO orders VALUES (?, ?, ?)', [id, 'open', id * 10])
    await settleToQueries(db, [
      { query, sql: ALL_ORDERS },
      { query: silent, sql: ALL_ORDERS },
    ])
    await settleCallbacks()

    const updates = first.length
    expect(updates).toBeGreaterThan(0)
    expect(last).toEqual(first)
    expect(stalled).toEqual(first)
    const perUpdate = [
      [true, 'listener failed'],
      [true, 'async listener failed'],
      [true, 'thrown text'],
      [true, 'reporter trips too'],
    ]
    expect(reported.map(entry => entry.join(':')).sort()).toEqual(
      first.flatMap(() => perUpdate.map(entry => entry.join(':'))).sort(),
    )
    expect(rejections.reasons).toEqual([])
  })

  it('may close the query or remove themselves while it calls them, and later updates reach only the listeners that remain', async () => {
    const db = await openOrders('reentrant')
    await db.execute("INSERT INTO orders VALUES (1, 'open', 10)")
    const closing = track(await db.live(ALL_ORDERS))
    const removing = track(await db.live(ALL_ORDERS))
    const afterClose: string[] = []
    const selfRemoved: string[] = []
    const kept: string[] = []

    closing.subscribe(() => {
      void closing.close()
    })
    closing.subscribe(update => afterClose.push(update.kind))
    const stop = removing.subscribe(update => {
      selfRemoved.push(update.kind)
      stop()
    })
    removing.subscribe(update => kept.push(update.kind))

    await db.execute("INSERT INTO orders VALUES (2, 'open', 20)")
    await settlesToQuery(removing, db, ALL_ORDERS)
    await db.execute("INSERT INTO orders VALUES (3, 'open', 30)")
    await db.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
    await settlesToQuery(removing, db, ALL_ORDERS)
    await settleCallbacks()

    expect(afterClose).toEqual(['ops'])
    expect(readyRows(closing)).toEqual([
      { id: 1, status: 'open', total: 10 },
      { id: 2, status: 'open', total: 20 },
    ])
    expect(selfRemoved).toEqual(['ops'])
    expect(kept).toEqual(['ops', 'ops', 'ops'])
    expect(() => stop()).not.toThrow()
    expect(rejections.reasons).toEqual([])
  })

  it('hear nothing once the query closes, even with a change already on its way, and a second close does nothing', async () => {
    const db = await openOrders('closed')
    await db.execute("INSERT INTO orders VALUES (1, 'open', 10)")
    const query = await db.live(ALL_ORDERS)
    let updates = 0
    query.subscribe(() => {
      updates++
    })

    await db.execute("INSERT INTO orders VALUES (2, 'open', 20)")
    await query.close()
    await expect(query.close()).resolves.toBeUndefined()
    await db.execute("INSERT INTO orders VALUES (3, 'open', 30)")
    await pause(QUIET_PERIOD_MS)

    expect(updates).toBe(0)
    expect(readyRows(query)).toEqual([{ id: 1, status: 'open', total: 10 }])
    expect(query.subscribe(() => {})).toBeTypeOf('function')
  })
})

describe('many live queries on one table', () => {
  it('each match their own statement through interleaved writes', async () => {
    const db = await openOrders('crowd')
    await db.executeBatch(
      'INSERT INTO orders VALUES (?, ?, ?)',
      Array.from({ length: 60 }, (_, index) => [index + 1, index % 3 === 0 ? 'paid' : 'open', (index * 37) % 101]),
    )
    const statements: [string, unknown[]][] = []
    for (let index = 0; index < 40; index++) {
      const status = index % 2 === 0 ? 'open' : 'paid'
      if (index % 4 === 0)
        statements.push(['SELECT id, total FROM orders WHERE status = ? ORDER BY total DESC, id', [status]])
      else if (index % 4 === 1)
        statements.push([`SELECT id FROM orders WHERE total >= ? ORDER BY id LIMIT ${index % 7}`, [index * 2]])
      else if (index % 4 === 2)
        statements.push([`SELECT id, status FROM orders ORDER BY id DESC LIMIT 5 OFFSET ${index % 5}`, []])
      else statements.push(['SELECT id, total FROM orders WHERE id % ? = 0 ORDER BY total, id', [(index % 6) + 1]])
    }
    const cases = []
    for (const [sql, params] of statements) {
      cases.push({ query: track(await db.live(sql, params, { rereadJitterMs: 5 })) as LiveQuery<unknown>, sql, params })
    }

    for (let round = 0; round < 6; round++) {
      await db.transaction(async tx => {
        await tx.execute('INSERT INTO orders VALUES (?, ?, ?)', [1000 + round, 'open', round * 13])
        await tx.execute('UPDATE orders SET total = total + ? WHERE id % 5 = ?', [round + 1, round % 5])
        await tx.execute("UPDATE orders SET status = 'paid' WHERE id = ?", [round * 7 + 2])
      })
      await db.execute('DELETE FROM orders WHERE id = ?', [round * 9 + 3])
      await settleToQueries(db, cases, 0)
    }
    await settleToQueries(db, cases)
  })
})

describe('a live query whose database closes', () => {
  it('stops changing, still answers getState and close, and the closed database refuses a new one', async () => {
    const db = await openOrders('closing-database')
    await db.execute("INSERT INTO orders VALUES (1, 'open', 10)")
    const query = await db.live(ALL_ORDERS)
    let updates = 0
    query.subscribe(() => {
      updates++
    })
    await db.execute("INSERT INTO orders VALUES (2, 'open', 20)")
    await waitFor(() => updates === 1, 'the insert to reach the query')

    await db.close()
    await pause(QUIET_PERIOD_MS)
    const rowsAtClose = readyRows(query)

    expect(rowsAtClose).toEqual([
      { id: 1, status: 'open', total: 10 },
      { id: 2, status: 'open', total: 20 },
    ])
    expect(updates).toBe(1)
    await expect(query.close()).resolves.toBeUndefined()
    await expect(db.live(ALL_ORDERS)).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    await settleCallbacks()
    expect(rejections.reasons).toEqual([])
  })
})
