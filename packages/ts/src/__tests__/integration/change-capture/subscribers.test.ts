import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ChangeEvent, Subscription } from '../../../core/types.js'
import {
  collect,
  createWorkspace,
  pause,
  QUIET_PERIOD_MS,
  type RejectionWatch,
  settleCallbacks,
  type Workspace,
  waitFor,
  waitForCount,
  watchUnhandledRejections,
  writeRaw,
} from './harness.js'

const RESUBSCRIBE_GUARD = 50

let workspace: Workspace
let rejections: RejectionWatch

beforeEach(() => {
  workspace = createWorkspace('subscribers')
  rejections = watchUnhandledRejections()
})

afterEach(async () => {
  rejections.stop()
  await workspace.dispose()
})

async function openOrders(name: string, options?: { cdcPollInterval?: number }) {
  const db = await workspace.open(name, options)
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)')
  await db.watch('orders')
  return db
}

describe('subscribers that fail', () => {
  it('cannot stop delivery to the others, and each failure reaches only its own onError', async () => {
    const db = await openOrders('failing')
    const healthyFirst = collect(db.on('orders'))
    const syncFailures: string[] = []
    const asyncFailures: string[] = []
    const thrownTextFailures: Array<[boolean, string]> = []
    const stalled: ChangeEvent[] = []

    db.on('orders').subscribe(
      () => {
        throw new Error('sync failure')
      },
      { onError: error => syncFailures.push(error.message) },
    )
    db.on('orders').subscribe(async () => Promise.reject(new Error('async failure')), {
      onError: error => asyncFailures.push(error.message),
    })
    db.on('orders').subscribe(
      () => {
        throw 'thrown text'
      },
      { onError: error => thrownTextFailures.push([error instanceof Error, error.message]) },
    )
    db.on('orders').subscribe(() => {
      throw new Error('nobody listens')
    })
    db.on('orders').subscribe(async () => Promise.reject(new Error('nobody listens either')))
    db.on('orders').subscribe(
      () => {
        throw new Error('first failure')
      },
      {
        onError: () => {
          throw new Error('the reporter fails too')
        },
      },
    )
    db.on('orders').subscribe(event => {
      stalled.push(event)
      return new Promise(() => {})
    })
    const healthyLast = collect(db.on('orders'))

    const items = ['tea', 'cocoa', 'shea', 'kente', 'gari']
    for (const item of items) await db.execute('INSERT INTO orders (item) VALUES (?)', [item])
    await waitForCount(healthyLast, items.length)
    await settleCallbacks()

    for (const collected of [healthyFirst, healthyLast]) {
      expect(collected.events.map(event => event.row.item)).toEqual(items)
      expect(collected.errors).toEqual([])
    }
    expect(stalled.map(event => event.row.item)).toEqual(items)
    expect(syncFailures).toEqual(items.map(() => 'sync failure'))
    expect(asyncFailures).toEqual(items.map(() => 'async failure'))
    expect(thrownTextFailures).toEqual(items.map(() => [true, 'thrown text']))
    expect(rejections.reasons).toEqual([])
  })
})

describe('many subscribers', () => {
  it('each receive every change to their own row once and nothing else', async () => {
    const db = await openOrders('crowd')
    const rows = 300
    const own = Array.from({ length: rows }, (_, index) => collect(db.on('orders').filter({ id: index + 1 })))
    const everyone = [collect(db.on('orders')), collect(db.on('orders')), collect(db.on('orders'))]

    await db.executeBatch(
      'INSERT INTO orders (id, item) VALUES (?, ?)',
      Array.from({ length: rows }, (_, index) => [index + 1, `item-${index + 1}`]),
    )
    await db.execute("UPDATE orders SET item = item || '-shipped'")
    await db.execute('DELETE FROM orders WHERE id % 2 = 0')
    await waitForCount(everyone[2], rows * 2 + rows / 2)

    for (const [index, collected] of own.entries()) {
      const id = index + 1
      const expected = [
        ['insert', { id, item: `item-${id}` }, undefined],
        ['update', { id, item: `item-${id}-shipped` }, { id, item: `item-${id}` }],
        ...(id % 2 === 0 ? [['delete', {}, { id, item: `item-${id}-shipped` }]] : []),
      ]
      expect(
        collected.events.map(event => [event.type, event.row, event.oldRow]),
        `subscriber of row ${id}`,
      ).toEqual(expected)
    }
    for (const collected of everyone) expect(collected.events).toHaveLength(rows * 2 + rows / 2)
  })
})

describe('subscribing and unsubscribing during delivery', () => {
  it('takes effect from the next change, so each subscription receives each later change once', async () => {
    const db = await openOrders('reentrant')
    const passive = collect(db.on('orders'))

    const selfStopping: number[] = []
    const stopSelf: { subscription?: Subscription } = {}
    stopSelf.subscription = db.on('orders').subscribe(event => {
      selfStopping.push(Number(event.row.id))
      stopSelf.subscription?.unsubscribe()
    })

    const victim: number[] = []
    const victimHolder: { subscription?: Subscription } = {}
    db.on('orders').subscribe(() => victimHolder.subscription?.unsubscribe())
    victimHolder.subscription = db.on('orders').subscribe(event => victim.push(Number(event.row.id)))

    const resubscribed: number[] = []
    let current: Subscription | undefined
    const resubscribe = (event: ChangeEvent) => {
      resubscribed.push(Number(event.row.id))
      current?.unsubscribe()
      if (resubscribed.length < RESUBSCRIBE_GUARD) current = db.on('orders').subscribe(resubscribe)
    }
    current = db.on('orders').subscribe(resubscribe)

    const lateJoiner: number[] = []
    let joined = false
    db.on('orders').subscribe(() => {
      if (joined) return
      joined = true
      db.on('orders').subscribe(event => lateJoiner.push(Number(event.row.id)))
    })

    for (const id of [1, 2, 3]) {
      await db.execute('INSERT INTO orders (id, item) VALUES (?, ?)', [id, `item-${id}`])
      await waitFor(() => passive.events.length === id, `change ${id}`)
    }
    await pause(QUIET_PERIOD_MS)

    expect(() => stopSelf.subscription?.unsubscribe()).not.toThrow()
    expect(passive.events.map(event => event.row.id)).toEqual([1, 2, 3])
    expect(selfStopping).toEqual([1])
    expect(victim).toEqual([])
    expect(resubscribed).toEqual([1, 2, 3])
    expect(lateJoiner).toEqual([2, 3])
  })
})

describe('a change log that cannot be read', () => {
  it('stops polling after repeated failures and passes the last failure to the onError of every subscription', async () => {
    const db = await workspace.open('unreadable')
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)')
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
    await db.watch('orders')
    await db.watch('invoices')
    const orders = collect(db.on('orders'))
    const filteredOrders = collect(db.on('orders').filter({ item: 'tea' }))
    const invoices = collect(db.on('invoices'))
    db.on('invoices').subscribe(() => {})

    await db.execute("INSERT INTO orders (item) VALUES ('tea')")
    await waitForCount(orders, 1)
    writeRaw(
      workspace.path('unreadable'),
      'DROP TABLE _sirannon_changes; CREATE TABLE _sirannon_changes (seq INTEGER PRIMARY KEY)',
    )

    await waitFor(
      () => [orders, filteredOrders, invoices].every(collected => collected.errors.length > 0),
      'every subscription to hear of the failure',
    )
    await pause(QUIET_PERIOD_MS * 2)
    await settleCallbacks()

    for (const collected of [orders, filteredOrders, invoices]) {
      expect(collected.errors).toHaveLength(1)
      expect(collected.errors[0]).toBeInstanceOf(Error)
      expect(collected.errors[0].message).toMatch(/no such column/)
    }
    expect(orders.events.map(event => event.row.item)).toEqual(['tea'])
    expect(rejections.reasons).toEqual([])
  })

  it('recovers when the change log becomes readable again after a few failed polls', async () => {
    const db = await openOrders('flaky', { cdcPollInterval: 50 })
    const seen = collect(db.on('orders'))
    await db.execute("INSERT INTO orders (item) VALUES ('before')")
    await waitForCount(seen, 1)

    writeRaw(workspace.path('flaky'), 'ALTER TABLE _sirannon_changes RENAME TO _sirannon_changes_parked')
    await pause(160)
    writeRaw(workspace.path('flaky'), 'ALTER TABLE _sirannon_changes_parked RENAME TO _sirannon_changes')
    await db.execute("INSERT INTO orders (item) VALUES ('after')")
    await waitForCount(seen, 2)

    expect(seen.events.map(event => event.row.item)).toEqual(['before', 'after'])
    expect(seen.errors).toEqual([])
  })
})
