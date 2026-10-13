import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { CDCMetrics } from '../../../core/types.js'
import { postJson } from '../lib/served-registry.js'
import {
  type ChangeLogRow,
  changeLog,
  pause,
  QUIET_PERIOD_MS,
  releaseChangeStreams,
  serveDatabase,
  type WireEvent,
  WireSocket,
  waitFor,
} from './harness.js'

const ORDERS_TABLE = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, owner INTEGER NOT NULL, status TEXT NOT NULL)'
const AUDIT_TABLE = 'CREATE TABLE audit (id INTEGER PRIMARY KEY, entry TEXT NOT NULL)'
const READERS = 24
const WRITERS = 8
const OPERATIONS_PER_WRITER = 30
const OWNERS = 4
const IDS_PER_WRITER = 1_000
const BUSY_DEADLINE_MS = 30_000

type Row = Record<string, unknown>

interface Delivered {
  type: string
  seq: string
  row: Row
  oldRow: Row | undefined
}

afterEach(releaseChangeStreams)

function matches(row: Row | undefined, filter: Row): boolean {
  if (row === undefined) return false
  return Object.entries(filter).every(([column, value]) => Object.hasOwn(row, column) && row[column] === value)
}

function expectedThroughFilter(log: readonly ChangeLogRow[], filter: Row | undefined): Delivered[] {
  const delivered: Delivered[] = []
  for (const entry of log) {
    const after = entry.new_data === null ? undefined : (JSON.parse(entry.new_data) as Row)
    const before = entry.old_data === null ? undefined : (JSON.parse(entry.old_data) as Row)
    const type = entry.operation.toLowerCase()
    const seq = entry.seq.toString()
    if (filter === undefined) {
      delivered.push({ type, seq, row: after ?? {}, oldRow: before })
      continue
    }
    const wasIn = matches(before, filter)
    const isIn = matches(after, filter)
    if (wasIn && isIn) delivered.push({ type, seq, row: after ?? {}, oldRow: before })
    else if (wasIn) delivered.push({ type: 'delete', seq, row: {}, oldRow: before })
    else if (isIn) delivered.push({ type: 'insert', seq, row: after ?? {}, oldRow: undefined })
  }
  return delivered
}

function asDelivered(events: readonly WireEvent[]): Delivered[] {
  return events.map(event => ({ type: event.type, seq: event.seq, row: event.row, oldRow: event.oldRow }))
}

function writerStatements(writer: number): Array<{ sql: string; params: unknown[] }> {
  const statements: Array<{ sql: string; params: unknown[] }> = []
  const live: number[] = []
  let next = writer * IDS_PER_WRITER
  for (let step = 0; step < OPERATIONS_PER_WRITER; step++) {
    const choice = (writer * 7 + step * 13) % 5
    const target = live[(writer + step) % Math.max(live.length, 1)]
    if (choice <= 1 || target === undefined) {
      next++
      live.push(next)
      statements.push({
        sql: 'INSERT INTO orders (id, owner, status) VALUES (?, ?, ?)',
        params: [next, (writer + step) % OWNERS, step % 2 === 0 ? 'open' : 'paid'],
      })
    } else if (choice === 2) {
      statements.push({
        sql: "UPDATE orders SET status = iif(status = 'open', 'paid', 'open') WHERE id = ?",
        params: [target],
      })
    } else if (choice === 3) {
      statements.push({ sql: 'UPDATE orders SET owner = ? WHERE id = ?', params: [(step + 1) % OWNERS, target] })
    } else {
      live.splice(live.indexOf(target), 1)
      statements.push({ sql: 'DELETE FROM orders WHERE id = ?', params: [target] })
    }
    if (step % 10 === 0) {
      statements.push({ sql: 'INSERT INTO audit (entry) VALUES (?)', params: [`writer ${writer} step ${step}`] })
    }
  }
  return statements
}

describe('subscriptions on many connections', () => {
  it('keeps each connection to its own subscriptions and database, so closing or unsubscribing one leaves the rest delivering', async () => {
    const cdcEvents: CDCMetrics[] = []
    const east = await serveDatabase({
      label: 'isolation',
      databaseId: 'east',
      schema: [ORDERS_TABLE],
      registry: { metrics: { onCDCEvent: event => cdcEvents.push(event) } },
    })
    const westDb = await east.sirannon.open('west', join(east.dir, 'west.db'))
    await westDb.execute(ORDERS_TABLE)
    const [first, second, third] = await Promise.all(
      [1, 2, 3].map(() => WireSocket.open(`${east.served.wsBase}/db/east`)),
    )
    const westward = await WireSocket.open(`${east.served.wsBase}/db/west`)
    if (first === undefined || second === undefined || third === undefined) throw new Error('three sockets expected')
    for (const socket of [first, second, third, westward]) {
      await socket.request({ id: 'feed', type: 'subscribe', table: 'orders' })
    }

    await postJson(east.served.base, '/db/east/execute', {
      sql: "INSERT INTO orders (owner, status) VALUES (1, 'east')",
    })
    await postJson(east.served.base, '/db/west/execute', {
      sql: "INSERT INTO orders (owner, status) VALUES (2, 'west')",
    })
    await Promise.all([first, second, third, westward].map(socket => socket.waitForEvents('feed', 1)))
    first.close()
    await first.closed
    await second.request({ id: 'feed', type: 'unsubscribe' })
    await pause(QUIET_PERIOD_MS)
    await postJson(east.served.base, '/db/east/execute', {
      sql: "INSERT INTO orders (owner, status) VALUES (3, 'later')",
    })
    await third.waitForEvents('feed', 2)
    await pause(QUIET_PERIOD_MS)

    const statuses = (socket: WireSocket) => socket.events('feed').map(event => event.row.status)
    expect([statuses(first), statuses(second), statuses(third), statuses(westward)]).toEqual([
      ['east'],
      ['east'],
      ['east', 'later'],
      ['west'],
    ])
    expect(cdcEvents.map(event => [event.databaseId, event.subscriberCount])).toEqual([
      ['east', 3],
      ['west', 1],
      ['east', 1],
    ])
  })

  it('delivers each change exactly once to every subscription of many connections while many clients write at once', async () => {
    const served = await serveDatabase({
      label: 'many-connections',
      databaseId: 'shop',
      schema: [ORDERS_TABLE, AUDIT_TABLE],
    })
    const filters: Array<[id: string, table: string, filter: Row | undefined]> = [
      ['every-order', 'orders', undefined],
      ['open-orders', 'orders', { status: 'open' }],
      ['audit', 'audit', undefined],
    ]
    const readers = await Promise.all(
      Array.from({ length: READERS }, async (_, reader) => {
        const socket = await WireSocket.open(`${served.served.wsBase}/db/shop`)
        const owned: Array<[id: string, table: string, filter: Row | undefined]> = [
          ...filters,
          ['owned', 'orders', { owner: reader % OWNERS }],
        ]
        await Promise.all(owned.map(([id, table, filter]) => socket.request({ id, type: 'subscribe', table, filter })))
        return { socket, owned }
      }),
    )

    await Promise.all(
      Array.from({ length: WRITERS }, async (_, writer) => {
        const statements = writerStatements(writer)
        if (writer % 2 === 0) {
          for (const statement of statements) {
            const answer = await postJson(served.served.base, '/db/shop/execute', statement)
            expect(answer.status, answer.text).toBe(200)
          }
          return
        }
        const socket = await WireSocket.open(`${served.served.wsBase}/db/shop`)
        for (const [index, statement] of statements.entries()) {
          const reply = await socket.request({ id: `write-${index}`, type: 'execute', ...statement })
          expect(reply.type, JSON.stringify(reply)).toBe('result')
        }
      }),
    )
    const ordersLog = changeLog(served.path, 'orders')
    const auditLog = changeLog(served.path, 'audit')
    await waitFor(
      () => readers.every(({ socket }) => socket.events('every-order').length >= ordersLog.length),
      'every reader to receive every order change',
      BUSY_DEADLINE_MS,
    )
    await pause(QUIET_PERIOD_MS)

    expect(ordersLog.length).toBeGreaterThan(WRITERS * OPERATIONS_PER_WRITER - 1)
    for (const [reader, { socket, owned }] of readers.entries()) {
      for (const [id, table, filter] of owned) {
        const log = table === 'audit' ? auditLog : ordersLog
        expect(asDelivered(socket.events(id)), `reader ${reader} ${id}`).toEqual(expectedThroughFilter(log, filter))
      }
    }
  })
})
