import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ChangeEvent, SubscriptionBuilder } from '../../../core/types.js'
import { type Collected, collect, createWorkspace, type Workspace, waitForCount } from './harness.js'

type Ticket = Record<string, unknown> & { id: number }

interface Delivery {
  type: string
  rowId: string | undefined
  row: Record<string, unknown>
  oldRow: Record<string, unknown> | undefined
}

interface FilterCase {
  label: string
  conditions: Record<string, unknown>
  build: (builder: SubscriptionBuilder) => SubscriptionBuilder
}

const HUGE_POINTS = 2n ** 60n
const STATUSES = ['open', 'closed', null]
const OWNERS = [3, 4, 5]
const POINTS = [1, HUGE_POINTS, 7, 7.5]
const TAGS = [Buffer.from([0, 255]), Buffer.from([0, 254]), Buffer.from([0]), null]
const OPERATIONS = 400

function plain(conditions: Record<string, unknown>): FilterCase['build'] {
  return builder => builder.filter(conditions)
}

const FILTERS: FilterCase[] = [
  { label: 'open tickets', conditions: { status: 'open' }, build: plain({ status: 'open' }) },
  {
    label: 'open tickets of owner 4',
    conditions: { status: 'open', owner: 4 },
    build: plain({ status: 'open', owner: 4 }),
  },
  {
    label: 'open tickets of owner 4 through two filter calls',
    conditions: { status: 'open', owner: 4 },
    build: builder => builder.filter({ status: 'open' }).filter({ owner: 4 }),
  },
  { label: 'owner 4 written as a bigint', conditions: { owner: 4n }, build: plain({ owner: 4n }) },
  {
    label: 'huge points written as a number',
    conditions: { points: 2 ** 60 },
    build: plain({ points: 2 ** 60 }),
  },
  {
    label: 'huge points written as a bigint',
    conditions: { points: HUGE_POINTS },
    build: plain({ points: HUGE_POINTS }),
  },
  { label: 'seven points written as a bigint', conditions: { points: 7n }, build: plain({ points: 7n }) },
  { label: 'seven and a half points', conditions: { points: 7.5 }, build: plain({ points: 7.5 }) },
  {
    label: 'tag bytes 00 FF',
    conditions: { tag: Uint8Array.from([0, 255]) },
    build: plain({ tag: Uint8Array.from([0, 255]) }),
  },
  { label: 'tickets with no status', conditions: { status: null }, build: plain({ status: null }) },
  { label: 'a column the table lacks', conditions: { priority: 1 }, build: plain({ priority: 1 }) },
]

function sameValue(stored: unknown, wanted: unknown): boolean {
  if (stored === null || wanted === null) return stored === wanted
  const storedInteger = typeof stored === 'bigint' || (typeof stored === 'number' && Number.isInteger(stored))
  const wantedInteger = typeof wanted === 'bigint' || (typeof wanted === 'number' && Number.isInteger(wanted))
  if (storedInteger && wantedInteger) return BigInt(stored as number | bigint) === BigInt(wanted as number | bigint)
  if (stored instanceof Uint8Array && wanted instanceof Uint8Array) {
    return Buffer.from(stored).equals(Buffer.from(wanted))
  }
  return stored === wanted
}

function matches(row: Ticket | undefined, conditions: Record<string, unknown>): boolean {
  if (row === undefined) return false
  return Object.entries(conditions).every(
    ([column, wanted]) => Object.hasOwn(row, column) && sameValue(row[column], wanted),
  )
}

function expectedFor(
  conditions: Record<string, unknown>,
  type: string,
  before: Ticket | undefined,
  after: Ticket | undefined,
): Delivery | null {
  const matchedBefore = matches(before, conditions)
  const matchedAfter = matches(after, conditions)
  const rowId = String((after ?? before)?.id)
  if (matchedBefore && matchedAfter) return { type, rowId, row: after ?? {}, oldRow: before }
  if (matchedBefore) return { type: 'delete', rowId, row: {}, oldRow: before }
  if (matchedAfter) return { type: 'insert', rowId, row: after ?? {}, oldRow: undefined }
  return null
}

function nextRandom(state: { seed: number }): number {
  state.seed = (state.seed * 1103515245 + 12345) % 2147483648
  return Math.floor(state.seed / 65536)
}

function pick<T>(state: { seed: number }, values: readonly T[]): T {
  return values[nextRandom(state) % values.length]
}

function comparable(delivery: Delivery): unknown {
  const encode = (row: Record<string, unknown> | undefined) =>
    row === undefined
      ? undefined
      : Object.fromEntries(
          Object.entries(row).map(([column, value]) => [
            column,
            value instanceof Uint8Array ? `bytes:${Buffer.from(value).toString('hex')}` : value,
          ]),
        )
  return { ...delivery, row: encode(delivery.row), oldRow: encode(delivery.oldRow) }
}

function delivered(event: ChangeEvent): Delivery {
  return { type: event.type, rowId: event.rowId, row: event.row, oldRow: event.oldRow }
}

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('filters')
})

afterEach(async () => {
  await workspace.dispose()
})

describe('filtered subscriptions', () => {
  it('deliver a row leaving the filter as a delete, a row entering it as an insert, and nothing for rows outside it', async () => {
    const db = await workspace.open('tickets')
    await db.execute(
      'CREATE TABLE tickets (id INTEGER PRIMARY KEY, status TEXT, owner INTEGER, points INTEGER, tag BLOB)',
    )
    await db.watch('tickets')
    const filtered: Collected[] = FILTERS.map(filter => collect(filter.build(db.on('tickets'))))
    const everything = collect(db.on('tickets'))

    const model = new Map<number, Ticket>()
    const changes: Array<{ type: string; before: Ticket | undefined; after: Ticket | undefined }> = []
    const random = { seed: 20261012 }
    let nextId = 1

    for (let operation = 0; operation < OPERATIONS; operation++) {
      const choice = nextRandom(random) % 10
      const ids = [...model.keys()].sort((a, b) => a - b)
      if (choice < 4 || ids.length === 0) {
        const after: Ticket = {
          id: nextId++,
          status: pick(random, STATUSES),
          owner: pick(random, OWNERS),
          points: pick(random, POINTS),
          tag: pick(random, TAGS),
        }
        await db.execute('INSERT INTO tickets VALUES (?, ?, ?, ?, ?)', [
          after.id,
          after.status,
          after.owner,
          after.points,
          after.tag,
        ])
        model.set(after.id, after)
        changes.push({ type: 'insert', before: undefined, after })
      } else if (choice < 8) {
        const before = model.get(pick(random, ids)) as Ticket
        const column = pick(random, ['status', 'owner', 'points', 'tag'] as const)
        const value = pick<unknown>(
          random,
          { status: STATUSES, owner: OWNERS, points: POINTS, tag: TAGS }[column] as readonly unknown[],
        )
        const after: Ticket = { ...before, [column]: value }
        await db.execute(`UPDATE tickets SET ${column} = ? WHERE id = ?`, [value, before.id])
        model.set(after.id, after)
        changes.push({ type: 'update', before, after })
      } else if (choice < 9) {
        const owner = pick(random, OWNERS)
        await db.execute("UPDATE tickets SET status = 'closed' WHERE owner = ?", [owner])
        for (const id of ids) {
          const before = model.get(id) as Ticket
          if (before.owner !== owner) continue
          const after: Ticket = { ...before, status: 'closed' }
          model.set(id, after)
          changes.push({ type: 'update', before, after })
        }
      } else {
        const before = model.get(pick(random, ids)) as Ticket
        await db.execute('DELETE FROM tickets WHERE id = ?', [before.id])
        model.delete(before.id)
        changes.push({ type: 'delete', before, after: undefined })
      }
    }
    await waitForCount(everything, changes.length)

    expect(everything.events.map(event => comparable(delivered(event)))).toEqual(
      changes.map(change =>
        comparable({
          type: change.type,
          rowId: String((change.after ?? change.before)?.id),
          row: change.after ?? {},
          oldRow: change.before,
        }),
      ),
    )
    const bySeq = new Map(everything.events.map(event => [event.seq, event]))
    for (const [index, filter] of FILTERS.entries()) {
      const wanted = changes
        .map(change => expectedFor(filter.conditions, change.type, change.before, change.after))
        .filter((delivery): delivery is Delivery => delivery !== null)
      expect(
        filtered[index].events.map(event => comparable(delivered(event))),
        filter.label,
      ).toEqual(wanted.map(comparable))
      for (const event of filtered[index].events) {
        const original = bySeq.get(event.seq)
        expect(original, filter.label).toBeDefined()
        expect([event.table, event.timestamp, event.txId], filter.label).toEqual([
          original?.table,
          original?.timestamp,
          original?.txId,
        ])
      }
    }
    expect(filtered.map(collected => collected.events.length > 0)).toEqual(
      FILTERS.map(filter => filter.label !== 'a column the table lacks'),
    )
  })
})
