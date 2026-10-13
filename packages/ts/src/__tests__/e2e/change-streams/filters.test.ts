import { afterEach, describe, expect, it } from 'vitest'
import { releaseChangeStreams, serveDatabase, type WireEvent, WireSocket, waitFor } from './harness.js'

type WireRow = Record<string, unknown> & { id: number }

interface Delivery {
  type: string
  rowId: string | undefined
  row: Record<string, unknown>
  oldRow: Record<string, unknown> | undefined
}

const OPERATIONS = 300
const RANDOM_SEED = 20261012
const DELIVERY_DEADLINE_MS = 30_000
const HUGE_POINTS = { __sirannon_int: '1152921504606846976' }
const PAST_SAFE_POINTS = { __sirannon_int: '9007199254740993' }
const STATUSES = ['open', 'closed', null]
const OWNERS = [3, 4, 5]
const POINTS: unknown[] = [1, HUGE_POINTS, 7, 7.5, PAST_SAFE_POINTS]
const TAGS: unknown[] = [{ __sirannon_blob: '00FF' }, { __sirannon_blob: '00FE' }, { __sirannon_blob: '' }, null]
const COLUMN_VALUES: Record<'status' | 'owner' | 'points' | 'tag', readonly unknown[]> = {
  status: STATUSES,
  owner: OWNERS,
  points: POINTS,
  tag: TAGS,
}

const FILTERS: Array<[label: string, filterJson: string]> = [
  ['open tickets', '{"status":"open"}'],
  ['open tickets of owner 4', '{"status":"open","owner":4}'],
  ['owner 4 sent as an integer envelope', '{"owner":{"__sirannon_int":"4"}}'],
  ['owner 4 sent as text', '{"owner":"4"}'],
  ['huge points sent as a plain number', '{"points":1152921504606846976}'],
  ['huge points sent as an envelope', '{"points":{"__sirannon_int":"1152921504606846976"}}'],
  ['points just past the safe range sent as an envelope', '{"points":{"__sirannon_int":"9007199254740993"}}'],
  ['points just past the safe range sent as a plain number', '{"points":9007199254740993}'],
  ['seven and a half points', '{"points":7.5}'],
  ['tag 00 FF', '{"tag":{"__sirannon_blob":"00FF"}}'],
  ['tag 00 FF in lower case', '{"tag":{"__sirannon_blob":"00ff"}}'],
  ['an empty tag', '{"tag":{"__sirannon_blob":""}}'],
  ['tickets with no status', '{"status":null}'],
  ['a column the table lacks', '{"priority":1}'],
  ['a list as a value', '{"status":["open"]}'],
  ['an empty filter', '{}'],
]

afterEach(releaseChangeStreams)

function integerOf(value: unknown): bigint | undefined {
  if (typeof value === 'number' && Number.isInteger(value)) return BigInt(value)
  if (typeof value === 'object' && value !== null && Object.keys(value).length === 1) {
    const digits = (value as { __sirannon_int?: unknown }).__sirannon_int
    if (typeof digits === 'string') return BigInt(digits)
  }
  return undefined
}

function bytesOf(value: unknown): string | undefined {
  if (typeof value !== 'object' || value === null || Object.keys(value).length !== 1) return undefined
  const hex = (value as { __sirannon_blob?: unknown }).__sirannon_blob
  return typeof hex === 'string' ? hex.toLowerCase() : undefined
}

function sameValue(stored: unknown, wanted: unknown): boolean {
  if (stored === null || wanted === null) return stored === wanted
  const storedInteger = integerOf(stored)
  const wantedInteger = integerOf(wanted)
  if (storedInteger !== undefined && wantedInteger !== undefined) return storedInteger === wantedInteger
  const storedBytes = bytesOf(stored)
  const wantedBytes = bytesOf(wanted)
  if (storedBytes !== undefined && wantedBytes !== undefined) return storedBytes === wantedBytes
  return typeof stored !== 'object' && stored === wanted
}

function matches(row: WireRow | undefined, filter: Record<string, unknown>): boolean {
  if (row === undefined) return false
  return Object.entries(filter).every(
    ([column, wanted]) => Object.hasOwn(row, column) && sameValue(row[column], wanted),
  )
}

function expectedFor(
  filter: Record<string, unknown>,
  type: string,
  before: WireRow | undefined,
  after: WireRow | undefined,
): Delivery | null {
  const rowId = String((after ?? before)?.id)
  const wasIn = matches(before, filter)
  const isIn = matches(after, filter)
  if (wasIn && isIn) return { type, rowId, row: after ?? {}, oldRow: before }
  if (wasIn) return { type: 'delete', rowId, row: {}, oldRow: before }
  if (isIn) return { type: 'insert', rowId, row: after ?? {}, oldRow: undefined }
  return null
}

function delivered(event: WireEvent): Delivery {
  return { type: event.type, rowId: event.rowId, row: event.row, oldRow: event.oldRow }
}

function nextRandom(state: { seed: number }): number {
  state.seed = (state.seed * 1103515245 + 12345) % 2147483648
  return Math.floor(state.seed / 65536)
}

function pick<T>(state: { seed: number }, values: readonly T[]): T {
  return values[nextRandom(state) % values.length] as T
}

describe('filtered subscriptions over the WebSocket protocol', () => {
  it('deliver a row leaving the filter as a delete, a row entering it as an insert, nothing for rows outside it, and the same on replay', async () => {
    const served = await serveDatabase({
      label: 'filters',
      databaseId: 'desk',
      schema: ['CREATE TABLE tickets (id INTEGER PRIMARY KEY, status TEXT, owner INTEGER, points INTEGER, tag BLOB)'],
    })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/desk`)
    const everything = await ws.request({ id: 'everything', type: 'subscribe', table: 'tickets' })
    for (const [index, [, filterJson]] of FILTERS.entries()) {
      ws.sendText(`{"type":"subscribe","id":"filter-${index}","table":"tickets","filter":${filterJson}}`)
    }
    await waitFor(
      () => ws.messages.filter(message => message.type === 'subscribed').length === FILTERS.length + 1,
      'every subscription',
    )

    const writer = await WireSocket.open(`${served.served.wsBase}/db/desk`)
    const model = new Map<number, WireRow>()
    const changes: Array<{ type: string; before: WireRow | undefined; after: WireRow | undefined }> = []
    const random = { seed: RANDOM_SEED }
    let nextId = 1
    let written = 0
    const write = async (sql: string, params: unknown[]) => {
      const reply = await writer.request({ id: `write-${written++}`, type: 'execute', sql, params })
      expect(reply.type, JSON.stringify(reply)).toBe('result')
    }

    for (let operation = 0; operation < OPERATIONS; operation++) {
      const choice = nextRandom(random) % 10
      const ids = [...model.keys()].sort((a, b) => a - b)
      if (choice < 4 || ids.length === 0) {
        const after: WireRow = {
          id: nextId++,
          status: pick(random, STATUSES),
          owner: pick(random, OWNERS),
          points: pick(random, POINTS),
          tag: pick(random, TAGS),
        }
        await write('INSERT INTO tickets VALUES (?, ?, ?, ?, ?)', [
          after.id,
          after.status,
          after.owner,
          after.points,
          after.tag,
        ])
        model.set(after.id, after)
        changes.push({ type: 'insert', before: undefined, after })
      } else if (choice < 8) {
        const before = model.get(pick(random, ids)) as WireRow
        const column = pick(random, ['status', 'owner', 'points', 'tag'] as const)
        const value = pick(random, COLUMN_VALUES[column])
        const after: WireRow = { ...before, [column]: value }
        await write(`UPDATE tickets SET ${column} = ? WHERE id = ?`, [value, before.id])
        model.set(after.id, after)
        changes.push({ type: 'update', before, after })
      } else if (choice < 9) {
        const owner = pick(random, OWNERS)
        await write("UPDATE tickets SET status = 'closed' WHERE owner = ?", [owner])
        for (const id of ids) {
          const before = model.get(id) as WireRow
          if (before.owner !== owner) continue
          const after: WireRow = { ...before, status: 'closed' }
          model.set(id, after)
          changes.push({ type: 'update', before, after })
        }
      } else {
        const before = model.get(pick(random, ids)) as WireRow
        await write('DELETE FROM tickets WHERE id = ?', [before.id])
        model.delete(before.id)
        changes.push({ type: 'delete', before, after: undefined })
      }
    }
    await ws.waitForEvents('everything', changes.length, DELIVERY_DEADLINE_MS)

    const replay = await WireSocket.open(`${served.served.wsBase}/db/desk`)
    for (const [index, [, filterJson]] of FILTERS.entries()) {
      replay.sendText(
        `{"type":"subscribe","id":"filter-${index}","table":"tickets","sinceSeq":"${everything.seq}","epoch":"${everything.epoch}","filter":${filterJson}}`,
      )
    }
    await waitFor(
      () => replay.messages.filter(message => message.type === 'subscribed').length === FILTERS.length,
      'every replaying subscription',
    )

    expect(ws.events('everything').map(delivered)).toEqual(
      changes.map(change => ({
        type: change.type,
        rowId: String((change.after ?? change.before)?.id),
        row: change.after ?? {},
        oldRow: change.before,
      })),
    )
    for (const [index, [label, filterJson]] of FILTERS.entries()) {
      const filter = JSON.parse(filterJson) as Record<string, unknown>
      const wanted = changes
        .map(change => expectedFor(filter, change.type, change.before, change.after))
        .filter((delivery): delivery is Delivery => delivery !== null)
      const live = ws.events(`filter-${index}`)
      expect(live.map(delivered), label).toEqual(wanted)
      if (wanted.length > 0) await replay.waitForEvents(`filter-${index}`, wanted.length)
      expect(replay.events(`filter-${index}`), `${label} on replay`).toEqual(live)
    }
    expect(FILTERS.filter((_, index) => ws.events(`filter-${index}`).length === 0).map(([label]) => label)).toEqual([
      'owner 4 sent as text',
      'points just past the safe range sent as a plain number',
      'a column the table lacks',
      'a list as a value',
    ])
  })
})
