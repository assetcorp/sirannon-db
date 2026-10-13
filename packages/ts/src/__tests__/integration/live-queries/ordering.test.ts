import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { LiveQuery } from '../../../core/live/types.js'
import { createWorkspace, replicate, type Workspace, waitFor } from './harness.js'

const VALUES: unknown[] = [
  null,
  -9223372036854775808n,
  -9007199254740993n,
  -Infinity,
  -3,
  -0.5,
  -0,
  0,
  5e-324,
  1,
  1.5,
  2,
  9007199254740992,
  9007199254740993n,
  9223372036854775807n,
  Infinity,
  '',
  ' ',
  'A',
  'B',
  'Z',
  '[',
  '_',
  '`',
  'a',
  'a_b',
  'aB',
  'ab',
  'b',
  'z  ',
  'z',
  'Z ',
  'é',
  'É',
  '\u{1f600}',
  '�',
  '10',
  '9',
  new Uint8Array([]),
  new Uint8Array([0]),
  new Uint8Array([0, 1]),
  new Uint8Array([97]),
  new Uint8Array([255]),
]

const ORDERINGS = ['', ' COLLATE NOCASE', ' COLLATE RTRIM', ' COLLATE BINARY'].flatMap(collation =>
  ['ASC', 'DESC'].flatMap(direction =>
    ['', ' NULLS FIRST', ' NULLS LAST'].map(nulls => `v${collation} ${direction}${nulls}`),
  ),
)

let workspace: Workspace
const opened: LiveQuery<unknown>[] = []

beforeEach(() => {
  workspace = createWorkspace('ordering')
})

afterEach(async () => {
  for (const query of opened.splice(0)) await query.close().catch(() => {})
  await workspace.dispose()
})

function shuffled<T>(items: readonly T[], seed: number): T[] {
  const copy = items.slice()
  let state = seed
  for (let index = copy.length - 1; index > 0; index--) {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648
    const swap = state % (index + 1)
    const held = copy[index]
    copy[index] = copy[swap]
    copy[swap] = held
  }
  return copy
}

describe('the order of a live query', () => {
  it('matches SQLite for every storage class, collation, direction, and NULLS placement as rows arrive and move one at a time', async () => {
    const db = await workspace.open('storage-classes')
    await db.execute('CREATE TABLE ordering (idx INTEGER PRIMARY KEY, v)')
    await db.execute("INSERT INTO ordering (idx, v) VALUES (-1, 'seed')")
    const statements = ORDERINGS.map(term => `SELECT idx FROM ordering ORDER BY ${term}, idx`)
    const queries: LiveQuery<{ idx: number }>[] = []
    for (const sql of statements) {
      const query = await db.live<{ idx: number }>(sql, undefined, { rereadJitterMs: 0 })
      opened.push(query as LiveQuery<unknown>)
      queries.push(query)
    }
    const replicas = queries.map(query => replicate(query))

    const order = shuffled(
      VALUES.map((_, index) => index),
      7,
    )
    for (const index of order) await db.execute('INSERT INTO ordering (idx, v) VALUES (?, ?)', [index, VALUES[index]])
    for (const index of shuffled(order, 11).slice(0, 15)) {
      await db.execute('UPDATE ordering SET v = ? WHERE idx = ?', [VALUES[(index * 7) % VALUES.length], index])
    }
    const expectedUpdates = VALUES.length + 15

    for (const [position, sql] of statements.entries()) {
      await waitFor(() => replicas[position].kinds.length === expectedUpdates, `every change to reach ${sql}`)
      const sqlite = (await db.query<{ idx: number }>(sql)).map(row => row.idx)
      const state = queries[position].getState()
      expect(state.status === 'ready' && state.rows.map(row => row.idx), sql).toEqual(sqlite)
      expect(
        replicas[position].kinds.every(kind => kind === 'ops'),
        sql,
      ).toBe(true)
    }
  })
})
