import { describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { buildSortKeyPlan, toSortValue } from '../../live/sqlite-order.js'
import type { SortTerm } from '../../live/statement-shape.js'

const VALUES: unknown[] = [
  null,
  -9007199254740993n,
  -3,
  -0.5,
  0,
  1,
  1.5,
  2,
  9007199254740993n,
  '',
  'A',
  'B',
  'a',
  'ab',
  'b',
  'item',
  'z  ',
  'z',
  'é',
  '\u{1f600}',
  '�',
  new Uint8Array([0]),
  new Uint8Array([0, 1]),
  new Uint8Array([255]),
]

function term(direction: 'asc' | 'desc', collation: string | null): SortTerm {
  return {
    expression: 'v',
    direction,
    nulls: direction === 'asc' ? 'first' : 'last',
    collation,
  }
}

async function sqliteOrder(
  conn: SQLiteConnection,
  values: readonly unknown[],
  direction: 'asc' | 'desc',
  collation: string | null,
): Promise<number[]> {
  await conn.exec('DROP TABLE IF EXISTS ordering')
  await conn.exec('CREATE TABLE ordering (idx INTEGER PRIMARY KEY, v)')
  const insert = await conn.prepare('INSERT INTO ordering (idx, v) VALUES (?, ?)')
  for (let index = 0; index < values.length; index++) {
    await insert.run(index, values[index])
  }
  const collate = collation === null ? '' : ` COLLATE ${collation}`
  const stmt = await conn.prepare(`SELECT idx FROM ordering ORDER BY v${collate} ${direction}, idx ASC`)
  const rows = await stmt.all<{ idx: number }>()
  return rows.map(row => Number(row.idx))
}

describe('sort ordering', () => {
  it('matches SQLite for every storage class, direction, and supported collation', async () => {
    const conn = await betterSqlite3().open(':memory:')
    try {
      for (const direction of ['asc', 'desc'] as const) {
        for (const collation of [null, 'NOCASE', 'RTRIM']) {
          const plan = buildSortKeyPlan([term(direction, collation)])
          const collationName = plan.collations[0]

          const mine = VALUES.map((value, index) => ({ index, sort: [toSortValue(value, collationName)] }))
            .sort((left, right) => plan.compare(left.sort, right.sort) || left.index - right.index)
            .map(entry => entry.index)

          const theirs = await sqliteOrder(conn, VALUES, direction, collation)
          expect(mine, `${direction} ${collation ?? 'BINARY'}`).toEqual(theirs)
        }
      }
    } finally {
      await conn.close()
    }
  })

  it('places nulls where the term asks, whichever way the rows are ordered', () => {
    const plan = buildSortKeyPlan([{ expression: 'v', direction: 'asc', nulls: 'last', collation: null }])
    const ordered = [null, 1, 2]
      .map((value, index) => ({ index, sort: [toSortValue(value, 'binary')] }))
      .sort((left, right) => plan.compare(left.sort, right.sort))
      .map(entry => entry.index)

    expect(ordered).toEqual([1, 2, 0])
  })

  it('orders a large integer against a float without losing precision', () => {
    const plan = buildSortKeyPlan([{ expression: 'v', direction: 'asc', nulls: 'first', collation: null }])
    const big = toSortValue(9007199254740993n, 'binary')
    const float = toSortValue(9007199254740992, 'binary')

    expect(plan.compare([big], [float])).toBe(1)
    expect(plan.compare([float], [big])).toBe(-1)
  })
})
