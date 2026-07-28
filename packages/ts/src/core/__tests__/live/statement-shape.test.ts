import { describe, expect, it } from 'vitest'
import { parseColumnCollations } from '../../live/column-collations.js'
import { analyseStatement } from '../../live/statement-shape.js'

describe('analyseStatement', () => {
  it('splits a statement into the parts a live query maintains', () => {
    const shape = analyseStatement(
      'SELECT id, name AS label FROM items AS i WHERE bucket = :bucket ORDER BY name COLLATE NOCASE DESC LIMIT 10 OFFSET 5',
    )

    expect(shape.table).toBe('items')
    expect(shape.alias).toBe('i')
    expect(shape.selectList).toBe('id, name AS label')
    expect(shape.where).toBe('bucket = :bucket')
    expect(shape.sortTerms).toEqual([
      { expression: 'name COLLATE NOCASE', direction: 'desc', nulls: 'last', collation: 'NOCASE' },
    ])
    expect(shape.limit?.literal).toBe(10)
    expect(shape.offset?.literal).toBe(5)
    expect(shape.tail).toBe('ORDER BY name COLLATE NOCASE DESC LIMIT 10 OFFSET 5')
  })

  it('reads the older comma form of LIMIT as offset then count', () => {
    const shape = analyseStatement('SELECT id FROM items ORDER BY id LIMIT 5, 10')
    expect(shape.offset?.literal).toBe(5)
    expect(shape.limit?.literal).toBe(10)
  })

  it('counts the parameters each clause binds', () => {
    const shape = analyseStatement('SELECT ?, id FROM items WHERE a = ? AND b = ? ORDER BY id LIMIT ?')
    expect(shape.selectParameters).toBe(1)
    expect(shape.whereParameters).toBe(2)
    expect(shape.usesPositionalParameters).toBe(true)
  })

  it('keeps a quoted identifier that reads like a keyword out of the way', () => {
    const shape = analyseStatement('SELECT "order", "group" FROM items ORDER BY "order"')
    expect(shape.table).toBe('items')
    expect(shape.sortTerms[0].expression).toBe('"order"')
  })

  it('ignores clause words inside strings and comments', () => {
    const shape = analyseStatement("SELECT id FROM items WHERE name = 'order by group' -- limit 3\n ORDER BY id")
    expect(shape.where).toContain("name = 'order by group'")
    expect(shape.limit).toBeNull()
    expect(shape.sortTerms).toEqual([{ expression: 'id', direction: 'asc', nulls: 'first', collation: null }])
  })

  it('names the construct it cannot maintain', () => {
    const cases: [string, RegExp][] = [
      ['SELECT SUM(qty) FROM items', /aggregate/i],
      ['SELECT bucket FROM items GROUP BY bucket', /GROUP BY/i],
      ['SELECT DISTINCT bucket FROM items', /DISTINCT/i],
      ['SELECT id FROM items UNION SELECT id FROM items', /compound SELECT/i],
      ['SELECT id FROM items a, items b', /join|one table/i],
      ['SELECT id FROM (SELECT id FROM items)', /subquery/i],
      ['SELECT id FROM items WHERE id IN (SELECT id FROM items)', /subquery/i],
      ['WITH x AS (SELECT 1) SELECT id FROM items', /single SELECT|common table/i],
      ['SELECT row_number() OVER (ORDER BY id) FROM items', /window function/i],
      ['SELECT id FROM items ORDER BY id LIMIT id + 1', /LIMIT and OFFSET/i],
      ['SELECT id FROM items; DELETE FROM items', /one statement/i],
      ['UPDATE items SET qty = 1', /single SELECT/i],
    ]

    for (const [sql, pattern] of cases) {
      expect(() => analyseStatement(sql), sql).toThrow(pattern)
    }
  })

  it('refuses a statement that reaches for an internal table', () => {
    expect(() => analyseStatement('SELECT * FROM _sirannon_changes')).toThrow(/reserved/i)
  })
})

describe('parseColumnCollations', () => {
  it('reads a collation from each column definition and skips table constraints', () => {
    const collations = parseColumnCollations(
      `CREATE TABLE people (
        id INTEGER PRIMARY KEY,
        name TEXT COLLATE NOCASE NOT NULL,
        code TEXT DEFAULT 'collate rtrim',
        tag TEXT COLLATE RTRIM,
        UNIQUE (name COLLATE BINARY)
      )`,
    )

    expect(collations.get('name')).toBe('NOCASE')
    expect(collations.get('tag')).toBe('RTRIM')
    expect(collations.has('code')).toBe(false)
    expect(collations.has('id')).toBe(false)
  })
})
