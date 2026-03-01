import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { QueryError } from '../errors.js'
import { execute, executeBatch, query, queryOne } from '../query-executor.js'

let db: InstanceType<typeof Database>

beforeEach(() => {
  db = new Database(':memory:')
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)")
})

afterEach(() => {
  db.close()
})

describe('query', () => {
  it('returns all rows for a SELECT', () => {
    const rows = query<{ id: number; name: string }>(db, 'SELECT * FROM users ORDER BY id')
    expect(rows).toHaveLength(2)
    expect(rows[0].name).toBe('Alice')
    expect(rows[1].name).toBe('Bob')
  })

  it('supports named parameters', () => {
    const rows = query<{ name: string }>(db, 'SELECT * FROM users WHERE name = :name', { name: 'Bob' })
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Bob')
  })

  it('supports positional parameters', () => {
    const rows = query<{ name: string }>(db, 'SELECT * FROM users WHERE age > ?', [26])
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
  })

  it('returns an empty array for no matches', () => {
    const rows = query(db, 'SELECT * FROM users WHERE id = 999')
    expect(rows).toEqual([])
  })

  it('throws QueryError on invalid SQL', () => {
    expect(() => query(db, 'SELCT * FORM users')).toThrow(QueryError)
  })

  it('throws QueryError with the offending SQL', () => {
    const badSql = 'SELECT * FROM nonexistent_table'
    try {
      query(db, badSql)
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).sql).toBe(badSql)
    }
  })

  it('wraps non-Error throwables in QueryError', () => {
    const fakeDb = {
      prepare() {
        throw 'string error'
      },
    } as unknown as InstanceType<typeof Database>

    try {
      query(fakeDb, 'SELECT 1')
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).message).toBe('string error')
    }
  })
})

describe('queryOne', () => {
  it('returns the first matching row', () => {
    const row = queryOne<{ name: string }>(db, 'SELECT * FROM users WHERE id = 1')
    expect(row).toBeDefined()
    expect(row?.name).toBe('Alice')
  })

  it('returns undefined when no match', () => {
    const row = queryOne(db, 'SELECT * FROM users WHERE id = 999')
    expect(row).toBeUndefined()
  })

  it('throws QueryError on invalid SQL', () => {
    expect(() => queryOne(db, 'SELCT oops')).toThrow(QueryError)
  })
})

describe('execute', () => {
  it('returns changes and lastInsertRowId', () => {
    const result = execute(db, "INSERT INTO users (name, age) VALUES ('Carol', 22)")
    expect(result.changes).toBe(1)
    expect(Number(result.lastInsertRowId)).toBe(3)
  })

  it('reports update changes', () => {
    const result = execute(db, 'UPDATE users SET age = 99')
    expect(result.changes).toBe(2)
  })

  it('reports delete changes', () => {
    const result = execute(db, 'DELETE FROM users WHERE name = ?', ['Alice'])
    expect(result.changes).toBe(1)
  })

  it('throws QueryError on constraint violation', () => {
    db.exec('CREATE TABLE strict (id INTEGER PRIMARY KEY, val TEXT NOT NULL)')
    expect(() => execute(db, 'INSERT INTO strict (val) VALUES (NULL)')).toThrow(QueryError)
  })
})

describe('executeBatch', () => {
  it('executes the same statement with multiple param sets', () => {
    const results = executeBatch(db, 'INSERT INTO users (name, age) VALUES (?, ?)', [
      ['Carol', 22],
      ['Dave', 40],
    ])
    expect(results).toHaveLength(2)
    expect(results[0].changes).toBe(1)
    expect(results[1].changes).toBe(1)

    const rows = query(db, 'SELECT COUNT(*) as count FROM users')
    expect((rows[0] as { count: number }).count).toBe(4)
  })

  it('returns empty array for empty batch', () => {
    const results = executeBatch(db, 'INSERT INTO users (name, age) VALUES (?, ?)', [])
    expect(results).toEqual([])
  })

  it('throws QueryError on bad SQL', () => {
    expect(() => executeBatch(db, 'INSERT INTO nonexistent (x) VALUES (?)', [['a']])).toThrow(QueryError)
  })
})

describe('statement caching', () => {
  it('reuses prepared statements for repeated SQL', () => {
    const sql = 'SELECT * FROM users WHERE id = ?'
    const row1 = queryOne<{ name: string }>(db, sql, [1])
    const row2 = queryOne<{ name: string }>(db, sql, [2])
    expect(row1?.name).toBe('Alice')
    expect(row2?.name).toBe('Bob')
  })

  it('evicts the oldest statement when cache capacity is exceeded', () => {
    for (let i = 0; i < 140; i++) {
      query(db, `SELECT ${i} as val`)
    }

    const rows = query<{ val: number }>(db, 'SELECT 139 as val')
    expect(rows[0].val).toBe(139)
  })
})
