import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../driver/types.js'
import { QueryError } from '../errors.js'
import { execute, executeBatch, query, queryOne } from '../query-executor.js'
import { testDriver } from './helpers/test-driver.js'

let conn: SQLiteConnection

beforeEach(async () => {
  conn = await testDriver.open(':memory:')
  await conn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  await conn.exec("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)")
})

afterEach(async () => {
  await conn.close()
})

describe('query', () => {
  it('returns all rows for a SELECT', async () => {
    const rows = await query<{ id: number; name: string }>(conn, 'SELECT * FROM users ORDER BY id')
    expect(rows).toHaveLength(2)
    expect(rows[0].name).toBe('Alice')
    expect(rows[1].name).toBe('Bob')
  })

  it('supports named parameters', async () => {
    const rows = await query<{ name: string }>(conn, 'SELECT * FROM users WHERE name = :name', { name: 'Bob' })
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Bob')
  })

  it('supports positional parameters', async () => {
    const rows = await query<{ name: string }>(conn, 'SELECT * FROM users WHERE age > ?', [26])
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
  })

  it('returns an empty array for no matches', async () => {
    const rows = await query(conn, 'SELECT * FROM users WHERE id = 999')
    expect(rows).toEqual([])
  })

  it('throws QueryError on invalid SQL', async () => {
    await expect(query(conn, 'SELCT * FORM users')).rejects.toThrow(QueryError)
  })

  it('throws QueryError with the offending SQL', async () => {
    const badSql = 'SELECT * FROM nonexistent_table'
    try {
      await query(conn, badSql)
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).sql).toBe(badSql)
    }
  })

  it('wraps non-Error throwables in QueryError', async () => {
    const fakeConn = {
      prepare() {
        throw 'string error'
      },
    } as unknown as SQLiteConnection

    try {
      await query(fakeConn, 'SELECT 1')
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).message).toBe('string error')
    }
  })
})

describe('queryOne', () => {
  it('returns the first matching row', async () => {
    const row = await queryOne<{ name: string }>(conn, 'SELECT * FROM users WHERE id = 1')
    expect(row).toBeDefined()
    expect(row?.name).toBe('Alice')
  })

  it('returns undefined when no match', async () => {
    const row = await queryOne(conn, 'SELECT * FROM users WHERE id = 999')
    expect(row).toBeUndefined()
  })

  it('throws QueryError on invalid SQL', async () => {
    await expect(queryOne(conn, 'SELCT oops')).rejects.toThrow(QueryError)
  })

  it('wraps non-Error throwables in QueryError', async () => {
    const fakeConn = {
      prepare() {
        throw 'string error'
      },
    } as unknown as SQLiteConnection

    try {
      await queryOne(fakeConn, 'SELECT 1')
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).message).toBe('string error')
    }
  })
})

describe('execute', () => {
  it('returns changes and lastInsertRowId', async () => {
    const result = await execute(conn, "INSERT INTO users (name, age) VALUES ('Carol', 22)")
    expect(result.changes).toBe(1)
    expect(Number(result.lastInsertRowId)).toBe(3)
  })

  it('reports update changes', async () => {
    const result = await execute(conn, 'UPDATE users SET age = 99')
    expect(result.changes).toBe(2)
  })

  it('reports delete changes', async () => {
    const result = await execute(conn, 'DELETE FROM users WHERE name = ?', ['Alice'])
    expect(result.changes).toBe(1)
  })

  it('throws QueryError on constraint violation', async () => {
    await conn.exec('CREATE TABLE strict (id INTEGER PRIMARY KEY, val TEXT NOT NULL)')
    await expect(execute(conn, 'INSERT INTO strict (val) VALUES (NULL)')).rejects.toThrow(QueryError)
  })

  it('wraps non-Error throwables in QueryError', async () => {
    const fakeConn = {
      prepare() {
        throw 'string error'
      },
    } as unknown as SQLiteConnection

    try {
      await execute(fakeConn, 'INSERT INTO users (name) VALUES (?)', ['Alice'])
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).message).toBe('string error')
    }
  })
})

describe('executeBatch', () => {
  it('executes the same statement with multiple param sets', async () => {
    const results = await executeBatch(conn, 'INSERT INTO users (name, age) VALUES (?, ?)', [
      ['Carol', 22],
      ['Dave', 40],
    ])
    expect(results).toHaveLength(2)
    expect(results[0].changes).toBe(1)
    expect(results[1].changes).toBe(1)

    const rows = await query(conn, 'SELECT COUNT(*) as count FROM users')
    expect((rows[0] as { count: number }).count).toBe(4)
  })

  it('returns empty array for empty batch', async () => {
    const results = await executeBatch(conn, 'INSERT INTO users (name, age) VALUES (?, ?)', [])
    expect(results).toEqual([])
  })

  it('throws QueryError on bad SQL', async () => {
    await expect(executeBatch(conn, 'INSERT INTO nonexistent (x) VALUES (?)', [['a']])).rejects.toThrow(QueryError)
  })

  it('wraps non-Error throwables in QueryError', async () => {
    const fakeConn = {
      prepare() {
        throw 'string error'
      },
      async transaction<T>(fn: (txConn: unknown) => Promise<T>) {
        return fn(this)
      },
    } as unknown as SQLiteConnection

    try {
      await executeBatch(fakeConn, 'INSERT INTO users (name) VALUES (?)', [['Alice']])
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(QueryError)
      expect((err as QueryError).message).toBe('string error')
    }
  })
})

describe('statement caching', () => {
  it('reuses prepared statements for repeated SQL', async () => {
    const sql = 'SELECT * FROM users WHERE id = ?'
    const row1 = await queryOne<{ name: string }>(conn, sql, [1])
    const row2 = await queryOne<{ name: string }>(conn, sql, [2])
    expect(row1?.name).toBe('Alice')
    expect(row2?.name).toBe('Bob')
  })

  it('evicts the oldest statement when cache capacity is exceeded', async () => {
    for (let i = 0; i < 140; i++) {
      await query(conn, `SELECT ${i} as val`)
    }

    const rows = await query<{ val: number }>(conn, 'SELECT 139 as val')
    expect(rows[0].val).toBe(139)
  })

  it('handles unexpected undefined oldest key during eviction', async () => {
    const originalKeys = Map.prototype.keys
    Map.prototype.keys = function () {
      const iterator = originalKeys.call(this)
      let first = true
      return {
        next() {
          if (first) {
            first = false
            return { value: undefined, done: false }
          }
          return iterator.next()
        },
        [Symbol.iterator]() {
          return this
        },
      } as unknown as ReturnType<typeof originalKeys>
    }

    try {
      for (let i = 0; i < 140; i++) {
        await query(conn, `SELECT ${i} as val`)
      }
      const rows = await query<{ val: number }>(conn, 'SELECT 139 as val')
      expect(rows[0].val).toBe(139)
    } finally {
      Map.prototype.keys = originalKeys
    }
  })
})
