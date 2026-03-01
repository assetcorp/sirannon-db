import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../database.js'
import { ConnectionPoolError, QueryError, SirannonError } from '../errors.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-db-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

function createTestDb(options?: { readOnly?: boolean; readPoolSize?: number }): Database {
  const dbPath = join(tempDir, 'test.db')

  if (options?.readOnly) {
    const setup = new Database('setup', dbPath)
    setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    setup.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    setup.close()
    return new Database('test', dbPath, { readOnly: true })
  }

  const db = new Database('test', dbPath, options)
  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  return db
}

describe('Database', () => {
  describe('query', () => {
    it('returns all matching rows', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const rows = db.query<{
        id: number
        name: string
        age: number
      }>('SELECT * FROM users')
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')
      db.close()
    })

    it('returns empty array when no rows match', () => {
      const db = createTestDb()
      const rows = db.query('SELECT * FROM users WHERE id = 999')
      expect(rows).toEqual([])
      db.close()
    })

    it('supports named parameters', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

      const rows = db.query<{ name: string }>('SELECT * FROM users WHERE name = :name', { name: 'Alice' })
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      db.close()
    })

    it('supports positional parameters', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const rows = db.query<{ name: string }>('SELECT * FROM users WHERE age > ?', [26])
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      db.close()
    })

    it('throws QueryError on invalid SQL', () => {
      const db = createTestDb()
      expect(() => db.query('SELECT * FORM users')).toThrow(QueryError)
      db.close()
    })
  })

  describe('queryOne', () => {
    it('returns the first matching row', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

      const row = db.queryOne<{ name: string }>('SELECT * FROM users WHERE id = 1')
      expect(row).toBeDefined()
      expect(row?.name).toBe('Alice')
      db.close()
    })

    it('returns undefined when no rows match', () => {
      const db = createTestDb()
      const row = db.queryOne('SELECT * FROM users WHERE id = 999')
      expect(row).toBeUndefined()
      db.close()
    })
  })

  describe('execute', () => {
    it('returns changes and lastInsertRowId', () => {
      const db = createTestDb()
      const result = db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      expect(result.changes).toBe(1)
      expect(result.lastInsertRowId).toBe(1)
      db.close()
    })

    it('reports correct changes for UPDATE', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const result = db.execute('UPDATE users SET age = 99')
      expect(result.changes).toBe(2)
      db.close()
    })

    it('reports correct changes for DELETE', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      const result = db.execute('DELETE FROM users WHERE id = 1')
      expect(result.changes).toBe(1)
      db.close()
    })

    it('supports named parameters', () => {
      const db = createTestDb()
      const result = db.execute('INSERT INTO users (name, age) VALUES (:name, :age)', { name: 'Alice', age: 30 })
      expect(result.changes).toBe(1)

      const row = db.queryOne<{ name: string; age: number }>('SELECT * FROM users WHERE id = 1')
      expect(row?.name).toBe('Alice')
      expect(row?.age).toBe(30)
      db.close()
    })

    it('supports positional parameters', () => {
      const db = createTestDb()
      const result = db.execute('INSERT INTO users (name, age) VALUES (?, ?)', ['Bob', 40])
      expect(result.changes).toBe(1)

      const row = db.queryOne<{ name: string; age: number }>('SELECT * FROM users WHERE id = 1')
      expect(row?.name).toBe('Bob')
      expect(row?.age).toBe(40)
      db.close()
    })

    it('throws QueryError on constraint violation', () => {
      const db = createTestDb()
      db.execute('CREATE TABLE strict (id INTEGER PRIMARY KEY, val TEXT NOT NULL)')
      expect(() => db.execute('INSERT INTO strict (val) VALUES (NULL)')).toThrow(QueryError)
      db.close()
    })
  })

  describe('executeBatch', () => {
    it('inserts multiple rows with the same statement', () => {
      const db = createTestDb()
      const results = db.executeBatch('INSERT INTO users (name, age) VALUES (:name, :age)', [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Carol', age: 35 },
      ])
      expect(results).toHaveLength(3)
      expect(results.every(r => r.changes === 1)).toBe(true)

      const rows = db.query('SELECT * FROM users')
      expect(rows).toHaveLength(3)
      db.close()
    })
  })

  describe('transaction', () => {
    it('commits on success', () => {
      const db = createTestDb()
      const result = db.transaction(tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        return tx.lastInsertRowId
      })

      expect(result).toBe(2)
      const rows = db.query('SELECT * FROM users')
      expect(rows).toHaveLength(2)
      db.close()
    })

    it('rolls back on error', () => {
      const db = createTestDb()
      expect(() =>
        db.transaction(tx => {
          tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
          throw new Error('rollback')
        }),
      ).toThrow('rollback')

      const rows = db.query('SELECT * FROM users')
      expect(rows).toHaveLength(0)
      db.close()
    })

    it('supports queries within a transaction', () => {
      const db = createTestDb()
      db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

      const found = db.transaction(tx => {
        const rows = tx.query<{ name: string }>('SELECT * FROM users WHERE name = ?', ['Alice'])
        return rows.length > 0
      })

      expect(found).toBe(true)
      db.close()
    })

    it('supports executeBatch within a transaction', () => {
      const db = createTestDb()
      db.transaction(tx => {
        tx.executeBatch('INSERT INTO users (name, age) VALUES (?, ?)', [
          ['Alice', 30],
          ['Bob', 25],
        ])
      })

      const rows = db.query('SELECT * FROM users')
      expect(rows).toHaveLength(2)
      db.close()
    })
  })

  describe('read-only mode', () => {
    it('can query a read-only database', () => {
      const db = createTestDb({ readOnly: true })
      const rows = db.query<{ name: string }>('SELECT * FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      db.close()
    })

    it('throws ConnectionPoolError on execute', () => {
      const db = createTestDb({ readOnly: true })
      expect(() => db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")).toThrow(ConnectionPoolError)
      db.close()
    })
  })

  describe('lifecycle', () => {
    it('exposes id, path, readOnly, and closed', () => {
      const dbPath = join(tempDir, 'meta.db')
      const db = new Database('my-db', dbPath)
      expect(db.id).toBe('my-db')
      expect(db.path).toBe(dbPath)
      expect(db.readOnly).toBe(false)
      expect(db.closed).toBe(false)
      db.close()
      expect(db.closed).toBe(true)
    })

    it('throws SirannonError with DATABASE_CLOSED after close', () => {
      const dbPath = join(tempDir, 'closed.db')
      const db = new Database('test', dbPath)
      db.close()

      expect(() => db.query('SELECT 1')).toThrow(SirannonError)
      expect(() => db.execute('SELECT 1')).toThrow(SirannonError)

      try {
        db.query('SELECT 1')
      } catch (err) {
        expect((err as SirannonError).code).toBe('DATABASE_CLOSED')
      }
    })

    it('close is idempotent', () => {
      const dbPath = join(tempDir, 'idem.db')
      const db = new Database('test', dbPath)
      db.close()
      expect(() => db.close()).not.toThrow()
    })

    it('reports reader count', () => {
      const dbPath = join(tempDir, 'readers.db')
      const db = new Database('test', dbPath, {
        readPoolSize: 2,
      })
      expect(db.readerCount).toBe(2)
      db.close()
    })
  })

  describe('close listeners', () => {
    it('invokes listeners on close', () => {
      const dbPath = join(tempDir, 'listener.db')
      const db = new Database('test', dbPath)
      let called = false
      db.addCloseListener(() => {
        called = true
      })
      db.close()
      expect(called).toBe(true)
    })

    it('invokes multiple listeners in order', () => {
      const dbPath = join(tempDir, 'multi.db')
      const db = new Database('test', dbPath)
      const order: number[] = []
      db.addCloseListener(() => order.push(1))
      db.addCloseListener(() => order.push(2))
      db.addCloseListener(() => order.push(3))
      db.close()
      expect(order).toEqual([1, 2, 3])
    })

    it('does not invoke listeners on second close call', () => {
      const dbPath = join(tempDir, 'once.db')
      const db = new Database('test', dbPath)
      let count = 0
      db.addCloseListener(() => {
        count++
      })
      db.close()
      db.close()
      expect(count).toBe(1)
    })

    it('throws when adding listener to closed database', () => {
      const dbPath = join(tempDir, 'closed-listen.db')
      const db = new Database('test', dbPath)
      db.close()
      expect(() => db.addCloseListener(() => {})).toThrow(SirannonError)
    })

    it('still invokes listeners when pool.close throws', () => {
      const dbPath = join(tempDir, 'pool-err.db')
      const db = new Database('test', dbPath)
      let listenerCalled = false
      db.addCloseListener(() => {
        listenerCalled = true
      })
      db.close()
      expect(listenerCalled).toBe(true)
    })
  })
})
