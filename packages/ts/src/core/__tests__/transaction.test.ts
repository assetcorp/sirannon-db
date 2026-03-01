import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Transaction } from '../transaction.js'

let db: InstanceType<typeof Database>

beforeEach(() => {
  db = new Database(':memory:')
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
})

afterEach(() => {
  db.close()
})

describe('Transaction', () => {
  describe('run', () => {
    it('commits when the callback succeeds', () => {
      Transaction.run(db, tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
      })

      const rows = db.prepare('SELECT * FROM users').all()
      expect(rows).toHaveLength(2)
    })

    it('rolls back when the callback throws', () => {
      expect(() =>
        Transaction.run(db, tx => {
          tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
          throw new Error('abort')
        }),
      ).toThrow('abort')

      const rows = db.prepare('SELECT * FROM users').all()
      expect(rows).toHaveLength(0)
    })

    it('returns the callback return value', () => {
      const result = Transaction.run(db, tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        return 'done'
      })
      expect(result).toBe('done')
    })
  })

  describe('query', () => {
    it('reads rows within the transaction', () => {
      Transaction.run(db, tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        const rows = tx.query<{ name: string }>('SELECT * FROM users')
        expect(rows).toHaveLength(1)
        expect(rows[0].name).toBe('Alice')
      })
    })

    it('supports parameters', () => {
      Transaction.run(db, tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        const rows = tx.query<{ name: string }>('SELECT * FROM users WHERE age > ?', [26])
        expect(rows).toHaveLength(1)
        expect(rows[0].name).toBe('Alice')
      })
    })
  })

  describe('execute', () => {
    it('returns changes and lastInsertRowId', () => {
      Transaction.run(db, tx => {
        const result = tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        expect(result.changes).toBe(1)
        expect(Number(result.lastInsertRowId)).toBe(1)
      })
    })

    it('tracks lastInsertRowId across multiple executes', () => {
      const lastId = Transaction.run(db, tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        return tx.lastInsertRowId
      })
      expect(Number(lastId)).toBe(2)
    })
  })

  describe('executeBatch', () => {
    it('inserts multiple rows and tracks lastInsertRowId', () => {
      const lastId = Transaction.run(db, tx => {
        tx.executeBatch('INSERT INTO users (name, age) VALUES (?, ?)', [
          ['Alice', 30],
          ['Bob', 25],
          ['Carol', 35],
        ])
        return tx.lastInsertRowId
      })

      expect(Number(lastId)).toBe(3)
      const rows = db.prepare('SELECT * FROM users').all()
      expect(rows).toHaveLength(3)
    })

    it('does not update lastInsertRowId for an empty batch', () => {
      const lastId = Transaction.run(db, tx => {
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        tx.executeBatch('INSERT INTO users (name, age) VALUES (?, ?)', [])
        return tx.lastInsertRowId
      })
      expect(Number(lastId)).toBe(1)
    })
  })

  describe('lastInsertRowId', () => {
    it('starts at 0 before any execute', () => {
      Transaction.run(db, tx => {
        expect(Number(tx.lastInsertRowId)).toBe(0)
        tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        expect(Number(tx.lastInsertRowId)).toBe(1)
      })
    })
  })
})
