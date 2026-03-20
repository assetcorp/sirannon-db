import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../driver/types.js'
import { Transaction } from '../transaction.js'
import { testDriver } from './helpers/test-driver.js'

let conn: SQLiteConnection

beforeEach(async () => {
  conn = await testDriver.open(':memory:')
  await conn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
})

afterEach(async () => {
  await conn.close()
})

describe('Transaction', () => {
  describe('run', () => {
    it('commits when the callback succeeds', async () => {
      await Transaction.run(conn, async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        await tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
      })

      const stmt = await conn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(2)
    })

    it('rolls back when the callback throws', async () => {
      await expect(
        Transaction.run(conn, async tx => {
          await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
          throw new Error('abort')
        }),
      ).rejects.toThrow('abort')

      const stmt = await conn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(0)
    })

    it('returns the callback return value', async () => {
      const result = await Transaction.run(conn, async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        return 'done'
      })
      expect(result).toBe('done')
    })
  })

  describe('query', () => {
    it('reads rows within the transaction', async () => {
      await Transaction.run(conn, async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        const rows = await tx.query<{ name: string }>('SELECT * FROM users')
        expect(rows).toHaveLength(1)
        expect(rows[0].name).toBe('Alice')
      })
    })

    it('supports parameters', async () => {
      await Transaction.run(conn, async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        await tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        const rows = await tx.query<{ name: string }>('SELECT * FROM users WHERE age > ?', [26])
        expect(rows).toHaveLength(1)
        expect(rows[0].name).toBe('Alice')
      })
    })
  })

  describe('execute', () => {
    it('returns changes and lastInsertRowId', async () => {
      await Transaction.run(conn, async tx => {
        const result = await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        expect(result.changes).toBe(1)
        expect(Number(result.lastInsertRowId)).toBe(1)
      })
    })

    it('tracks lastInsertRowId across multiple executes', async () => {
      const lastId = await Transaction.run(conn, async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        await tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        return tx.lastInsertRowId
      })
      expect(Number(lastId)).toBe(2)
    })
  })

  describe('executeBatch', () => {
    it('inserts multiple rows and tracks lastInsertRowId', async () => {
      const tx = new Transaction(conn)
      await tx.executeBatch('INSERT INTO users (name, age) VALUES (?, ?)', [
        ['Alice', 30],
        ['Bob', 25],
        ['Carol', 35],
      ])
      const lastId = tx.lastInsertRowId

      expect(Number(lastId)).toBe(3)
      const stmt = await conn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(3)
    })

    it('does not update lastInsertRowId for an empty batch', async () => {
      const tx = new Transaction(conn)
      await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await tx.executeBatch('INSERT INTO users (name, age) VALUES (?, ?)', [])
      const lastId = tx.lastInsertRowId
      expect(Number(lastId)).toBe(1)
    })
  })

  describe('lastInsertRowId', () => {
    it('starts at 0 before any execute', async () => {
      await Transaction.run(conn, async tx => {
        expect(Number(tx.lastInsertRowId)).toBe(0)
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        expect(Number(tx.lastInsertRowId)).toBe(1)
      })
    })
  })
})
