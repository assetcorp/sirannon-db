import { describe, expect, it } from 'vitest'
import { QueryError } from '../../errors.js'
import { createTestDb } from './setup.js'

describe('Database', () => {
  describe('query', () => {
    it('returns all matching rows', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const rows = await db.query<{
        id: number
        name: string
        age: number
      }>('SELECT * FROM users')
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')
      await db.close()
    })

    it('returns empty array when no rows match', async () => {
      const db = await createTestDb()
      const rows = await db.query('SELECT * FROM users WHERE id = 999')
      expect(rows).toEqual([])
      await db.close()
    })

    it('supports named parameters', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

      const rows = await db.query<{ name: string }>('SELECT * FROM users WHERE name = :name', { name: 'Alice' })
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      await db.close()
    })

    it('supports positional parameters', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const rows = await db.query<{ name: string }>('SELECT * FROM users WHERE age > ?', [26])
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      await db.close()
    })

    it('throws QueryError on invalid SQL', async () => {
      const db = await createTestDb()
      await expect(db.query('SELECT * FORM users')).rejects.toThrow(QueryError)
      await db.close()
    })
  })

  describe('queryOne', () => {
    it('returns the first matching row', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

      const row = await db.queryOne<{ name: string }>('SELECT * FROM users WHERE id = 1')
      expect(row).toBeDefined()
      expect(row?.name).toBe('Alice')
      await db.close()
    })

    it('returns undefined when no rows match', async () => {
      const db = await createTestDb()
      const row = await db.queryOne('SELECT * FROM users WHERE id = 999')
      expect(row).toBeUndefined()
      await db.close()
    })
  })
})
