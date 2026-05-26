import { describe, expect, it } from 'vitest'
import { createTestDb } from './setup.js'

describe('Database', () => {
  describe('transaction', () => {
    it('commits on success', async () => {
      const db = await createTestDb()
      const result = await db.transaction(async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        await tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        return tx.lastInsertRowId
      })

      expect(result).toBe(2)
      const rows = await db.query('SELECT * FROM users')
      expect(rows).toHaveLength(2)
      await db.close()
    })

    it('rolls back on error', async () => {
      const db = await createTestDb()
      await expect(
        db.transaction(async tx => {
          await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
          throw new Error('rollback')
        }),
      ).rejects.toThrow('rollback')

      const rows = await db.query('SELECT * FROM users')
      expect(rows).toHaveLength(0)
      await db.close()
    })

    it('supports queries within a transaction', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

      const found = await db.transaction(async tx => {
        const rows = await tx.query<{ name: string }>('SELECT * FROM users WHERE name = ?', ['Alice'])
        return rows.length > 0
      })

      expect(found).toBe(true)
      await db.close()
    })

    it('supports executeBatch within a transaction', async () => {
      const db = await createTestDb()
      await db.transaction(async tx => {
        await tx.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        await tx.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
      })

      const rows = await db.query('SELECT * FROM users')
      expect(rows).toHaveLength(2)
      await db.close()
    })
  })
})
