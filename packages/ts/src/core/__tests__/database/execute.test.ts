import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { QueryError } from '../../errors.js'
import { MetricsCollector } from '../../metrics/collector.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, getTempDir, state } from './setup.js'

describe('Database', () => {
  describe('execute', () => {
    it('returns changes and lastInsertRowId', async () => {
      const db = await createTestDb()
      const result = await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      expect(result.changes).toBe(1)
      expect(result.lastInsertRowId).toBe(1)
      await db.close()
    })

    it('reports correct changes for UPDATE', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

      const result = await db.execute('UPDATE users SET age = 99')
      expect(result.changes).toBe(2)
      await db.close()
    })

    it('reports correct changes for DELETE', async () => {
      const db = await createTestDb()
      await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
      const result = await db.execute('DELETE FROM users WHERE id = 1')
      expect(result.changes).toBe(1)
      await db.close()
    })

    it('supports named parameters', async () => {
      const db = await createTestDb()
      const result = await db.execute('INSERT INTO users (name, age) VALUES (:name, :age)', { name: 'Alice', age: 30 })
      expect(result.changes).toBe(1)

      const row = await db.queryOne<{ name: string; age: number }>('SELECT * FROM users WHERE id = 1')
      expect(row?.name).toBe('Alice')
      expect(row?.age).toBe(30)
      await db.close()
    })

    it('supports positional parameters', async () => {
      const db = await createTestDb()
      const result = await db.execute('INSERT INTO users (name, age) VALUES (?, ?)', ['Bob', 40])
      expect(result.changes).toBe(1)

      const row = await db.queryOne<{ name: string; age: number }>('SELECT * FROM users WHERE id = 1')
      expect(row?.name).toBe('Bob')
      expect(row?.age).toBe(40)
      await db.close()
    })

    it('throws QueryError on constraint violation', async () => {
      const db = await createTestDb()
      await db.execute('CREATE TABLE strict (id INTEGER PRIMARY KEY, val TEXT NOT NULL)')
      await expect(db.execute('INSERT INTO strict (val) VALUES (NULL)')).rejects.toThrow(QueryError)
      await db.close()
    })
  })

  describe('executeBatch', () => {
    it('inserts multiple rows with the same statement', async () => {
      const db = await createTestDb()
      const results = await db.executeBatch('INSERT INTO users (name, age) VALUES (:name, :age)', [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Carol', age: 35 },
      ])
      expect(results).toHaveLength(3)
      expect(results.every(r => r.changes === 1)).toBe(true)

      const rows = await db.query('SELECT * FROM users')
      expect(rows).toHaveLength(3)
      await db.close()
    })

    it('tracks executeBatch via metrics collector when configured', async () => {
      const metrics: { sql: string }[] = []
      const collector = new MetricsCollector({
        onQueryComplete: metric => {
          metrics.push({ sql: metric.sql })
        },
      })

      const dbPath = join(getTempDir(), 'metrics-batch.db')
      const db = await Database.create('test', dbPath, testDriver, undefined, { metrics: collector })
      state.openDbs.push(db)
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
      metrics.length = 0

      const results = await db.executeBatch('INSERT INTO users (name, age) VALUES (?, ?)', [
        ['Alice', 30],
        ['Bob', 25],
      ])

      expect(results).toHaveLength(2)
      expect(metrics).toHaveLength(1)
      expect(metrics[0].sql).toContain('INSERT INTO users')
      await db.close()
    })
  })

  describe('executeBatch transaction behavior', () => {
    it('rolls back all rows on batch failure (implicit transaction)', async () => {
      const dbPath = join(getTempDir(), 'batch-tx.db')
      const db = await Database.create('test', dbPath, testDriver)
      state.openDbs.push(db)
      await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT UNIQUE)')
      await db.execute("INSERT INTO items (val) VALUES ('existing')")

      await expect(
        db.executeBatch('INSERT INTO items (val) VALUES (?)', [['a'], ['b'], ['existing']]),
      ).rejects.toThrow()

      const rows = await db.query<{ val: string }>('SELECT val FROM items')
      expect(rows).toHaveLength(1)
      expect(rows[0].val).toBe('existing')
    })
  })
})
