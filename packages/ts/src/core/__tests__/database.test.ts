import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Database } from '../database.js'
import { ExtensionError, QueryError, ReadOnlyError, SirannonError } from '../errors.js'
import { MetricsCollector } from '../metrics/collector.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string
const openDbs: Database[] = []

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-db-'))
})

afterEach(async () => {
  for (const db of openDbs) {
    try {
      if (!db.closed) await db.close()
    } catch {
      /* best-effort cleanup */
    }
  }
  openDbs.length = 0
  rmSync(tempDir, { recursive: true, force: true })
})

async function createTestDb(options?: { readOnly?: boolean; readPoolSize?: number }): Promise<Database> {
  const dbPath = join(tempDir, 'test.db')

  if (options?.readOnly) {
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    await setup.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await setup.close()
    const db = await Database.create('test', dbPath, testDriver, { readOnly: true })
    openDbs.push(db)
    return db
  }

  const db = await Database.create('test', dbPath, testDriver, options)
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  openDbs.push(db)
  return db
}

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

      const dbPath = join(tempDir, 'metrics-batch.db')
      const db = await Database.create('test', dbPath, testDriver, undefined, { metrics: collector })
      openDbs.push(db)
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

  describe('read-only mode', () => {
    it('can query a read-only database', async () => {
      const db = await createTestDb({ readOnly: true })
      const rows = await db.query<{ name: string }>('SELECT * FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')
      await db.close()
    })

    it('throws ReadOnlyError on execute', async () => {
      const db = await createTestDb({ readOnly: true })
      await expect(db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")).rejects.toThrow(ReadOnlyError)
      await db.close()
    })
  })

  describe('lifecycle', () => {
    it('exposes id, path, readOnly, and closed', async () => {
      const dbPath = join(tempDir, 'meta.db')
      const db = await Database.create('my-db', dbPath, testDriver)
      expect(db.id).toBe('my-db')
      expect(db.path).toBe(dbPath)
      expect(db.readOnly).toBe(false)
      expect(db.closed).toBe(false)
      await db.close()
      expect(db.closed).toBe(true)
    })

    it('throws SirannonError with DATABASE_CLOSED after close', async () => {
      const dbPath = join(tempDir, 'closed.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.close()

      await expect(db.query('SELECT 1')).rejects.toThrow(SirannonError)
      await expect(db.execute('SELECT 1')).rejects.toThrow(SirannonError)

      try {
        await db.query('SELECT 1')
      } catch (err) {
        expect((err as SirannonError).code).toBe('DATABASE_CLOSED')
      }
    })

    it('close is idempotent', async () => {
      const dbPath = join(tempDir, 'idem.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.close()
      await expect(db.close()).resolves.not.toThrow()
    })

    it('reports reader count', async () => {
      const dbPath = join(tempDir, 'readers.db')
      const db = await Database.create('test', dbPath, testDriver, {
        readPoolSize: 2,
      })
      expect(db.readerCount).toBe(2)
      await db.close()
    })

    it('tracks queryOne via metrics collector when configured', async () => {
      const metrics: { sql: string }[] = []
      const collector = new MetricsCollector({
        onQueryComplete: metric => {
          metrics.push({ sql: metric.sql })
        },
      })

      const dbPath = join(tempDir, 'metrics-queryone.db')
      const db = await Database.create('test', dbPath, testDriver, undefined, { metrics: collector })
      openDbs.push(db)
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.execute("INSERT INTO users (name) VALUES ('Alice')")
      metrics.length = 0

      const row = await db.queryOne<{ name: string }>('SELECT name FROM users WHERE id = 1')
      expect(row?.name).toBe('Alice')
      expect(metrics).toHaveLength(1)
      expect(metrics[0].sql).toContain('SELECT name FROM users')
      await db.close()
    })
  })

  describe('close listeners', () => {
    it('invokes listeners on close', async () => {
      const dbPath = join(tempDir, 'listener.db')
      const db = await Database.create('test', dbPath, testDriver)
      let called = false
      db.addCloseListener(() => {
        called = true
      })
      await db.close()
      expect(called).toBe(true)
    })

    it('invokes multiple listeners in order', async () => {
      const dbPath = join(tempDir, 'multi.db')
      const db = await Database.create('test', dbPath, testDriver)
      const order: number[] = []
      db.addCloseListener(() => order.push(1))
      db.addCloseListener(() => order.push(2))
      db.addCloseListener(() => order.push(3))
      await db.close()
      expect(order).toEqual([1, 2, 3])
    })

    it('does not invoke listeners on second close call', async () => {
      const dbPath = join(tempDir, 'once.db')
      const db = await Database.create('test', dbPath, testDriver)
      let count = 0
      db.addCloseListener(() => {
        count++
      })
      await db.close()
      await db.close()
      expect(count).toBe(1)
    })

    it('throws when adding listener to closed database', async () => {
      const dbPath = join(tempDir, 'closed-listen.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.close()
      expect(() => db.addCloseListener(() => {})).toThrow(SirannonError)
    })

    it('invokes listeners during normal close', async () => {
      const dbPath = join(tempDir, 'pool-err.db')
      const db = await Database.create('test', dbPath, testDriver)
      let listenerCalled = false
      db.addCloseListener(() => {
        listenerCalled = true
      })
      await db.close()
      expect(listenerCalled).toBe(true)
    })

    it('swallows backup cancel errors during close', async () => {
      const dbPath = join(tempDir, 'close-cancel-error.db')
      const db = await Database.create('test', dbPath, testDriver)
      ;(db as unknown as { scheduledBackupCancellers: (() => void)[] }).scheduledBackupCancellers.push(() => {
        throw new Error('cancel failed')
      })

      await expect(db.close()).resolves.not.toThrow()
    })

    it('rethrows pool close errors after listener processing', async () => {
      const dbPath = join(tempDir, 'pool-close-error.db')
      const db = await Database.create('test', dbPath, testDriver)
      ;(db as unknown as { pool: { close: () => Promise<void> } }).pool.close = async () => {
        throw new Error('pool close failed')
      }

      await expect(db.close()).rejects.toThrow('pool close failed')
    })
  })

  describe('backup scheduling', () => {
    it('stores backup cancel functions from scheduleBackup', async () => {
      const dbPath = join(tempDir, 'schedule.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      db.scheduleBackup({
        cron: '0 0 1 1 *',
        destDir: join(tempDir, 'scheduled-backups'),
      })

      const cancellers = (db as unknown as { scheduledBackupCancellers: (() => void)[] }).scheduledBackupCancellers
      expect(cancellers).toHaveLength(1)
      await db.close()
    })
  })

  describe('CDC guard branches', () => {
    it('unwatch returns when CDC has not been initialized', async () => {
      const db = await createTestDb()
      await expect(db.unwatch('users')).resolves.not.toThrow()
      await db.close()
    })

    it('unwatch does not stop polling when other watched tables remain', async () => {
      const dbPath = join(tempDir, 'watch-multi.db')
      const db = await Database.create('test', dbPath, testDriver)
      openDbs.push(db)
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      await db.watch('users')
      await db.watch('posts')

      const stopFn = vi.fn()
      ;(db as unknown as { stopCdcPolling: (() => void) | null }).stopCdcPolling = stopFn
      await db.unwatch('users')

      expect(stopFn).not.toHaveBeenCalled()
      await db.close()
    })

    it('throws when subscription manager is unexpectedly missing', async () => {
      const db = await createTestDb()
      ;(db as unknown as { ensureCdc: () => void }).ensureCdc = () => {}
      ;(db as unknown as { subscriptionManager: unknown }).subscriptionManager = null

      expect(() => db.on('users')).toThrow('subscriptionManager not initialized')
      await db.close()
    })

    it('ensureCdcPolling returns when CDC objects are absent', async () => {
      const db = await createTestDb()
      const pool = (db as unknown as { pool: { acquireWriter: () => unknown } }).pool
      const acquireWriter = vi.spyOn(pool, 'acquireWriter')

      ;(db as unknown as { ensureCdcPolling: () => void }).ensureCdcPolling()

      expect(acquireWriter).not.toHaveBeenCalled()
      await db.close()
    })

    it('ensureCdcPolling returns when polling is already active', async () => {
      const db = await createTestDb()
      const pool = (db as unknown as { pool: { acquireWriter: () => unknown } }).pool
      const acquireWriter = vi.spyOn(pool, 'acquireWriter')
      ;(db as unknown as { stopCdcPolling: (() => void) | null }).stopCdcPolling = vi.fn()

      ;(db as unknown as { ensureCdcPolling: () => void }).ensureCdcPolling()

      expect(acquireWriter).not.toHaveBeenCalled()
      await db.close()
    })
  })

  describe('loadExtension validation', () => {
    it('rejects empty extension path', async () => {
      const db = await createTestDb()
      await expect(db.loadExtension('')).rejects.toThrow(ExtensionError)
      await expect(db.loadExtension('')).rejects.toThrow('empty or contains null bytes')
    })

    it('rejects paths with null bytes', async () => {
      const db = await createTestDb()
      await expect(db.loadExtension('/lib/ext\x00.so')).rejects.toThrow(ExtensionError)
      await expect(db.loadExtension('/lib/ext\x00.so')).rejects.toThrow('empty or contains null bytes')
    })

    it('rejects paths with directory traversal segments', async () => {
      const db = await createTestDb()
      await expect(db.loadExtension('/lib/../etc/ext.so')).rejects.toThrow(ExtensionError)
      await expect(db.loadExtension('/lib/../etc/ext.so')).rejects.toThrow('directory traversal')
    })

    it('rejects relative paths with leading traversal', async () => {
      const db = await createTestDb()
      await expect(db.loadExtension('../sneaky/ext.so')).rejects.toThrow(ExtensionError)
    })

    it('throws ExtensionError for nonexistent extension file', async () => {
      const db = await createTestDb()
      await expect(db.loadExtension('/nonexistent/path/ext.so')).rejects.toThrow(ExtensionError)
    })

    it('converts non-Error extension load failures', async () => {
      const db = await createTestDb()
      ;(
        db as unknown as { pool: { acquireWriter: () => { exec: (sql: string) => Promise<void> } } }
      ).pool.acquireWriter = () => ({
        exec: async () => {
          throw 'load failed'
        },
      })

      try {
        await db.loadExtension('ext.so')
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(ExtensionError)
        expect((err as ExtensionError).message).toContain('load failed')
      }

      await db.close()
    })
  })

  describe('executeBatch transaction behavior', () => {
    it('rolls back all rows on batch failure (implicit transaction)', async () => {
      const dbPath = join(tempDir, 'batch-tx.db')
      const db = await Database.create('test', dbPath, testDriver)
      openDbs.push(db)
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
