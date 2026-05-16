import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { ReadOnlyError, SirannonError } from '../../errors.js'
import { MetricsCollector } from '../../metrics/collector.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, getTempDir, state } from './setup.js'

describe('Database', () => {
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
      const dbPath = join(getTempDir(), 'meta.db')
      const db = await Database.create('my-db', dbPath, testDriver)
      expect(db.id).toBe('my-db')
      expect(db.path).toBe(dbPath)
      expect(db.readOnly).toBe(false)
      expect(db.closed).toBe(false)
      await db.close()
      expect(db.closed).toBe(true)
    })

    it('throws SirannonError with DATABASE_CLOSED after close', async () => {
      const dbPath = join(getTempDir(), 'closed.db')
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
      const dbPath = join(getTempDir(), 'idem.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.close()
      await expect(db.close()).resolves.not.toThrow()
    })

    it('reports reader count', async () => {
      const dbPath = join(getTempDir(), 'readers.db')
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

      const dbPath = join(getTempDir(), 'metrics-queryone.db')
      const db = await Database.create('test', dbPath, testDriver, undefined, { metrics: collector })
      state.openDbs.push(db)
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
      const dbPath = join(getTempDir(), 'listener.db')
      const db = await Database.create('test', dbPath, testDriver)
      let called = false
      db.addCloseListener(() => {
        called = true
      })
      await db.close()
      expect(called).toBe(true)
    })

    it('invokes multiple listeners in order', async () => {
      const dbPath = join(getTempDir(), 'multi.db')
      const db = await Database.create('test', dbPath, testDriver)
      const order: number[] = []
      db.addCloseListener(() => {
        order.push(1)
      })
      db.addCloseListener(() => {
        order.push(2)
      })
      db.addCloseListener(() => {
        order.push(3)
      })
      await db.close()
      expect(order).toEqual([1, 2, 3])
    })

    it('does not invoke listeners on second close call', async () => {
      const dbPath = join(getTempDir(), 'once.db')
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
      const dbPath = join(getTempDir(), 'closed-listen.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.close()
      expect(() => db.addCloseListener(() => {})).toThrow(SirannonError)
    })

    it('invokes listeners during normal close', async () => {
      const dbPath = join(getTempDir(), 'pool-err.db')
      const db = await Database.create('test', dbPath, testDriver)
      let listenerCalled = false
      db.addCloseListener(() => {
        listenerCalled = true
      })
      await db.close()
      expect(listenerCalled).toBe(true)
    })

    it('swallows backup cancel errors during close', async () => {
      const dbPath = join(getTempDir(), 'close-cancel-error.db')
      const db = await Database.create('test', dbPath, testDriver)
      ;(db as unknown as { scheduledBackupCancellers: (() => void)[] }).scheduledBackupCancellers.push(() => {
        throw new Error('cancel failed')
      })

      await expect(db.close()).resolves.not.toThrow()
    })

    it('rethrows pool close errors after listener processing', async () => {
      const dbPath = join(getTempDir(), 'pool-close-error.db')
      const db = await Database.create('test', dbPath, testDriver)
      ;(db as unknown as { pool: { close: () => Promise<void> } }).pool.close = async () => {
        throw new Error('pool close failed')
      }

      await expect(db.close()).rejects.toThrow('pool close failed')
    })
  })

  describe('backup scheduling', () => {
    it('stores backup cancel functions from scheduleBackup', async () => {
      const dbPath = join(getTempDir(), 'schedule.db')
      const db = await Database.create('test', dbPath, testDriver)
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      db.scheduleBackup({
        cron: '0 0 1 1 *',
        destDir: join(getTempDir(), 'scheduled-backups'),
      })

      const cancellers = (db as unknown as { scheduledBackupCancellers: (() => void)[] }).scheduledBackupCancellers
      expect(cancellers).toHaveLength(1)
      await db.close()
    })
  })
})
