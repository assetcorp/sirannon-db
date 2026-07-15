import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { Database } from '../../database.js'
import { ExtensionError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, getTempDir, state } from './setup.js'

describe('Database', () => {
  describe('CDC guard branches', () => {
    it('unwatch returns when CDC has not been initialized', async () => {
      const db = await createTestDb()
      await expect(db.unwatch('users')).resolves.not.toThrow()
      await db.close()
    })

    it('unwatch does not stop polling when other watched tables remain', async () => {
      const dbPath = join(getTempDir(), 'watch-multi.db')
      const db = await Database.create('test', dbPath, testDriver)
      state.openDbs.push(db)
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      await db.watch('users')
      await db.watch('posts')

      const stopFn = vi.fn()
      ;(db as unknown as { cdc: { stopPolling: (() => void) | null } }).cdc.stopPolling = stopFn
      await db.unwatch('users')

      expect(stopFn).not.toHaveBeenCalled()
      await db.close()
    })

    it('throws when subscription manager is unexpectedly missing', async () => {
      const db = await createTestDb()
      const cdc = db as unknown as { cdc: { ensure: () => void; subscriptions: unknown } }
      cdc.cdc.ensure = () => {}
      cdc.cdc.subscriptions = null

      expect(() => db.on('users')).toThrow('subscriptionManager not initialized')
      await db.close()
    })

    it('ensureCdcPolling returns when CDC objects are absent', async () => {
      const db = await createTestDb()
      const pool = (db as unknown as { pool: { acquireWriter: () => unknown } }).pool
      const acquireWriter = vi.spyOn(pool, 'acquireWriter')

      ;(db as unknown as { cdc: { ensurePolling: () => void } }).cdc.ensurePolling()

      expect(acquireWriter).not.toHaveBeenCalled()
      await db.close()
    })

    it('ensureCdcPolling returns when polling is already active', async () => {
      const db = await createTestDb()
      const pool = (db as unknown as { pool: { acquireWriter: () => unknown } }).pool
      const acquireWriter = vi.spyOn(pool, 'acquireWriter')
      ;(db as unknown as { cdc: { stopPolling: (() => void) | null } }).cdc.stopPolling = vi.fn()

      ;(db as unknown as { cdc: { ensurePolling: () => void } }).cdc.ensurePolling()

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
})
