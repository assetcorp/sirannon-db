import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { MaxDatabasesError, SirannonError } from '../../errors.js'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { createRegistry, getTempDir } from './setup.js'

describe('LifecycleManager', () => {
  describe('resolve', () => {
    it('auto-opens a database when resolver returns a match', async () => {
      const tempDir = getTempDir()
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )

      const db = await manager.resolve('tenantA')
      expect(db).toBeDefined()
      expect(db?.id).toBe('tenantA')
      expect(dbs.has('tenantA')).toBe(true)

      manager.dispose()
      await db?.close()
    })

    it('returns undefined when no resolver is configured', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      expect(await manager.resolve('anything')).toBeUndefined()
      manager.dispose()
    })

    it('returns undefined when resolver returns undefined', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: () => undefined,
          },
        },
        callbacks,
      )

      expect(await manager.resolve('unknown')).toBeUndefined()
      manager.dispose()
    })

    it('marks the database as active after resolve', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )

      await manager.resolve('db1')
      expect(manager.trackedCount).toBe(1)

      manager.dispose()
      await callbacks.close('db1')
    })

    it('evicts LRU database when at maxOpen capacity', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            maxOpen: 2,
          },
          callbacks,
        )

        await manager.resolve('db1')
        vi.advanceTimersByTime(100)
        await manager.resolve('db2')
        vi.advanceTimersByTime(100)

        expect(dbs.size).toBe(2)

        const db3 = await manager.resolve('db3')
        expect(db3).toBeDefined()
        expect(dbs.has('db1')).toBe(false)
        expect(dbs.has('db2')).toBe(true)
        expect(dbs.has('db3')).toBe(true)

        manager.dispose()
        for (const db of dbs.values()) await db.close()
      } finally {
        vi.useRealTimers()
      }
    })

    it('throws MaxDatabasesError when at capacity and nothing to evict', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()
      await callbacks.open('manual1', join(tempDir, 'manual1.db'))
      await callbacks.open('manual2', join(tempDir, 'manual2.db'))

      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: 2,
        },
        callbacks,
      )

      await expect(manager.resolve('new')).rejects.toThrow(MaxDatabasesError)

      manager.dispose()
      await callbacks.close('manual1')
      await callbacks.close('manual2')
    })

    it('throws after dispose', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )
      manager.dispose()

      await expect(manager.resolve('anything')).rejects.toThrow(SirannonError)
      try {
        await manager.resolve('anything')
      } catch (err) {
        expect((err as SirannonError).code).toBe('LIFECYCLE_DISPOSED')
      }
    })

    it('propagates open errors from callbacks without leaving stale tracking', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: () => ({
              path: join(tempDir, 'no', 'such', 'dir', 'test.db'),
            }),
          },
        },
        callbacks,
      )

      await expect(manager.resolve('bad')).rejects.toThrow()
      expect(manager.trackedCount).toBe(0)
      manager.dispose()
    })

    it('handles negative maxOpen as unlimited', async () => {
      const tempDir = getTempDir()
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: -1,
        },
        callbacks,
      )

      await manager.resolve('db1')
      await manager.resolve('db2')
      await manager.resolve('db3')
      expect(dbs.size).toBe(3)

      manager.dispose()
      for (const db of dbs.values()) await db.close()
    })

    it('works with maxOpen of 1 (single slot)', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            maxOpen: 1,
          },
          callbacks,
        )

        await manager.resolve('db1')
        expect(dbs.size).toBe(1)

        vi.advanceTimersByTime(100)
        await manager.resolve('db2')

        expect(dbs.size).toBe(1)
        expect(dbs.has('db1')).toBe(false)
        expect(dbs.has('db2')).toBe(true)

        manager.dispose()
        for (const db of dbs.values()) await db.close()
      } finally {
        vi.useRealTimers()
      }
    })

    it('passes database options from resolver through to open', async () => {
      const tempDir = getTempDir()
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({
              path: join(tempDir, `${id}.db`),
              options: { readPoolSize: 2 },
            }),
          },
        },
        callbacks,
      )

      const db = await manager.resolve('custom')
      expect(db).toBeDefined()
      expect(db?.readerCount).toBe(2)

      manager.dispose()
      for (const d of dbs.values()) await d.close()
    })
  })
})
