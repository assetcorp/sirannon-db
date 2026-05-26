import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { Database } from '../../database.js'
import type { LifecycleCallbacks } from '../../lifecycle/manager.js'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { testDriver } from '../helpers/test-driver.js'
import { createRegistry, getTempDir } from './setup.js'

describe('LifecycleManager', () => {
  describe('checkIdle', () => {
    it('closes databases past the idle timeout', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 500,
          },
          callbacks,
        )

        await manager.resolve('db1')
        expect(callbacks.has('db1')).toBe(true)

        await vi.advanceTimersByTimeAsync(800)

        expect(callbacks.has('db1')).toBe(false)
        expect(manager.trackedCount).toBe(0)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not close databases that are still active', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
          },
          callbacks,
        )

        await manager.resolve('db1')
        vi.advanceTimersByTime(500)

        await manager.checkIdle()

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('handles idleTimeout of 0 (disabled)', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          idleTimeout: 0,
        },
        callbacks,
      )

      await manager.resolve('db1')
      await manager.checkIdle()

      expect(callbacks.has('db1')).toBe(true)

      manager.dispose()
      await callbacks.close('db1')
    })

    it('cleans up tracking for externally closed databases', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          idleTimeout: 10_000,
        },
        callbacks,
      )

      await manager.resolve('db1')
      expect(manager.trackedCount).toBe(1)

      await callbacks.close('db1')
      await manager.checkIdle()

      expect(manager.trackedCount).toBe(0)

      manager.dispose()
    })

    it('swallows close errors during idle cleanup', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const closeFn = vi.fn(async () => {
          throw new Error('close failed')
        })
        const callbacks: LifecycleCallbacks = {
          open: async (id, path) => {
            const db = await Database.create(id, path, testDriver)
            return db
          },
          close: closeFn,
          count: () => 1,
          has: () => true,
        }

        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 100,
          },
          callbacks,
        )

        await manager.resolve('db1')
        vi.advanceTimersByTime(200)

        await expect(manager.checkIdle()).resolves.not.toThrow()
        expect(closeFn).toHaveBeenCalledWith('db1')

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('closes multiple idle databases in a single pass', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 500,
          },
          callbacks,
        )

        await manager.resolve('db1')
        await manager.resolve('db2')
        await manager.resolve('db3')

        await vi.advanceTimersByTimeAsync(800)

        expect(callbacks.count()).toBe(0)
        expect(manager.trackedCount).toBe(0)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('only closes idle databases while keeping active ones', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
          },
          callbacks,
        )

        await manager.resolve('db1')
        vi.advanceTimersByTime(500)
        await manager.resolve('db2')

        await vi.advanceTimersByTimeAsync(600)

        expect(callbacks.has('db1')).toBe(false)
        expect(callbacks.has('db2')).toBe(true)

        manager.dispose()
        await callbacks.close('db2')
      } finally {
        vi.useRealTimers()
      }
    })
  })
})
