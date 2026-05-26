import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { createRegistry, getTempDir } from './setup.js'

describe('LifecycleManager', () => {
  describe('evict', () => {
    it('closes the least recently used database', async () => {
      vi.useFakeTimers()
      try {
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
        vi.advanceTimersByTime(100)
        await manager.resolve('db2')
        vi.advanceTimersByTime(100)
        await manager.resolve('db3')

        await manager.evict()

        expect(callbacks.has('db1')).toBe(false)
        expect(callbacks.has('db2')).toBe(true)
        expect(callbacks.has('db3')).toBe(true)

        manager.dispose()
        await callbacks.close('db2')
        await callbacks.close('db3')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does nothing when no databases are tracked', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      await expect(manager.evict()).resolves.not.toThrow()

      manager.dispose()
    })

    it('cleans up stale entries before evicting', async () => {
      vi.useFakeTimers()
      try {
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
        vi.advanceTimersByTime(100)
        await manager.resolve('db2')

        await callbacks.close('db1')

        await manager.evict()

        expect(callbacks.has('db2')).toBe(false)
        expect(manager.trackedCount).toBe(0)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('evicts after markActive reorders access', async () => {
      vi.useFakeTimers()
      try {
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
        vi.advanceTimersByTime(100)
        await manager.resolve('db2')
        vi.advanceTimersByTime(100)

        manager.markActive('db1')

        await manager.evict()

        expect(callbacks.has('db1')).toBe(true)
        expect(callbacks.has('db2')).toBe(false)

        manager.dispose()
        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })
  })
})
