import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { createRegistry, getTempDir } from './setup.js'

describe('LifecycleManager', () => {
  describe('markActive', () => {
    it('tracks access time for a database', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      manager.markActive('db1')
      expect(manager.trackedCount).toBe(1)

      manager.dispose()
    })

    it('updates access time on subsequent calls', async () => {
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
        vi.advanceTimersByTime(800)

        manager.markActive('db1')
        vi.advanceTimersByTime(800)

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does nothing after dispose', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)
      manager.dispose()

      manager.markActive('db1')
      expect(manager.trackedCount).toBe(0)
    })
  })

  describe('untrack', () => {
    it('removes a database from idle tracking', async () => {
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

      manager.untrack('db1')
      expect(manager.trackedCount).toBe(0)

      manager.dispose()
      await callbacks.close('db1')
    })

    it('is safe to call for untracked IDs', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      expect(() => manager.untrack('nonexistent')).not.toThrow()
      manager.dispose()
    })
  })

  describe('dispose', () => {
    it('clears timers and tracking state', async () => {
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
      expect(manager.trackedCount).toBe(1)

      manager.dispose()

      expect(manager.disposed).toBe(true)
      expect(manager.trackedCount).toBe(0)

      await callbacks.close('db1')
    })

    it('is idempotent', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      manager.dispose()
      expect(() => manager.dispose()).not.toThrow()
      expect(manager.disposed).toBe(true)
    })
  })
})
