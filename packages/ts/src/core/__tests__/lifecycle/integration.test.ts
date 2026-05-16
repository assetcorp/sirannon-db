import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { createRegistry, getTempDir } from './setup.js'

describe('LifecycleManager', () => {
  describe('integration: manual open/close alongside lifecycle', () => {
    it('manual open and close does not interfere with lifecycle tracking', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
            maxOpen: 5,
          },
          callbacks,
        )

        await callbacks.open('manual', join(tempDir, 'manual.db'))

        await manager.resolve('auto1')

        expect(dbs.size).toBe(2)

        await vi.advanceTimersByTimeAsync(1500)

        expect(callbacks.has('manual')).toBe(true)
        expect(callbacks.has('auto1')).toBe(false)

        manager.dispose()
        await callbacks.close('manual')
      } finally {
        vi.useRealTimers()
      }
    })

    it('maxOpen counts both manual and lifecycle databases', async () => {
      const tempDir = getTempDir()
      const { callbacks } = createRegistry()

      await callbacks.open('manual1', join(tempDir, 'manual1.db'))
      await callbacks.open('manual2', join(tempDir, 'manual2.db'))

      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: 3,
        },
        callbacks,
      )

      const db = await manager.resolve('auto1')
      expect(db).toBeDefined()

      await manager.resolve('auto2')
      expect(callbacks.has('auto1')).toBe(false)
      expect(callbacks.has('auto2')).toBe(true)

      manager.dispose()
      await callbacks.close('manual1')
      await callbacks.close('manual2')
      await callbacks.close('auto2')
    })
  })
})
