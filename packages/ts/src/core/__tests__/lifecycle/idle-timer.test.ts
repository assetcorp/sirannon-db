import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import type { LifecycleCallbacks } from '../../lifecycle/manager.js'
import { LifecycleManager } from '../../lifecycle/manager.js'
import { createRegistry, getTempDir } from './setup.js'

describe('LifecycleManager', () => {
  describe('idle timer', () => {
    it('starts a timer and periodically closes idle databases', async () => {
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
        expect(callbacks.has('db1')).toBe(true)

        await vi.advanceTimersByTimeAsync(1500)

        expect(callbacks.has('db1')).toBe(false)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not overlap async idle cleanup between timer ticks', async () => {
      vi.useFakeTimers()
      try {
        let releaseClose: (() => void) | undefined
        let isOpen = true
        let activeCloses = 0
        let maxActiveCloses = 0

        const close = vi.fn(async () => {
          activeCloses += 1
          maxActiveCloses = Math.max(maxActiveCloses, activeCloses)

          await new Promise<void>(resolve => {
            releaseClose = () => {
              isOpen = false
              activeCloses -= 1
              resolve()
            }
          })
        })

        const callbacks: LifecycleCallbacks = {
          open: async () => {
            throw new Error('open should not be called')
          },
          close,
          count: () => 1,
          has: () => isOpen,
        }

        const manager = new LifecycleManager({ idleTimeout: 100 }, callbacks)
        manager.markActive('db1')

        await vi.advanceTimersByTimeAsync(100)
        expect(close).toHaveBeenCalledTimes(1)

        await vi.advanceTimersByTimeAsync(200)
        expect(close).toHaveBeenCalledTimes(1)
        expect(maxActiveCloses).toBe(1)

        releaseClose?.()
        await vi.advanceTimersByTimeAsync(0)

        expect(manager.trackedCount).toBe(0)
        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is 0', async () => {
      vi.useFakeTimers()
      try {
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
        vi.advanceTimersByTime(100_000)

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is negative', async () => {
      vi.useFakeTimers()
      try {
        const tempDir = getTempDir()
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: -500,
          },
          callbacks,
        )

        await manager.resolve('db1')
        vi.advanceTimersByTime(100_000)

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is undefined', async () => {
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
        vi.advanceTimersByTime(100_000)

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('stops the timer on dispose', async () => {
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
        manager.dispose()

        vi.advanceTimersByTime(10_000)
        expect(callbacks.has('db1')).toBe(true)

        await callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('handles timer handles without unref', () => {
      const tempDir = getTempDir()
      const originalSetInterval = globalThis.setInterval
      const originalClearInterval = globalThis.clearInterval
      const setIntervalSpy = vi
        .spyOn(globalThis, 'setInterval')
        .mockImplementation((fn: Parameters<typeof setInterval>[0]) => {
          if (typeof fn === 'function') {
            fn()
          }
          return 1 as unknown as ReturnType<typeof setInterval>
        })
      const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval').mockImplementation(() => {})

      try {
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

        expect(setIntervalSpy).toHaveBeenCalled()
        manager.dispose()
        expect(clearIntervalSpy).toHaveBeenCalled()
      } finally {
        setIntervalSpy.mockRestore()
        clearIntervalSpy.mockRestore()
        globalThis.setInterval = originalSetInterval
        globalThis.clearInterval = originalClearInterval
      }
    })
  })
})
