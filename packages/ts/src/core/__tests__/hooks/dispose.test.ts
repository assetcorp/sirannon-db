import { describe, expect, it, vi } from 'vitest'
import { HookRegistry } from '../../hooks/registry.js'
import type { QueryHookContext } from '../../types.js'

describe('HookRegistry', () => {
  const queryCtx: QueryHookContext = {
    databaseId: 'main',
    sql: 'SELECT 1',
  }

  describe('dispose', () => {
    it('returns a dispose function that removes the hook', async () => {
      const registry = new HookRegistry()
      const hook = vi.fn()

      const dispose = registry.register('beforeQuery', hook)
      dispose()

      await registry.invoke('beforeQuery', queryCtx)
      expect(hook).not.toHaveBeenCalled()
    })

    it('is idempotent when called multiple times', () => {
      const registry = new HookRegistry()
      const hook = vi.fn()

      const dispose = registry.register('beforeQuery', hook)
      dispose()
      dispose()
      dispose()

      expect(registry.count('beforeQuery')).toBe(0)
    })

    it('removes only the specific hook, leaving others intact', async () => {
      const registry = new HookRegistry()
      const hookA = vi.fn()
      const hookB = vi.fn()

      const disposeA = registry.register('beforeQuery', hookA)
      registry.register('beforeQuery', hookB)

      disposeA()
      await registry.invoke('beforeQuery', queryCtx)

      expect(hookA).not.toHaveBeenCalled()
      expect(hookB).toHaveBeenCalledOnce()
    })

    it('reflects in has and count after disposal', () => {
      const registry = new HookRegistry()
      const dispose = registry.register('beforeQuery', vi.fn())

      expect(registry.has('beforeQuery')).toBe(true)
      expect(registry.count('beforeQuery')).toBe(1)

      dispose()

      expect(registry.has('beforeQuery')).toBe(false)
      expect(registry.count('beforeQuery')).toBe(0)
    })

    it('dispose is safe when hooks for the event were cleared', () => {
      const registry = new HookRegistry()
      const dispose = registry.register('beforeQuery', vi.fn())
      registry.clear('beforeQuery')

      expect(() => dispose()).not.toThrow()
    })

    it('dispose is safe when event exists but the target hook is already absent', () => {
      const registry = new HookRegistry()
      const dispose = registry.register('beforeQuery', vi.fn())
      registry.clear('beforeQuery')
      registry.register('beforeQuery', vi.fn())

      expect(() => dispose()).not.toThrow()
      expect(registry.count('beforeQuery')).toBe(1)
    })
  })
})
