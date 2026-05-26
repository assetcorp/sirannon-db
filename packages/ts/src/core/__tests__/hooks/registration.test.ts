import { describe, expect, it, vi } from 'vitest'
import { HookRegistry } from '../../hooks/registry.js'
import type { QueryHookContext } from '../../types.js'

describe('HookRegistry', () => {
  const queryCtx: QueryHookContext = {
    databaseId: 'main',
    sql: 'SELECT 1',
  }

  describe('register and invoke', () => {
    it('invokes a registered hook with the provided context', async () => {
      const registry = new HookRegistry()
      const hook = vi.fn()

      registry.register('beforeQuery', hook)
      await registry.invoke('beforeQuery', queryCtx)

      expect(hook).toHaveBeenCalledOnce()
      expect(hook).toHaveBeenCalledWith(queryCtx)
    })

    it('invokes multiple hooks in registration order', async () => {
      const registry = new HookRegistry()
      const order: number[] = []

      registry.register('beforeQuery', () => {
        order.push(1)
      })
      registry.register('beforeQuery', () => {
        order.push(2)
      })
      registry.register('beforeQuery', () => {
        order.push(3)
      })

      await registry.invoke('beforeQuery', queryCtx)

      expect(order).toEqual([1, 2, 3])
    })

    it('does nothing when invoking an event with no hooks', async () => {
      const registry = new HookRegistry()
      await expect(registry.invoke('beforeQuery', queryCtx)).resolves.toBeUndefined()
    })

    it('keeps hooks for different events independent', async () => {
      const registry = new HookRegistry()
      const beforeQuery = vi.fn()
      const afterQuery = vi.fn()

      registry.register('beforeQuery', beforeQuery)
      registry.register('afterQuery', afterQuery)

      await registry.invoke('beforeQuery', queryCtx)

      expect(beforeQuery).toHaveBeenCalledOnce()
      expect(afterQuery).not.toHaveBeenCalled()
    })
  })

  describe('duplicate registration', () => {
    it('calls the same function twice when registered twice', async () => {
      const registry = new HookRegistry()
      const hook = vi.fn()

      registry.register('beforeQuery', hook)
      registry.register('beforeQuery', hook)

      await registry.invoke('beforeQuery', queryCtx)
      expect(hook).toHaveBeenCalledTimes(2)
    })

    it('disposes only one instance when the same function is registered twice', async () => {
      const registry = new HookRegistry()
      const hook = vi.fn()

      const dispose1 = registry.register('beforeQuery', hook)
      registry.register('beforeQuery', hook)

      dispose1()
      await registry.invoke('beforeQuery', queryCtx)
      expect(hook).toHaveBeenCalledOnce()
    })
  })

  describe('has and count', () => {
    it('reports whether hooks are registered for an event', () => {
      const registry = new HookRegistry()

      expect(registry.has('beforeQuery')).toBe(false)
      registry.register('beforeQuery', vi.fn())
      expect(registry.has('beforeQuery')).toBe(true)
    })

    it('reports the number of hooks registered for an event', () => {
      const registry = new HookRegistry()

      expect(registry.count('beforeQuery')).toBe(0)
      registry.register('beforeQuery', vi.fn())
      registry.register('beforeQuery', vi.fn())
      expect(registry.count('beforeQuery')).toBe(2)
    })
  })

  describe('clear', () => {
    it('clears hooks for a specific event', async () => {
      const registry = new HookRegistry()
      const hook = vi.fn()

      registry.register('beforeQuery', hook)
      registry.clear('beforeQuery')
      await registry.invoke('beforeQuery', queryCtx)

      expect(hook).not.toHaveBeenCalled()
    })

    it('clears all hooks when called without arguments', async () => {
      const registry = new HookRegistry()
      const beforeQuery = vi.fn()
      const afterQuery = vi.fn()

      registry.register('beforeQuery', beforeQuery)
      registry.register('afterQuery', afterQuery)
      registry.clear()

      await registry.invoke('beforeQuery', queryCtx)
      await registry.invoke('afterQuery', {
        ...queryCtx,
        durationMs: 1,
      })

      expect(beforeQuery).not.toHaveBeenCalled()
      expect(afterQuery).not.toHaveBeenCalled()
    })
  })
})
