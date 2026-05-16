import { describe, expect, it, vi } from 'vitest'
import { HookRegistry } from '../../hooks/registry.js'
import type { HookDispose } from '../../hooks/types.js'
import type { QueryHookContext } from '../../types.js'

describe('HookRegistry', () => {
  const queryCtx: QueryHookContext = {
    databaseId: 'main',
    sql: 'SELECT 1',
  }

  describe('invokeSync', () => {
    it('runs sync hooks in registration order', () => {
      const registry = new HookRegistry()
      const order: number[] = []

      registry.register('beforeQuery', () => {
        order.push(1)
      })
      registry.register('beforeQuery', () => {
        order.push(2)
      })

      registry.invokeSync('beforeQuery', queryCtx)
      expect(order).toEqual([1, 2])
    })

    it('does nothing when no hooks are registered', () => {
      const registry = new HookRegistry()
      expect(() => registry.invokeSync('beforeQuery', queryCtx)).not.toThrow()
    })

    it('throws when a hook returns a Promise', () => {
      const registry = new HookRegistry()
      registry.register('beforeQuery', async () => {})

      expect(() => registry.invokeSync('beforeQuery', queryCtx)).toThrow("Hook for 'beforeQuery' returned a Promise")
    })

    it('propagates sync hook errors', () => {
      const registry = new HookRegistry()
      registry.register('beforeQuery', () => {
        throw new Error('sync denial')
      })

      expect(() => registry.invokeSync('beforeQuery', queryCtx)).toThrow('sync denial')
    })

    it('snapshots the array so mutations during invocation are safe', () => {
      const registry = new HookRegistry()
      const lateHook = vi.fn()

      registry.register('beforeQuery', () => {
        registry.register('beforeQuery', lateHook)
      })

      registry.invokeSync('beforeQuery', queryCtx)
      expect(lateHook).not.toHaveBeenCalled()

      registry.invokeSync('beforeQuery', queryCtx)
      expect(lateHook).toHaveBeenCalledOnce()
    })

    it('disposed hooks still run in the current sync cycle', () => {
      const registry = new HookRegistry()
      const order: number[] = []
      let dispose2: HookDispose

      registry.register('beforeQuery', () => {
        order.push(1)
        dispose2()
      })

      dispose2 = registry.register('beforeQuery', () => {
        order.push(2)
      })

      registry.invokeSync('beforeQuery', queryCtx)
      expect(order).toEqual([1, 2])

      order.length = 0
      registry.invokeSync('beforeQuery', queryCtx)
      expect(order).toEqual([1])
    })
  })
})
