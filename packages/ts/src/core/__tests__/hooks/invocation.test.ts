import { describe, expect, it, vi } from 'vitest'
import { HookRegistry } from '../../hooks/registry.js'
import type { HookDispose } from '../../hooks/types.js'
import type { QueryHookContext } from '../../types.js'

describe('HookRegistry', () => {
  const queryCtx: QueryHookContext = {
    databaseId: 'main',
    sql: 'SELECT 1',
  }

  describe('async hooks', () => {
    it('awaits async hooks before proceeding to the next one', async () => {
      const registry = new HookRegistry()
      const order: number[] = []

      registry.register('beforeQuery', async () => {
        await new Promise(r => setTimeout(r, 10))
        order.push(1)
      })
      registry.register('beforeQuery', () => {
        order.push(2)
      })

      await registry.invoke('beforeQuery', queryCtx)

      expect(order).toEqual([1, 2])
    })
  })

  describe('throw-to-deny', () => {
    it('propagates errors thrown by sync hooks', async () => {
      const registry = new HookRegistry()
      registry.register('beforeQuery', () => {
        throw new Error('access denied')
      })

      await expect(registry.invoke('beforeQuery', queryCtx)).rejects.toThrow('access denied')
    })

    it('propagates errors thrown by async hooks', async () => {
      const registry = new HookRegistry()
      registry.register('beforeQuery', async () => {
        throw new Error('async denial')
      })

      await expect(registry.invoke('beforeQuery', queryCtx)).rejects.toThrow('async denial')
    })

    it('stops invoking remaining hooks after one throws', async () => {
      const registry = new HookRegistry()
      const secondHook = vi.fn()

      registry.register('beforeQuery', () => {
        throw new Error('denied')
      })
      registry.register('beforeQuery', secondHook)

      await expect(registry.invoke('beforeQuery', queryCtx)).rejects.toThrow('denied')
      expect(secondHook).not.toHaveBeenCalled()
    })
  })

  describe('snapshot isolation', () => {
    it('hooks registered during invocation do not run in the current cycle', async () => {
      const registry = new HookRegistry()
      const lateHook = vi.fn()

      registry.register('beforeQuery', () => {
        registry.register('beforeQuery', lateHook)
      })

      await registry.invoke('beforeQuery', queryCtx)
      expect(lateHook).not.toHaveBeenCalled()

      await registry.invoke('beforeQuery', queryCtx)
      expect(lateHook).toHaveBeenCalledOnce()
    })

    it('hooks disposed during invocation still run in the current cycle', async () => {
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

      await registry.invoke('beforeQuery', queryCtx)
      expect(order).toEqual([1, 2])

      order.length = 0
      await registry.invoke('beforeQuery', queryCtx)
      expect(order).toEqual([1])
    })
  })
})
