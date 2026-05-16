import { describe, expect, it, vi } from 'vitest'
import { HookRegistry } from '../../hooks/registry.js'
import type { HookConfig, QueryHookContext } from '../../types.js'

describe('HookRegistry', () => {
  const queryCtx: QueryHookContext = {
    databaseId: 'main',
    sql: 'SELECT 1',
  }

  describe('initialization from HookConfig', () => {
    it('loads single hooks from config', async () => {
      const beforeQuery = vi.fn()
      const afterQuery = vi.fn()
      const config: HookConfig = {
        onBeforeQuery: beforeQuery,
        onAfterQuery: afterQuery,
      }

      const registry = new HookRegistry(config)

      await registry.invoke('beforeQuery', queryCtx)
      await registry.invoke('afterQuery', {
        ...queryCtx,
        durationMs: 1,
      })

      expect(beforeQuery).toHaveBeenCalledOnce()
      expect(afterQuery).toHaveBeenCalledOnce()
    })

    it('loads hook arrays from config', async () => {
      const hook1 = vi.fn()
      const hook2 = vi.fn()
      const config: HookConfig = {
        onBeforeQuery: [hook1, hook2],
      }

      const registry = new HookRegistry(config)
      await registry.invoke('beforeQuery', queryCtx)

      expect(hook1).toHaveBeenCalledOnce()
      expect(hook2).toHaveBeenCalledOnce()
    })

    it('maps all config keys to the correct event names', async () => {
      const hooks = {
        onBeforeQuery: vi.fn(),
        onAfterQuery: vi.fn(),
        onBeforeConnect: vi.fn(),
        onDatabaseOpen: vi.fn(),
        onDatabaseClose: vi.fn(),
        onBeforeSubscribe: vi.fn(),
      }

      const registry = new HookRegistry(hooks)

      await registry.invoke('beforeQuery', queryCtx)
      await registry.invoke('afterQuery', {
        ...queryCtx,
        durationMs: 1,
      })
      await registry.invoke('beforeConnect', {
        databaseId: 'main',
        path: '/data/main.db',
      })
      await registry.invoke('databaseOpen', {
        databaseId: 'main',
        path: '/data/main.db',
      })
      await registry.invoke('databaseClose', {
        databaseId: 'main',
        path: '/data/main.db',
      })
      await registry.invoke('beforeSubscribe', {
        databaseId: 'main',
        table: 'users',
      })

      for (const hook of Object.values(hooks)) {
        expect(hook).toHaveBeenCalledOnce()
      }
    })
  })
})
