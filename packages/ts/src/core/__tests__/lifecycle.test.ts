import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Database } from '../database.js'
import { MaxDatabasesError, SirannonError } from '../errors.js'
import type { LifecycleCallbacks } from '../lifecycle/manager.js'
import { LifecycleManager } from '../lifecycle/manager.js'
import { createTenantResolver, sanitizeTenantId, tenantPath } from '../lifecycle/tenant.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-lifecycle-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

function createRegistry() {
  const dbs = new Map<string, Database>()

  const callbacks: LifecycleCallbacks = {
    async open(id: string, path: string, options?) {
      const db = await Database.create(id, path, testDriver, options)
      db.addCloseListener(() => dbs.delete(id))
      dbs.set(id, db)
      return db
    },
    async close(id: string) {
      const db = dbs.get(id)
      if (!db) throw new Error(`Database '${id}' not found`)
      await db.close()
    },
    count: () => dbs.size,
    has: (id: string) => dbs.has(id),
  }

  return { dbs, callbacks }
}

describe('LifecycleManager', () => {
  describe('resolve', () => {
    it('auto-opens a database when resolver returns a match', async () => {
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )

      const db = await manager.resolve('tenantA')
      expect(db).toBeDefined()
      expect(db?.id).toBe('tenantA')
      expect(dbs.has('tenantA')).toBe(true)

      manager.dispose()
      await db?.close()
    })

    it('returns undefined when no resolver is configured', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      expect(await manager.resolve('anything')).toBeUndefined()
      manager.dispose()
    })

    it('returns undefined when resolver returns undefined', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: () => undefined,
          },
        },
        callbacks,
      )

      expect(await manager.resolve('unknown')).toBeUndefined()
      manager.dispose()
    })

    it('marks the database as active after resolve', async () => {
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

      manager.dispose()
      await callbacks.close('db1')
    })

    it('evicts LRU database when at maxOpen capacity', async () => {
      vi.useFakeTimers()
      try {
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            maxOpen: 2,
          },
          callbacks,
        )

        await manager.resolve('db1')
        vi.advanceTimersByTime(100)
        await manager.resolve('db2')
        vi.advanceTimersByTime(100)

        expect(dbs.size).toBe(2)

        const db3 = await manager.resolve('db3')
        expect(db3).toBeDefined()
        expect(dbs.has('db1')).toBe(false)
        expect(dbs.has('db2')).toBe(true)
        expect(dbs.has('db3')).toBe(true)

        manager.dispose()
        for (const db of dbs.values()) await db.close()
      } finally {
        vi.useRealTimers()
      }
    })

    it('throws MaxDatabasesError when at capacity and nothing to evict', async () => {
      const { callbacks } = createRegistry()
      await callbacks.open('manual1', join(tempDir, 'manual1.db'))
      await callbacks.open('manual2', join(tempDir, 'manual2.db'))

      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: 2,
        },
        callbacks,
      )

      await expect(manager.resolve('new')).rejects.toThrow(MaxDatabasesError)

      manager.dispose()
      await callbacks.close('manual1')
      await callbacks.close('manual2')
    })

    it('throws after dispose', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )
      manager.dispose()

      await expect(manager.resolve('anything')).rejects.toThrow(SirannonError)
      try {
        await manager.resolve('anything')
      } catch (err) {
        expect((err as SirannonError).code).toBe('LIFECYCLE_DISPOSED')
      }
    })

    it('propagates open errors from callbacks without leaving stale tracking', async () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: () => ({
              path: join(tempDir, 'no', 'such', 'dir', 'test.db'),
            }),
          },
        },
        callbacks,
      )

      await expect(manager.resolve('bad')).rejects.toThrow()
      expect(manager.trackedCount).toBe(0)
      manager.dispose()
    })

    it('handles negative maxOpen as unlimited', async () => {
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: -1,
        },
        callbacks,
      )

      await manager.resolve('db1')
      await manager.resolve('db2')
      await manager.resolve('db3')
      expect(dbs.size).toBe(3)

      manager.dispose()
      for (const db of dbs.values()) await db.close()
    })

    it('works with maxOpen of 1 (single slot)', async () => {
      vi.useFakeTimers()
      try {
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: id => ({ path: join(tempDir, `${id}.db`) }),
            },
            maxOpen: 1,
          },
          callbacks,
        )

        await manager.resolve('db1')
        expect(dbs.size).toBe(1)

        vi.advanceTimersByTime(100)
        await manager.resolve('db2')

        expect(dbs.size).toBe(1)
        expect(dbs.has('db1')).toBe(false)
        expect(dbs.has('db2')).toBe(true)

        manager.dispose()
        for (const db of dbs.values()) await db.close()
      } finally {
        vi.useRealTimers()
      }
    })

    it('passes database options from resolver through to open', async () => {
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: id => ({
              path: join(tempDir, `${id}.db`),
              options: { readPoolSize: 2 },
            }),
          },
        },
        callbacks,
      )

      const db = await manager.resolve('custom')
      expect(db).toBeDefined()
      expect(db?.readerCount).toBe(2)

      manager.dispose()
      for (const d of dbs.values()) await d.close()
    })
  })

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

  describe('checkIdle', () => {
    it('closes databases past the idle timeout', async () => {
      vi.useFakeTimers()
      try {
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

  describe('evict', () => {
    it('closes the least recently used database', async () => {
      vi.useFakeTimers()
      try {
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

  describe('idle timer', () => {
    it('starts a timer and periodically closes idle databases', async () => {
      vi.useFakeTimers()
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

        await manager.resolve('db1')
        expect(callbacks.has('db1')).toBe(true)

        await vi.advanceTimersByTimeAsync(1500)

        expect(callbacks.has('db1')).toBe(false)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is 0', async () => {
      vi.useFakeTimers()
      try {
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
      const originalSetInterval = globalThis.setInterval
      const originalClearInterval = globalThis.clearInterval
      const setIntervalSpy = vi.spyOn(globalThis, 'setInterval').mockImplementation((fn: TimerHandler) => {
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

  describe('untrack', () => {
    it('removes a database from idle tracking', async () => {
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

  describe('integration: manual open/close alongside lifecycle', () => {
    it('manual open and close does not interfere with lifecycle tracking', async () => {
      vi.useFakeTimers()
      try {
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

describe('tenant utilities', () => {
  describe('sanitizeTenantId', () => {
    it('allows simple alphanumeric IDs', () => {
      expect(sanitizeTenantId('tenant1')).toBe('tenant1')
      expect(sanitizeTenantId('ABC')).toBe('ABC')
      expect(sanitizeTenantId('a')).toBe('a')
    })

    it('allows hyphens and underscores after the first character', () => {
      expect(sanitizeTenantId('my-tenant')).toBe('my-tenant')
      expect(sanitizeTenantId('my_tenant')).toBe('my_tenant')
      expect(sanitizeTenantId('a-b-c_d')).toBe('a-b-c_d')
    })

    it('rejects IDs starting with a hyphen', () => {
      expect(sanitizeTenantId('-invalid')).toBeUndefined()
    })

    it('rejects IDs starting with an underscore', () => {
      expect(sanitizeTenantId('_invalid')).toBeUndefined()
    })

    it('rejects path traversal attempts', () => {
      expect(sanitizeTenantId('../etc/passwd')).toBeUndefined()
      expect(sanitizeTenantId('..%2F..%2Fetc')).toBeUndefined()
      expect(sanitizeTenantId('foo/../bar')).toBeUndefined()
    })

    it('rejects slashes', () => {
      expect(sanitizeTenantId('a/b')).toBeUndefined()
      expect(sanitizeTenantId('a\\b')).toBeUndefined()
    })

    it('rejects empty strings', () => {
      expect(sanitizeTenantId('')).toBeUndefined()
    })

    it('rejects IDs longer than 255 characters', () => {
      const long = 'a'.repeat(256)
      expect(sanitizeTenantId(long)).toBeUndefined()

      const exact = 'a'.repeat(255)
      expect(sanitizeTenantId(exact)).toBe(exact)
    })

    it('rejects special characters', () => {
      expect(sanitizeTenantId('ten ant')).toBeUndefined()
      expect(sanitizeTenantId('ten@ant')).toBeUndefined()
      expect(sanitizeTenantId('ten.ant')).toBeUndefined()
      expect(sanitizeTenantId('ten$ant')).toBeUndefined()
    })
  })

  describe('tenantPath', () => {
    it('generates a path from basePath and tenantId', () => {
      const result = tenantPath('/data/tenants', 'acme')
      expect(result).toBe('/data/tenants/acme.db')
    })

    it('uses a custom extension', () => {
      const result = tenantPath('/data', 'acme', '.sqlite')
      expect(result).toBe('/data/acme.sqlite')
    })

    it('throws on invalid tenant ID', () => {
      expect(() => tenantPath('/data', '../escape')).toThrow('Invalid tenant ID')
      expect(() => tenantPath('/data', '')).toThrow('Invalid tenant ID')
      expect(() => tenantPath('/data', '-bad')).toThrow('Invalid tenant ID')
    })

    it('throws when filename (id + extension) exceeds 255 characters', () => {
      const longId = 'a'.repeat(253)
      expect(() => tenantPath('/data', longId)).toThrow('maximum length')

      const okId = 'a'.repeat(252)
      expect(() => tenantPath('/data', okId)).not.toThrow()
    })

    it('throws when a long extension pushes filename past the limit', () => {
      const id = 'a'.repeat(250)
      expect(() => tenantPath('/data', id, '.sqlite3')).toThrow('maximum length')
    })
  })

  describe('createTenantResolver', () => {
    it('creates a resolver that maps IDs to paths', () => {
      const resolver = createTenantResolver({ basePath: '/data/dbs' })

      const result = resolver('tenant1')
      expect(result).toBeDefined()
      expect(result?.path).toBe('/data/dbs/tenant1.db')
    })

    it('uses custom extension', () => {
      const resolver = createTenantResolver({
        basePath: '/data',
        extension: '.sqlite3',
      })

      const result = resolver('mydb')
      expect(result?.path).toBe('/data/mydb.sqlite3')
    })

    it('returns undefined for invalid IDs', () => {
      const resolver = createTenantResolver({ basePath: '/data' })

      expect(resolver('../escape')).toBeUndefined()
      expect(resolver('')).toBeUndefined()
      expect(resolver('a/b')).toBeUndefined()
      expect(resolver('-bad')).toBeUndefined()
    })

    it('passes through default options', () => {
      const resolver = createTenantResolver({
        basePath: '/data',
        defaultOptions: { readOnly: true, readPoolSize: 2 },
      })

      const result = resolver('tenant1')
      expect(result).toBeDefined()
      expect(result?.options).toEqual({ readOnly: true, readPoolSize: 2 })
    })

    it('returns undefined options when none are configured', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const result = resolver('tenant1')
      expect(result?.options).toBeUndefined()
    })

    it('returns undefined when filename exceeds 255 characters', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const longId = 'a'.repeat(253)
      expect(resolver(longId)).toBeUndefined()
    })

    it('accepts IDs where filename is exactly 255 characters', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const okId = 'a'.repeat(252)
      expect(resolver(okId)).toBeDefined()
    })

    it('works with LifecycleManager for end-to-end tenant resolution', async () => {
      const { dbs, callbacks } = createRegistry()
      const resolver = createTenantResolver({ basePath: tempDir })

      const manager = new LifecycleManager({ autoOpen: { resolver } }, callbacks)

      const db = await manager.resolve('acme')
      expect(db).toBeDefined()
      expect(db?.id).toBe('acme')
      expect(db?.path).toBe(`${tempDir}/acme.db`)
      expect(dbs.has('acme')).toBe(true)

      manager.dispose()
      await db?.close()
    })
  })
})
