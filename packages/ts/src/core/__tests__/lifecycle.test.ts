import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Database } from '../database.js'
import { MaxDatabasesError, SirannonError } from '../errors.js'
import type { LifecycleCallbacks } from '../lifecycle/manager.js'
import { LifecycleManager } from '../lifecycle/manager.js'
import { createTenantResolver, sanitizeTenantId, tenantPath } from '../lifecycle/tenant.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-lifecycle-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

/**
 * Helpers: creates a real Database registry backed by a temp directory
 * so the LifecycleManager can be tested end-to-end against real SQLite.
 */
function createRegistry() {
  const dbs = new Map<string, Database>()

  const callbacks: LifecycleCallbacks = {
    open(id: string, path: string, options?) {
      const db = new Database(id, path, options)
      db.addCloseListener(() => dbs.delete(id))
      dbs.set(id, db)
      return db
    },
    close(id: string) {
      const db = dbs.get(id)
      if (!db) throw new Error(`Database '${id}' not found`)
      db.close()
    },
    count: () => dbs.size,
    has: (id: string) => dbs.has(id),
  }

  return { dbs, callbacks }
}

describe('LifecycleManager', () => {
  describe('resolve', () => {
    it('auto-opens a database when resolver returns a match', () => {
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )

      const db = manager.resolve('tenantA')
      expect(db).toBeDefined()
      expect(db!.id).toBe('tenantA')
      expect(dbs.has('tenantA')).toBe(true)

      manager.dispose()
      db!.close()
    })

    it('returns undefined when no resolver is configured', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      expect(manager.resolve('anything')).toBeUndefined()
      manager.dispose()
    })

    it('returns undefined when resolver returns undefined', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: () => undefined,
          },
        },
        callbacks,
      )

      expect(manager.resolve('unknown')).toBeUndefined()
      manager.dispose()
    })

    it('marks the database as active after resolve', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )

      manager.resolve('db1')
      expect(manager.trackedCount).toBe(1)

      manager.dispose()
      callbacks.close('db1')
    })

    it('evicts LRU database when at maxOpen capacity', () => {
      vi.useFakeTimers()
      try {
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            maxOpen: 2,
          },
          callbacks,
        )

        // Open two databases with staggered access times.
        manager.resolve('db1')
        vi.advanceTimersByTime(100)
        manager.resolve('db2')
        vi.advanceTimersByTime(100)

        expect(dbs.size).toBe(2)

        // Opening a third should evict db1 (the oldest).
        const db3 = manager.resolve('db3')
        expect(db3).toBeDefined()
        expect(dbs.has('db1')).toBe(false)
        expect(dbs.has('db2')).toBe(true)
        expect(dbs.has('db3')).toBe(true)

        manager.dispose()
        for (const db of dbs.values()) db.close()
      } finally {
        vi.useRealTimers()
      }
    })

    it('throws MaxDatabasesError when at capacity and nothing to evict', () => {
      const { callbacks } = createRegistry()
      // Manually open databases so they're not tracked by lifecycle manager.
      callbacks.open('manual1', join(tempDir, 'manual1.db'))
      callbacks.open('manual2', join(tempDir, 'manual2.db'))

      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: 2,
        },
        callbacks,
      )

      expect(() => manager.resolve('new')).toThrow(MaxDatabasesError)

      manager.dispose()
      callbacks.close('manual1')
      callbacks.close('manual2')
    })

    it('throws after dispose', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )
      manager.dispose()

      expect(() => manager.resolve('anything')).toThrow(SirannonError)
      try {
        manager.resolve('anything')
      } catch (err) {
        expect((err as SirannonError).code).toBe('LIFECYCLE_DISPOSED')
      }
    })

    it('propagates open errors from callbacks without leaving stale tracking', () => {
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

      expect(() => manager.resolve('bad')).toThrow()
      // markActive is only called after a successful open, so no stale entry.
      expect(manager.trackedCount).toBe(0)
      manager.dispose()
    })

    it('handles negative maxOpen as unlimited', () => {
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: -1,
        },
        callbacks,
      )

      manager.resolve('db1')
      manager.resolve('db2')
      manager.resolve('db3')
      expect(dbs.size).toBe(3)

      manager.dispose()
      for (const db of dbs.values()) db.close()
    })

    it('works with maxOpen of 1 (single slot)', () => {
      vi.useFakeTimers()
      try {
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            maxOpen: 1,
          },
          callbacks,
        )

        manager.resolve('db1')
        expect(dbs.size).toBe(1)

        vi.advanceTimersByTime(100)
        manager.resolve('db2')

        // db1 evicted, db2 is the only one.
        expect(dbs.size).toBe(1)
        expect(dbs.has('db1')).toBe(false)
        expect(dbs.has('db2')).toBe(true)

        manager.dispose()
        for (const db of dbs.values()) db.close()
      } finally {
        vi.useRealTimers()
      }
    })

    it('passes database options from resolver through to open', () => {
      const { dbs, callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({
              path: join(tempDir, `${id}.db`),
              options: { readPoolSize: 2 },
            }),
          },
        },
        callbacks,
      )

      const db = manager.resolve('custom')
      expect(db).toBeDefined()
      expect(db!.readerCount).toBe(2)

      manager.dispose()
      for (const d of dbs.values()) d.close()
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

    it('updates access time on subsequent calls', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(800)

        // Re-mark as active; should reset the idle clock.
        manager.markActive('db1')
        vi.advanceTimersByTime(800)

        // 800ms since last activity, which is under the 1000ms timeout.
        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does nothing after dispose', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)
      manager.dispose()

      // Should not throw.
      manager.markActive('db1')
      expect(manager.trackedCount).toBe(0)
    })
  })

  describe('checkIdle', () => {
    it('closes databases past the idle timeout', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 500,
          },
          callbacks,
        )

        manager.resolve('db1')
        expect(callbacks.has('db1')).toBe(true)

        vi.advanceTimersByTime(600)
        manager.checkIdle()

        expect(callbacks.has('db1')).toBe(false)
        expect(manager.trackedCount).toBe(0)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not close databases that are still active', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(500)

        manager.checkIdle()

        // Only 500ms elapsed, which is under the 1000ms timeout.
        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('handles idleTimeout of 0 (disabled)', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
          idleTimeout: 0,
        },
        callbacks,
      )

      manager.resolve('db1')
      manager.checkIdle()

      // Database should still be open.
      expect(callbacks.has('db1')).toBe(true)

      manager.dispose()
      callbacks.close('db1')
    })

    it('cleans up tracking for externally closed databases', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
          idleTimeout: 10_000,
        },
        callbacks,
      )

      manager.resolve('db1')
      expect(manager.trackedCount).toBe(1)

      // Close the database externally (not through the manager).
      callbacks.close('db1')
      manager.checkIdle()

      // The stale tracking entry should be cleaned up.
      expect(manager.trackedCount).toBe(0)

      manager.dispose()
    })

    it('swallows close errors during idle cleanup', () => {
      vi.useFakeTimers()
      try {
        const closeFn = vi.fn(() => {
          throw new Error('close failed')
        })
        const callbacks: LifecycleCallbacks = {
          open: (id, path) => {
            const db = new Database(id, path)
            return db
          },
          close: closeFn,
          count: () => 1,
          has: () => true,
        }

        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 100,
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(200)

        // Should not throw despite close() throwing.
        expect(() => manager.checkIdle()).not.toThrow()
        expect(closeFn).toHaveBeenCalledWith('db1')

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('closes multiple idle databases in a single pass', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 500,
          },
          callbacks,
        )

        manager.resolve('db1')
        manager.resolve('db2')
        manager.resolve('db3')

        vi.advanceTimersByTime(600)
        manager.checkIdle()

        expect(callbacks.count()).toBe(0)
        expect(manager.trackedCount).toBe(0)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('only closes idle databases while keeping active ones', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(500)
        manager.resolve('db2')
        vi.advanceTimersByTime(600)

        // db1 has been idle for 1100ms (past timeout).
        // db2 has been idle for 600ms (under timeout).
        manager.checkIdle()

        expect(callbacks.has('db1')).toBe(false)
        expect(callbacks.has('db2')).toBe(true)

        manager.dispose()
        callbacks.close('db2')
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('evict', () => {
    it('closes the least recently used database', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(100)
        manager.resolve('db2')
        vi.advanceTimersByTime(100)
        manager.resolve('db3')

        manager.evict()

        // db1 was accessed earliest, so it should be evicted.
        expect(callbacks.has('db1')).toBe(false)
        expect(callbacks.has('db2')).toBe(true)
        expect(callbacks.has('db3')).toBe(true)

        manager.dispose()
        callbacks.close('db2')
        callbacks.close('db3')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does nothing when no databases are tracked', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      // Should not throw.
      expect(() => manager.evict()).not.toThrow()

      manager.dispose()
    })

    it('cleans up stale entries before evicting', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(100)
        manager.resolve('db2')

        // Close db1 externally.
        callbacks.close('db1')

        manager.evict()

        // db1 was stale and cleaned up. db2 is the only tracked one
        // and should be evicted.
        expect(callbacks.has('db2')).toBe(false)
        expect(manager.trackedCount).toBe(0)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('evicts after markActive reorders access', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(100)
        manager.resolve('db2')
        vi.advanceTimersByTime(100)

        // Re-access db1, making db2 the LRU.
        manager.markActive('db1')

        manager.evict()

        expect(callbacks.has('db1')).toBe(true)
        expect(callbacks.has('db2')).toBe(false)

        manager.dispose()
        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('idle timer', () => {
    it('starts a timer and periodically closes idle databases', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
          },
          callbacks,
        )

        manager.resolve('db1')
        expect(callbacks.has('db1')).toBe(true)

        // The timer checks at idleTimeout/2 = 500ms intervals.
        // Advance past the timeout; the next tick should close db1.
        vi.advanceTimersByTime(1500)

        expect(callbacks.has('db1')).toBe(false)

        manager.dispose()
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is 0', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 0,
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(100_000)

        // No timer running; database should still be open.
        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is negative', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: -500,
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(100_000)

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('does not start a timer when idleTimeout is undefined', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
          },
          callbacks,
        )

        manager.resolve('db1')
        vi.advanceTimersByTime(100_000)

        expect(callbacks.has('db1')).toBe(true)

        manager.dispose()
        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })

    it('stops the timer on dispose', () => {
      vi.useFakeTimers()
      try {
        const { callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 500,
          },
          callbacks,
        )

        manager.resolve('db1')
        manager.dispose()

        // Even after advancing well past the idle timeout, the
        // database stays open because the timer was stopped.
        vi.advanceTimersByTime(10_000)
        // The database was opened through callbacks, still exists
        // in the registry (manager.dispose doesn't close databases).
        expect(callbacks.has('db1')).toBe(true)

        callbacks.close('db1')
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('untrack', () => {
    it('removes a database from idle tracking', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
        },
        callbacks,
      )

      manager.resolve('db1')
      expect(manager.trackedCount).toBe(1)

      manager.untrack('db1')
      expect(manager.trackedCount).toBe(0)

      manager.dispose()
      callbacks.close('db1')
    })

    it('is safe to call for untracked IDs', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager({}, callbacks)

      expect(() => manager.untrack('nonexistent')).not.toThrow()
      manager.dispose()
    })
  })

  describe('dispose', () => {
    it('clears timers and tracking state', () => {
      const { callbacks } = createRegistry()
      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
          idleTimeout: 1000,
        },
        callbacks,
      )

      manager.resolve('db1')
      expect(manager.trackedCount).toBe(1)

      manager.dispose()

      expect(manager.disposed).toBe(true)
      expect(manager.trackedCount).toBe(0)

      callbacks.close('db1')
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
    it('manual open and close does not interfere with lifecycle tracking', () => {
      vi.useFakeTimers()
      try {
        const { dbs, callbacks } = createRegistry()
        const manager = new LifecycleManager(
          {
            autoOpen: {
              resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
            },
            idleTimeout: 1000,
            maxOpen: 5,
          },
          callbacks,
        )

        // Manually open a database (not through lifecycle).
        callbacks.open('manual', join(tempDir, 'manual.db'))

        // Auto-open via lifecycle.
        manager.resolve('auto1')

        expect(dbs.size).toBe(2)

        vi.advanceTimersByTime(1500)

        // Only auto1 should be closed by idle timeout.
        // manual is not tracked and stays open.
        expect(callbacks.has('manual')).toBe(true)
        expect(callbacks.has('auto1')).toBe(false)

        manager.dispose()
        callbacks.close('manual')
      } finally {
        vi.useRealTimers()
      }
    })

    it('maxOpen counts both manual and lifecycle databases', () => {
      const { callbacks } = createRegistry()

      // Manually open 2 databases.
      callbacks.open('manual1', join(tempDir, 'manual1.db'))
      callbacks.open('manual2', join(tempDir, 'manual2.db'))

      const manager = new LifecycleManager(
        {
          autoOpen: {
            resolver: (id) => ({ path: join(tempDir, `${id}.db`) }),
          },
          maxOpen: 3,
        },
        callbacks,
      )

      // This should work (3rd database, at capacity).
      const db = manager.resolve('auto1')
      expect(db).toBeDefined()

      // This should fail. auto1 is the only tracked one but
      // after evicting it, we'd go from 3 to 2, then open
      // would bring it back to 3.
      manager.resolve('auto2')
      expect(callbacks.has('auto1')).toBe(false)
      expect(callbacks.has('auto2')).toBe(true)

      manager.dispose()
      callbacks.close('manual1')
      callbacks.close('manual2')
      callbacks.close('auto2')
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
      expect(result).toBe(join('/data/tenants', 'acme.db'))
    })

    it('uses a custom extension', () => {
      const result = tenantPath('/data', 'acme', '.sqlite')
      expect(result).toBe(join('/data', 'acme.sqlite'))
    })

    it('throws on invalid tenant ID', () => {
      expect(() => tenantPath('/data', '../escape')).toThrow('Invalid tenant ID')
      expect(() => tenantPath('/data', '')).toThrow('Invalid tenant ID')
      expect(() => tenantPath('/data', '-bad')).toThrow('Invalid tenant ID')
    })

    it('throws when filename (id + extension) exceeds 255 characters', () => {
      // 253-char ID + '.db' (3 chars) = 256 chars, exceeds the 255 limit.
      const longId = 'a'.repeat(253)
      expect(() => tenantPath('/data', longId)).toThrow('maximum length')

      // 252-char ID + '.db' = 255, exactly at the limit.
      const okId = 'a'.repeat(252)
      expect(() => tenantPath('/data', okId)).not.toThrow()
    })

    it('throws when a long extension pushes filename past the limit', () => {
      // 250-char ID + '.sqlite3' (8 chars) = 258, exceeds 255.
      const id = 'a'.repeat(250)
      expect(() => tenantPath('/data', id, '.sqlite3')).toThrow('maximum length')
    })
  })

  describe('createTenantResolver', () => {
    it('creates a resolver that maps IDs to paths', () => {
      const resolver = createTenantResolver({ basePath: '/data/dbs' })

      const result = resolver('tenant1')
      expect(result).toBeDefined()
      expect(result!.path).toBe(join('/data/dbs', 'tenant1.db'))
    })

    it('uses custom extension', () => {
      const resolver = createTenantResolver({
        basePath: '/data',
        extension: '.sqlite3',
      })

      const result = resolver('mydb')
      expect(result!.path).toBe(join('/data', 'mydb.sqlite3'))
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
      expect(result!.options).toEqual({ readOnly: true, readPoolSize: 2 })
    })

    it('returns undefined options when none are configured', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      const result = resolver('tenant1')
      expect(result!.options).toBeUndefined()
    })

    it('returns undefined when filename exceeds 255 characters', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      // 253-char ID + '.db' (3 chars) = 256, exceeds the 255-char limit.
      const longId = 'a'.repeat(253)
      expect(resolver(longId)).toBeUndefined()
    })

    it('accepts IDs where filename is exactly 255 characters', () => {
      const resolver = createTenantResolver({ basePath: '/data' })
      // 252-char ID + '.db' = 255, exactly at the limit.
      const okId = 'a'.repeat(252)
      expect(resolver(okId)).toBeDefined()
    })

    it('works with LifecycleManager for end-to-end tenant resolution', () => {
      const { dbs, callbacks } = createRegistry()
      const resolver = createTenantResolver({ basePath: tempDir })

      const manager = new LifecycleManager(
        { autoOpen: { resolver } },
        callbacks,
      )

      const db = manager.resolve('acme')
      expect(db).toBeDefined()
      expect(db!.id).toBe('acme')
      expect(db!.path).toBe(join(tempDir, 'acme.db'))
      expect(dbs.has('acme')).toBe(true)

      manager.dispose()
      db!.close()
    })
  })
})
