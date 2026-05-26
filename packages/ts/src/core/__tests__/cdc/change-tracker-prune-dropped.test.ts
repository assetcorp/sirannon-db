import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { createTestDb } from './_helpers.js'

describe('ChangeTracker.pruneDroppedTables', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker()
  })

  afterEach(async () => {
    await conn.close()
  })

  describe('committed DROP TABLE', () => {
    it('removes the watched entry after a successful drop inside a transaction', async () => {
      await tracker.watch(conn, 'users')
      expect(tracker.watchedTables.has('users')).toBe(true)

      await conn.transaction(async tx => {
        await tx.exec('DROP TABLE users')
      })
      await tracker.pruneDroppedTables(conn, ['users'])

      expect(tracker.watchedTables.has('users')).toBe(false)
    })

    it('invalidates the watchedTables cache after pruning', async () => {
      await tracker.watch(conn, 'users')
      const before = tracker.watchedTables
      expect(before.has('users')).toBe(true)

      await conn.transaction(async tx => {
        await tx.exec('DROP TABLE users')
      })
      await tracker.pruneDroppedTables(conn, ['users'])

      const after = tracker.watchedTables
      expect(after).not.toBe(before)
      expect(after.has('users')).toBe(false)
    })

    it('drops residual triggers when a same-named table was recreated mid-transaction', async () => {
      await tracker.watch(conn, 'users')

      await conn.transaction(async tx => {
        await tx.exec('DROP TABLE users')
        await tx.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, label TEXT)')
        await tracker.refreshAllTriggersUsingConnection(tx)
      })

      const beforePrune = await conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
      )
      const triggersBefore = (await beforePrune.all()) as Array<{ name: string }>
      expect(triggersBefore.length).toBeGreaterThan(0)

      await tracker.pruneDroppedTables(conn, ['users'])

      const afterPrune = await conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
      )
      const triggersAfter = (await afterPrune.all()) as Array<{ name: string }>
      expect(triggersAfter).toEqual([])

      expect(tracker.watchedTables.has('users')).toBe(false)
    })
  })

  describe('rolled-back DROP TABLE', () => {
    it('leaves the watched entry intact when the caller never invokes prune', async () => {
      await tracker.watch(conn, 'users')

      const intentional = new Error('rollback for test')
      await expect(
        conn.transaction(async tx => {
          await tx.exec('DROP TABLE users')
          throw intentional
        }),
      ).rejects.toBe(intentional)

      expect(tracker.watchedTables.has('users')).toBe(true)

      const stmt = await conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
      )
      const triggers = (await stmt.all()) as Array<{ name: string }>
      expect(triggers).toHaveLength(3)

      const insertStmt = await conn.prepare('INSERT INTO users (name) VALUES (?)')
      await insertStmt.run('alice-after-rollback')
      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      const row = events[0].row as { name: string }
      expect(row.name).toBe('alice-after-rollback')
    })
  })

  describe('unwatched table', () => {
    it('silently ignores a name not in the watched map', async () => {
      await tracker.watch(conn, 'users')

      await tracker.pruneDroppedTables(conn, ['never_watched'])

      expect(tracker.watchedTables.has('users')).toBe(true)
      expect(tracker.watchedTables.size).toBe(1)
    })
  })

  describe('multiple drops in one call', () => {
    it('removes every supplied watched entry', async () => {
      await conn.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      await conn.exec('CREATE TABLE comments (id INTEGER PRIMARY KEY, body TEXT)')
      await tracker.watch(conn, 'users')
      await tracker.watch(conn, 'posts')
      await tracker.watch(conn, 'comments')

      await conn.transaction(async tx => {
        await tx.exec('DROP TABLE posts')
        await tx.exec('DROP TABLE comments')
      })
      await tracker.pruneDroppedTables(conn, ['posts', 'comments'])

      expect(tracker.watchedTables.has('users')).toBe(true)
      expect(tracker.watchedTables.has('posts')).toBe(false)
      expect(tracker.watchedTables.has('comments')).toBe(false)
    })
  })

  describe('idempotency', () => {
    it('produces the same state when called twice with the same list', async () => {
      await tracker.watch(conn, 'users')

      await conn.transaction(async tx => {
        await tx.exec('DROP TABLE users')
      })
      await tracker.pruneDroppedTables(conn, ['users'])
      const first = tracker.watchedTables
      const firstHas = first.has('users')

      await tracker.pruneDroppedTables(conn, ['users'])
      const second = tracker.watchedTables
      const secondHas = second.has('users')

      expect(firstHas).toBe(false)
      expect(secondHas).toBe(false)
      expect(first).toBe(second)
    })
  })

  describe('unwatch after prune', () => {
    it('treats unwatch on a pruned table as a no-op without throwing', async () => {
      await tracker.watch(conn, 'users')

      await conn.transaction(async tx => {
        await tx.exec('DROP TABLE users')
      })
      await tracker.pruneDroppedTables(conn, ['users'])

      await expect(tracker.unwatch(conn, 'users')).resolves.not.toThrow()
      expect(tracker.watchedTables.has('users')).toBe(false)
    })
  })
})
