import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { CDCError } from '../../errors.js'
import { createTestDb, insertUser } from './_helpers.js'

describe('ChangeTracker', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker()
  })

  afterEach(async () => {
    await conn.close()
  })

  describe('watch', () => {
    it('creates the _sirannon_changes table', async () => {
      await tracker.watch(conn, 'users')

      const stmt = await conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='_sirannon_changes'")
      const tables = await stmt.all()
      expect(tables).toHaveLength(1)
    })

    it('creates an index on changed_at', async () => {
      await tracker.watch(conn, 'users')

      const stmt = await conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx__sirannon_changes_changed_at'",
      )
      const indexes = await stmt.all()
      expect(indexes).toHaveLength(1)
    })

    it('installs INSERT, UPDATE, and DELETE triggers', async () => {
      await tracker.watch(conn, 'users')

      const stmt = await conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
      )
      const triggers = (await stmt.all()) as { name: string }[]

      const names = triggers.map(t => t.name).sort()
      expect(names).toEqual(['_sirannon_trg_users_delete', '_sirannon_trg_users_insert', '_sirannon_trg_users_update'])
    })

    it('tracks the table in watchedTables', async () => {
      await tracker.watch(conn, 'users')
      expect(tracker.watchedTables.has('users')).toBe(true)
    })

    it('is idempotent when watching the same table twice', async () => {
      await tracker.watch(conn, 'users')
      await tracker.watch(conn, 'users')
      expect(tracker.watchedTables.size).toBe(1)
    })

    it('throws CDCError for a nonexistent table', async () => {
      await expect(tracker.watch(conn, 'nonexistent')).rejects.toThrow(CDCError)
      await expect(tracker.watch(conn, 'nonexistent')).rejects.toThrow(
        "Table 'nonexistent' does not exist or has no columns",
      )
    })
  })

  describe('input validation', () => {
    it('rejects table names with SQL injection characters', async () => {
      await expect(tracker.watch(conn, 'users; DROP TABLE users')).rejects.toThrow(CDCError)
      await expect(tracker.watch(conn, 'users; DROP TABLE users')).rejects.toThrow('Invalid table name')
    })

    it('rejects table names with special characters', async () => {
      await expect(tracker.watch(conn, 'my-table')).rejects.toThrow(CDCError)
      await expect(tracker.watch(conn, 'table name')).rejects.toThrow(CDCError)
      await expect(tracker.watch(conn, "users'--")).rejects.toThrow(CDCError)
    })

    it('rejects table names starting with a digit', async () => {
      await expect(tracker.watch(conn, '1users')).rejects.toThrow(CDCError)
    })

    it('allows valid identifier names', async () => {
      await conn.exec('CREATE TABLE my_table_123 (id INTEGER PRIMARY KEY)')
      await expect(tracker.watch(conn, 'my_table_123')).resolves.not.toThrow()
    })

    it('rejects invalid changesTable names at construction', () => {
      expect(
        () =>
          new ChangeTracker({
            changesTable: 'bad; name',
          }),
      ).toThrow(CDCError)
      expect(
        () =>
          new ChangeTracker({
            changesTable: 'bad; name',
          }),
      ).toThrow('Invalid changes table name')
    })
  })

  describe('schema evolution', () => {
    it('re-installs triggers when columns change after watch', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice', 'alice@example.com', 30)

      const events1 = await tracker.poll(conn)
      expect(events1).toHaveLength(1)
      expect(Object.keys(events1[0].row).sort()).toEqual(['age', 'email', 'id', 'name'])

      await conn.exec('ALTER TABLE users ADD COLUMN role TEXT')

      await tracker.watch(conn, 'users')

      const stmt = await conn.prepare('INSERT INTO users (name, email, age, role) VALUES (?, ?, ?, ?)')
      await stmt.run('Bob', 'bob@example.com', 25, 'admin')

      const events2 = await tracker.poll(conn)
      expect(events2).toHaveLength(1)
      expect(events2[0].row).toHaveProperty('role')
      expect(events2[0].row.role).toBe('admin')
    })
  })

  describe('unwatch', () => {
    it('removes triggers for the table', async () => {
      await tracker.watch(conn, 'users')
      await tracker.unwatch(conn, 'users')

      const stmt = await conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
      )
      const triggers = await stmt.all()
      expect(triggers).toHaveLength(0)
    })

    it('removes the table from watchedTables', async () => {
      await tracker.watch(conn, 'users')
      await tracker.unwatch(conn, 'users')
      expect(tracker.watchedTables.has('users')).toBe(false)
    })

    it('is safe to call for an unwatched table', async () => {
      await expect(tracker.unwatch(conn, 'users')).resolves.not.toThrow()
    })
  })

  describe('custom changes table name', () => {
    it('uses a custom table name when configured', async () => {
      const custom = new ChangeTracker({
        changesTable: 'my_changelog',
      })
      await custom.watch(conn, 'users')

      const stmt = await conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='my_changelog'")
      const tables = await stmt.all()
      expect(tables).toHaveLength(1)

      await insertUser(conn, 'Alice', 'alice@example.com', 30)

      const events = await custom.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      expect(events[0].row.name).toBe('Alice')
    })
  })

  describe('watchedTables caching', () => {
    it('returns the same Set reference when no changes', async () => {
      await tracker.watch(conn, 'users')
      const first = tracker.watchedTables
      const second = tracker.watchedTables
      expect(first).toBe(second)
    })

    it('returns a new Set reference after watch or unwatch', async () => {
      await tracker.watch(conn, 'users')
      const first = tracker.watchedTables

      await conn.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY)')
      await tracker.watch(conn, 'posts')
      const second = tracker.watchedTables

      expect(first).not.toBe(second)
      expect(second.size).toBe(2)
    })
  })
})
