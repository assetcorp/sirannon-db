import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import type { SQLiteConnection } from '../../driver/types.js'
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

  describe('cleanup', () => {
    it('deletes entries older than the retention period', async () => {
      const tracker = new ChangeTracker({
        retention: 1000,
      })
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice')

      const twoSecondsAgo = Date.now() / 1000 - 2
      const updateStmt = await conn.prepare('UPDATE _sirannon_changes SET changed_at = ?')
      await updateStmt.run(twoSecondsAgo)

      const deleted = await tracker.cleanup(conn)
      expect(deleted).toBe(1)

      const countStmt = await conn.prepare('SELECT COUNT(*) as count FROM _sirannon_changes')
      const remaining = (await countStmt.get()) as { count: number }
      expect(remaining.count).toBe(0)
    })

    it('preserves entries within the retention window', async () => {
      const tracker = new ChangeTracker({
        retention: 60_000,
      })
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice')

      const deleted = await tracker.cleanup(conn)
      expect(deleted).toBe(0)
    })

    it('returns 0 when no changes table exists', async () => {
      const deleted = await tracker.cleanup(conn)
      expect(deleted).toBe(0)
    })

    it('detects and cleans up a pre-existing changes table', async () => {
      await conn.exec(`
        CREATE TABLE _sirannon_changes (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          table_name TEXT NOT NULL,
          operation TEXT NOT NULL,
          row_id TEXT NOT NULL,
          changed_at REAL NOT NULL,
          old_data TEXT,
          new_data TEXT
        )
      `)

      const staleTime = Date.now() / 1000 - 10
      const stmt = await conn.prepare(
        "INSERT INTO _sirannon_changes (table_name, operation, row_id, changed_at, old_data, new_data) VALUES ('users', 'INSERT', '1', ?, NULL, '{\"id\":1}')",
      )
      await stmt.run(staleTime)

      const shortRetention = new ChangeTracker({ retention: 1000 })
      const deleted = await shortRetention.cleanup(conn)
      expect(deleted).toBe(1)
    })

    it('does not delete un-polled rows when poll has been used', async () => {
      const tracker = new ChangeTracker({
        retention: 1000,
      })
      await tracker.watch(conn, 'users')

      await insertUser(conn, 'Alice')
      await tracker.poll(conn)

      await insertUser(conn, 'Bob')

      const twoSecondsAgo = Date.now() / 1000 - 2
      const updateStmt = await conn.prepare('UPDATE _sirannon_changes SET changed_at = ?')
      await updateStmt.run(twoSecondsAgo)

      const deleted = await tracker.cleanup(conn)
      expect(deleted).toBe(1)

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].row.name).toBe('Bob')
    })
  })
})
