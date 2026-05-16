import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { createTestDb } from './_helpers.js'

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

  describe('WITHOUT ROWID tables', () => {
    it('tracks changes on WITHOUT ROWID tables using PK columns', async () => {
      await conn.exec(`
				CREATE TABLE kv (
					key TEXT PRIMARY KEY,
					value TEXT
				) WITHOUT ROWID
			`)

      await tracker.watch(conn, 'kv')

      const stmt = await conn.prepare('INSERT INTO kv (key, value) VALUES (?, ?)')
      await stmt.run('greeting', 'hello')

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      expect(events[0].row).toEqual({
        key: 'greeting',
        value: 'hello',
      })
    })

    it('tracks changes on tables with composite primary keys', async () => {
      await conn.exec(`
				CREATE TABLE user_roles (
					user_id INTEGER,
					role_id INTEGER,
					granted_at TEXT,
					PRIMARY KEY (user_id, role_id)
				) WITHOUT ROWID
			`)

      await tracker.watch(conn, 'user_roles')

      const stmt = await conn.prepare('INSERT INTO user_roles (user_id, role_id, granted_at) VALUES (?, ?, ?)')
      await stmt.run(1, 10, '2025-01-01')

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].row).toEqual({
        user_id: 1,
        role_id: 10,
        granted_at: '2025-01-01',
      })
    })

    it('captures updates and deletes on WITHOUT ROWID tables', async () => {
      await conn.exec(`
				CREATE TABLE kv (
					key TEXT PRIMARY KEY,
					value TEXT
				) WITHOUT ROWID
			`)

      await tracker.watch(conn, 'kv')

      const insertStmt = await conn.prepare('INSERT INTO kv (key, value) VALUES (?, ?)')
      await insertStmt.run('a', 'one')
      await tracker.poll(conn)

      const updateStmt = await conn.prepare('UPDATE kv SET value = ? WHERE key = ?')
      await updateStmt.run('two', 'a')
      const updateEvents = await tracker.poll(conn)
      expect(updateEvents).toHaveLength(1)
      expect(updateEvents[0].type).toBe('update')
      expect(updateEvents[0].oldRow).toEqual({
        key: 'a',
        value: 'one',
      })
      expect(updateEvents[0].row).toEqual({
        key: 'a',
        value: 'two',
      })

      const deleteStmt = await conn.prepare('DELETE FROM kv WHERE key = ?')
      await deleteStmt.run('a')
      const deleteEvents = await tracker.poll(conn)
      expect(deleteEvents).toHaveLength(1)
      expect(deleteEvents[0].type).toBe('delete')
      expect(deleteEvents[0].oldRow).toEqual({
        key: 'a',
        value: 'two',
      })
    })

    it('tracks changes for tables without explicit primary keys', async () => {
      await conn.exec('CREATE TABLE logs (message TEXT NOT NULL)')
      await tracker.watch(conn, 'logs')

      const stmt = await conn.prepare('INSERT INTO logs (message) VALUES (?)')
      await stmt.run('hello')

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      expect(events[0].row).toEqual({ message: 'hello' })
    })
  })
})
