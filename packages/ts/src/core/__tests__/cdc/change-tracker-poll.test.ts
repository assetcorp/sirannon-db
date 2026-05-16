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

  describe('poll - INSERT events', () => {
    it('captures an INSERT with full row data as JSON', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice', 'alice@example.com', 30)

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      expect(events[0].table).toBe('users')
      expect(events[0].row).toEqual({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })
      expect(events[0].oldRow).toBeUndefined()
      expect(events[0].seq).toBe(1n)
      expect(typeof events[0].timestamp).toBe('number')
    })

    it('captures multiple inserts in seq order', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice')
      await insertUser(conn, 'Bob')
      await insertUser(conn, 'Charlie')

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(3)
      expect(events[0].seq).toBe(1n)
      expect(events[1].seq).toBe(2n)
      expect(events[2].seq).toBe(3n)
      expect(events[0].row.name).toBe('Alice')
      expect(events[1].row.name).toBe('Bob')
      expect(events[2].row.name).toBe('Charlie')
    })
  })

  describe('poll - UPDATE events', () => {
    it('captures an UPDATE with both old and new row data', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice', 'alice@example.com', 30)
      await tracker.poll(conn)

      const stmt = await conn.prepare('UPDATE users SET age = ? WHERE name = ?')
      await stmt.run(31, 'Alice')

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('update')
      expect(events[0].row).toEqual({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 31,
      })
      expect(events[0].oldRow).toEqual({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })
    })
  })

  describe('poll - DELETE events', () => {
    it('captures a DELETE with old row data', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice', 'alice@example.com', 30)
      await tracker.poll(conn)

      const stmt = await conn.prepare('DELETE FROM users WHERE name = ?')
      await stmt.run('Alice')

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('delete')
      expect(events[0].oldRow).toEqual({
        id: 1,
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
      })
      expect(events[0].row).toEqual({})
    })
  })

  describe('poll - seq cursor advancement', () => {
    it('only returns new events on subsequent polls', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice')

      const first = await tracker.poll(conn)
      expect(first).toHaveLength(1)

      const second = await tracker.poll(conn)
      expect(second).toHaveLength(0)

      await insertUser(conn, 'Bob')
      const third = await tracker.poll(conn)
      expect(third).toHaveLength(1)
      expect(third[0].row.name).toBe('Bob')
    })
  })

  describe('poll - returns empty before watching', () => {
    it('returns empty array when no changes table exists', async () => {
      const events = await tracker.poll(conn)
      expect(events).toEqual([])
    })
  })

  describe('poll - batch size', () => {
    it('limits the number of events per poll to pollBatchSize', async () => {
      const tracker = new ChangeTracker({
        pollBatchSize: 2,
      })
      await tracker.watch(conn, 'users')

      await insertUser(conn, 'Alice')
      await insertUser(conn, 'Bob')
      await insertUser(conn, 'Charlie')
      await insertUser(conn, 'Dave')

      const first = await tracker.poll(conn)
      expect(first).toHaveLength(2)
      expect(first[0].row.name).toBe('Alice')
      expect(first[1].row.name).toBe('Bob')

      const second = await tracker.poll(conn)
      expect(second).toHaveLength(2)
      expect(second[0].row.name).toBe('Charlie')
      expect(second[1].row.name).toBe('Dave')

      const third = await tracker.poll(conn)
      expect(third).toHaveLength(0)
    })
  })

  describe('second instance detection', () => {
    it('a second tracker can poll the same changes table', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice')

      const tracker2 = new ChangeTracker()
      const events = await tracker2.poll(conn)
      expect(events).toHaveLength(1)
      expect(events[0].row.name).toBe('Alice')
    })
  })

  describe('multiple tables', () => {
    it('tracks changes across multiple watched tables', async () => {
      await conn.exec(`
				CREATE TABLE posts (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					title TEXT NOT NULL,
					user_id INTEGER
				)
			`)

      await tracker.watch(conn, 'users')
      await tracker.watch(conn, 'posts')

      await insertUser(conn, 'Alice')
      const stmt = await conn.prepare('INSERT INTO posts (title, user_id) VALUES (?, ?)')
      await stmt.run('Hello', 1)

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(2)
      expect(events[0].table).toBe('users')
      expect(events[1].table).toBe('posts')
    })
  })
})
