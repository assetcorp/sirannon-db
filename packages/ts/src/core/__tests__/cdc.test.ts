import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChangeTracker } from '../cdc/change-tracker.js'
import { SubscriptionBuilderImpl, SubscriptionManager, startPolling } from '../cdc/subscription.js'
import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import type { ChangeEvent } from '../types.js'
import { testDriver } from './helpers/test-driver.js'

async function createTestDb(): Promise<SQLiteConnection> {
  const conn = await testDriver.open(':memory:')
  await conn.exec('PRAGMA journal_mode = WAL')
  await conn.exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT,
			age INTEGER
		)
	`)
  return conn
}

async function insertUser(
  conn: SQLiteConnection,
  name: string,
  email: string | null = null,
  age: number | null = null,
): Promise<number> {
  const stmt = await conn.prepare('INSERT INTO users (name, email, age) VALUES (?, ?, ?)')
  const result = await stmt.run(name, email, age)
  return Number(result.lastInsertRowId)
}

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

  describe('null and special values', () => {
    it('handles null column values in JSON snapshots', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice', null, null)

      const events = await tracker.poll(conn)
      expect(events[0].row).toEqual({
        id: 1,
        name: 'Alice',
        email: null,
        age: null,
      })
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

describe('SubscriptionManager', () => {
  it('dispatches events to matching subscribers', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    manager.subscribe('users', undefined, event => {
      received.push(event)
    })

    const event: ChangeEvent = {
      type: 'insert',
      table: 'users',
      row: { id: 1, name: 'Alice' },
      seq: 1n,
      timestamp: Date.now() / 1000,
    }

    manager.dispatch([event])
    expect(received).toHaveLength(1)
    expect(received[0]).toBe(event)
  })

  it('does not dispatch events for non-matching tables', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    manager.subscribe('posts', undefined, event => {
      received.push(event)
    })

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1 },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received).toHaveLength(0)
  })

  it('supports multiple subscribers on the same table', () => {
    const manager = new SubscriptionManager()
    const received1: ChangeEvent[] = []
    const received2: ChangeEvent[] = []

    manager.subscribe('users', undefined, e => received1.push(e))
    manager.subscribe('users', undefined, e => received2.push(e))

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1 },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received1).toHaveLength(1)
    expect(received2).toHaveLength(1)
  })

  it('unsubscribe removes the subscriber', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    const sub = manager.subscribe('users', undefined, e => received.push(e))
    sub.unsubscribe()

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1 },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received).toHaveLength(0)
  })

  it('unsubscribe is safe to call multiple times', () => {
    const manager = new SubscriptionManager()
    const sub = manager.subscribe('users', undefined, () => {})

    sub.unsubscribe()
    expect(() => sub.unsubscribe()).not.toThrow()
  })

  it('tracks subscriber count per table', () => {
    const manager = new SubscriptionManager()
    const sub1 = manager.subscribe('users', undefined, () => {})
    manager.subscribe('users', undefined, () => {})
    manager.subscribe('posts', undefined, () => {})

    expect(manager.subscriberCount('users')).toBe(2)
    expect(manager.subscriberCount('posts')).toBe(1)
    expect(manager.size).toBe(3)

    sub1.unsubscribe()
    expect(manager.subscriberCount('users')).toBe(1)
    expect(manager.size).toBe(2)
  })

  it('returns zero subscriber count for unknown tables', () => {
    const manager = new SubscriptionManager()
    expect(manager.subscriberCount('missing')).toBe(0)
  })

  it('isolates subscriber exceptions from other subscribers', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    manager.subscribe('users', undefined, () => {
      throw new Error('subscriber error')
    })
    manager.subscribe('users', undefined, e => received.push(e))

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1 },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received).toHaveLength(1)
  })

  describe('filter matching', () => {
    it('dispatches events that match the filter', () => {
      const manager = new SubscriptionManager()
      const received: ChangeEvent[] = []

      manager.subscribe('users', { name: 'Alice' }, e => received.push(e))

      manager.dispatch([
        {
          type: 'insert',
          table: 'users',
          row: { id: 1, name: 'Alice' },
          seq: 1n,
          timestamp: Date.now() / 1000,
        },
      ])

      expect(received).toHaveLength(1)
    })

    it('excludes events that do not match the filter', () => {
      const manager = new SubscriptionManager()
      const received: ChangeEvent[] = []

      manager.subscribe('users', { name: 'Alice' }, e => received.push(e))

      manager.dispatch([
        {
          type: 'insert',
          table: 'users',
          row: { id: 2, name: 'Bob' },
          seq: 1n,
          timestamp: Date.now() / 1000,
        },
      ])

      expect(received).toHaveLength(0)
    })

    it('matches against multiple filter fields (AND logic)', () => {
      const manager = new SubscriptionManager()
      const received: ChangeEvent[] = []

      manager.subscribe('users', { name: 'Alice', age: 30 }, e => received.push(e))

      manager.dispatch([
        {
          type: 'insert',
          table: 'users',
          row: { id: 1, name: 'Alice', age: 30 },
          seq: 1n,
          timestamp: Date.now() / 1000,
        },
      ])
      expect(received).toHaveLength(1)

      manager.dispatch([
        {
          type: 'insert',
          table: 'users',
          row: { id: 2, name: 'Alice', age: 25 },
          seq: 2n,
          timestamp: Date.now() / 1000,
        },
      ])
      expect(received).toHaveLength(1)
    })

    it('matches delete events against oldRow', () => {
      const manager = new SubscriptionManager()
      const received: ChangeEvent[] = []

      manager.subscribe('users', { name: 'Alice' }, e => received.push(e))

      manager.dispatch([
        {
          type: 'delete',
          table: 'users',
          row: {},
          oldRow: { id: 1, name: 'Alice' },
          seq: 1n,
          timestamp: Date.now() / 1000,
        },
      ])

      expect(received).toHaveLength(1)
    })

    it('treats delete events without oldRow as non-matching for filtered subscriptions', () => {
      const manager = new SubscriptionManager()
      const received: ChangeEvent[] = []

      manager.subscribe('users', { name: 'Alice' }, e => received.push(e))

      manager.dispatch([
        {
          type: 'delete',
          table: 'users',
          row: {},
          seq: 1n,
          timestamp: Date.now() / 1000,
        },
      ])

      expect(received).toHaveLength(0)
    })
  })
})

describe('SubscriptionBuilderImpl', () => {
  it('creates a subscription without a filter', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    const builder = new SubscriptionBuilderImpl('users', manager)
    builder.subscribe(e => received.push(e))

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1 },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received).toHaveLength(1)
  })

  it('chains filter and subscribe', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    new SubscriptionBuilderImpl('users', manager).filter({ name: 'Alice' }).subscribe(e => received.push(e))

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1, name: 'Bob' },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received).toHaveLength(0)

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 2, name: 'Alice' },
        seq: 2n,
        timestamp: Date.now() / 1000,
      },
    ])

    expect(received).toHaveLength(1)
  })

  it('merges multiple filter calls', () => {
    const manager = new SubscriptionManager()
    const received: ChangeEvent[] = []

    new SubscriptionBuilderImpl('users', manager)
      .filter({ name: 'Alice' })
      .filter({ age: 30 })
      .subscribe(e => received.push(e))

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 1, name: 'Alice', age: 25 },
        seq: 1n,
        timestamp: Date.now() / 1000,
      },
    ])
    expect(received).toHaveLength(0)

    manager.dispatch([
      {
        type: 'insert',
        table: 'users',
        row: { id: 2, name: 'Alice', age: 30 },
        seq: 2n,
        timestamp: Date.now() / 1000,
      },
    ])
    expect(received).toHaveLength(1)
  })
})

describe('startPolling', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker
  let manager: SubscriptionManager

  beforeEach(async () => {
    vi.useFakeTimers()
    conn = await createTestDb()
    tracker = new ChangeTracker()
    manager = new SubscriptionManager()
    await tracker.watch(conn, 'users')
  })

  afterEach(async () => {
    await conn.close()
    vi.useRealTimers()
  })

  it('dispatches events on each polling interval', async () => {
    const received: ChangeEvent[] = []
    manager.subscribe('users', undefined, e => received.push(e))

    const stop = startPolling(conn, tracker, manager, 20)

    await insertUser(conn, 'Alice')
    await vi.advanceTimersByTimeAsync(20)

    stop()
    expect(received).toHaveLength(1)
    expect(received[0].row.name).toBe('Alice')
  })

  it('stops polling when the stop function is called', async () => {
    const received: ChangeEvent[] = []
    manager.subscribe('users', undefined, e => received.push(e))

    const stop = startPolling(conn, tracker, manager, 20)
    stop()

    await insertUser(conn, 'Alice')
    await vi.advanceTimersByTimeAsync(60)

    expect(received).toHaveLength(0)
  })

  it('skips polling when there are zero subscribers', async () => {
    const stop = startPolling(conn, tracker, manager, 20)

    await insertUser(conn, 'Alice')
    await vi.advanceTimersByTimeAsync(20)

    const events = await tracker.poll(conn)
    expect(events).toHaveLength(1)
    expect(events[0].row.name).toBe('Alice')

    stop()
  })

  it('continues polling after transient errors and calls onError', async () => {
    const received: ChangeEvent[] = []
    const errors: Error[] = []
    manager.subscribe('users', undefined, e => received.push(e))

    const originalPoll = tracker.poll.bind(tracker)
    let callCount = 0
    tracker.poll = async (pollConn: SQLiteConnection) => {
      callCount++
      if (callCount <= 3) throw new Error(`transient error ${callCount}`)
      return originalPoll(pollConn)
    }

    const stop = startPolling(conn, tracker, manager, 20, err => errors.push(err))

    await vi.advanceTimersByTimeAsync(20)
    await vi.advanceTimersByTimeAsync(20)
    await vi.advanceTimersByTimeAsync(20)
    expect(errors).toHaveLength(3)

    await insertUser(conn, 'Alice')
    await vi.advanceTimersByTimeAsync(20)

    expect(received).toHaveLength(1)
    expect(received[0].row.name).toBe('Alice')

    stop()
  })

  it('stops polling after 10 consecutive errors', async () => {
    const errors: Error[] = []
    manager.subscribe('users', undefined, () => {})

    tracker.poll = async () => {
      throw new Error('persistent failure')
    }

    const stop = startPolling(conn, tracker, manager, 20, err => errors.push(err))

    for (let i = 0; i < 15; i++) {
      await vi.advanceTimersByTimeAsync(20)
    }

    expect(errors).toHaveLength(10)
    stop()
  })

  it('does not call onError when no error callback is provided', async () => {
    manager.subscribe('users', undefined, () => {})
    tracker.poll = async () => {
      throw new Error('poll failed')
    }

    const stop = startPolling(conn, tracker, manager, 20)
    await vi.advanceTimersByTimeAsync(40)
    stop()
  })

  it('wraps non-Error poll failures before passing to onError', async () => {
    manager.subscribe('users', undefined, () => {})
    const errors: Error[] = []
    tracker.poll = async () => {
      throw 'poll failed as string'
    }

    const stop = startPolling(conn, tracker, manager, 20, err => errors.push(err))
    await vi.advanceTimersByTimeAsync(20)
    stop()

    expect(errors).toHaveLength(1)
    expect(errors[0]).toBeInstanceOf(Error)
    expect(errors[0].message).toContain('poll failed as string')
  })

  it('runs tracker cleanup after each 100 successful ticks', async () => {
    manager.subscribe('users', undefined, () => {})
    const cleanup = vi.fn(async () => 0)
    tracker.cleanup = cleanup

    const stop = startPolling(conn, tracker, manager, 10)
    await vi.advanceTimersByTimeAsync(1000)
    stop()

    expect(cleanup).toHaveBeenCalledTimes(1)
  })
})

describe('CDC integration', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker
  let manager: SubscriptionManager

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker()
    manager = new SubscriptionManager()
  })

  afterEach(async () => {
    await conn.close()
  })

  it('end-to-end: watch -> insert -> poll -> dispatch -> callback', async () => {
    const received: ChangeEvent[] = []

    await tracker.watch(conn, 'users')
    manager.subscribe('users', undefined, e => received.push(e))

    await insertUser(conn, 'Alice', 'alice@example.com', 30)
    const events = await tracker.poll(conn)
    manager.dispatch(events)

    expect(received).toHaveLength(1)
    expect(received[0].type).toBe('insert')
    expect(received[0].row).toEqual({
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
    })
  })

  it('end-to-end: insert -> update -> delete cycle', async () => {
    const received: ChangeEvent[] = []

    await tracker.watch(conn, 'users')
    manager.subscribe('users', undefined, e => received.push(e))

    await insertUser(conn, 'Alice', 'alice@example.com', 30)
    manager.dispatch(await tracker.poll(conn))

    const updateStmt = await conn.prepare('UPDATE users SET age = ? WHERE name = ?')
    await updateStmt.run(31, 'Alice')
    manager.dispatch(await tracker.poll(conn))

    const deleteStmt = await conn.prepare('DELETE FROM users WHERE name = ?')
    await deleteStmt.run('Alice')
    manager.dispatch(await tracker.poll(conn))

    expect(received).toHaveLength(3)
    expect(received[0].type).toBe('insert')
    expect(received[1].type).toBe('update')
    expect(received[1].oldRow?.age).toBe(30)
    expect(received[1].row.age).toBe(31)
    expect(received[2].type).toBe('delete')
    expect(received[2].oldRow?.name).toBe('Alice')
  })

  it('filtered subscription receives only matching events', async () => {
    const aliceEvents: ChangeEvent[] = []
    const allEvents: ChangeEvent[] = []

    await tracker.watch(conn, 'users')
    manager.subscribe('users', { name: 'Alice' }, e => aliceEvents.push(e))
    manager.subscribe('users', undefined, e => allEvents.push(e))

    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Alice')

    manager.dispatch(await tracker.poll(conn))

    expect(allEvents).toHaveLength(3)
    expect(aliceEvents).toHaveLength(2)
  })

  it('unwatch stops capturing new changes', async () => {
    await tracker.watch(conn, 'users')
    await insertUser(conn, 'Alice')
    await tracker.poll(conn)

    await tracker.unwatch(conn, 'users')
    await insertUser(conn, 'Bob')

    const events = await tracker.poll(conn)
    expect(events).toHaveLength(0)
  })
})
