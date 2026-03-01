import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../cdc/change-tracker.js'
import { SubscriptionBuilderImpl, SubscriptionManager, startPolling } from '../cdc/subscription.js'
import { CDCError } from '../errors.js'
import type { ChangeEvent } from '../types.js'

function createTestDb(): Database.Database {
  const db = new Database(':memory:')
  db.pragma('journal_mode = WAL')
  db.exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT,
			age INTEGER
		)
	`)
  return db
}

function insertUser(
  db: Database.Database,
  name: string,
  email: string | null = null,
  age: number | null = null,
): number {
  const result = db.prepare('INSERT INTO users (name, email, age) VALUES (?, ?, ?)').run(name, email, age)
  return Number(result.lastInsertRowid)
}

describe('ChangeTracker', () => {
  let db: Database.Database
  let tracker: ChangeTracker

  beforeEach(() => {
    db = createTestDb()
    tracker = new ChangeTracker()
  })

  afterEach(() => {
    db.close()
  })

  describe('watch', () => {
    it('creates the _sirannon_changes table', () => {
      tracker.watch(db, 'users')

      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='_sirannon_changes'").all()
      expect(tables).toHaveLength(1)
    })

    it('creates an index on changed_at', () => {
      tracker.watch(db, 'users')

      const indexes = db
        .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name='idx__sirannon_changes_changed_at'")
        .all()
      expect(indexes).toHaveLength(1)
    })

    it('installs INSERT, UPDATE, and DELETE triggers', () => {
      tracker.watch(db, 'users')

      const triggers = db
        .prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'")
        .all() as { name: string }[]

      const names = triggers.map(t => t.name).sort()
      expect(names).toEqual(['_sirannon_trg_users_delete', '_sirannon_trg_users_insert', '_sirannon_trg_users_update'])
    })

    it('tracks the table in watchedTables', () => {
      tracker.watch(db, 'users')
      expect(tracker.watchedTables.has('users')).toBe(true)
    })

    it('is idempotent when watching the same table twice', () => {
      tracker.watch(db, 'users')
      tracker.watch(db, 'users')
      expect(tracker.watchedTables.size).toBe(1)
    })

    it('throws CDCError for a nonexistent table', () => {
      expect(() => tracker.watch(db, 'nonexistent')).toThrow(CDCError)
      expect(() => tracker.watch(db, 'nonexistent')).toThrow("Table 'nonexistent' does not exist or has no columns")
    })
  })

  describe('input validation', () => {
    it('rejects table names with SQL injection characters', () => {
      expect(() => tracker.watch(db, 'users; DROP TABLE users')).toThrow(CDCError)
      expect(() => tracker.watch(db, 'users; DROP TABLE users')).toThrow('Invalid table name')
    })

    it('rejects table names with special characters', () => {
      expect(() => tracker.watch(db, 'my-table')).toThrow(CDCError)
      expect(() => tracker.watch(db, 'table name')).toThrow(CDCError)
      expect(() => tracker.watch(db, "users'--")).toThrow(CDCError)
    })

    it('rejects table names starting with a digit', () => {
      expect(() => tracker.watch(db, '1users')).toThrow(CDCError)
    })

    it('allows valid identifier names', () => {
      db.exec('CREATE TABLE my_table_123 (id INTEGER PRIMARY KEY)')
      expect(() => tracker.watch(db, 'my_table_123')).not.toThrow()
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
    it('re-installs triggers when columns change after watch', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice', 'alice@example.com', 30)

      const events1 = tracker.poll(db)
      expect(events1).toHaveLength(1)
      expect(Object.keys(events1[0].row).sort()).toEqual(['age', 'email', 'id', 'name'])

      db.exec('ALTER TABLE users ADD COLUMN role TEXT')

      tracker.watch(db, 'users')

      db.prepare('INSERT INTO users (name, email, age, role) VALUES (?, ?, ?, ?)').run(
        'Bob',
        'bob@example.com',
        25,
        'admin',
      )

      const events2 = tracker.poll(db)
      expect(events2).toHaveLength(1)
      expect(events2[0].row).toHaveProperty('role')
      expect(events2[0].row.role).toBe('admin')
    })
  })

  describe('unwatch', () => {
    it('removes triggers for the table', () => {
      tracker.watch(db, 'users')
      tracker.unwatch(db, 'users')

      const triggers = db
        .prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'")
        .all()
      expect(triggers).toHaveLength(0)
    })

    it('removes the table from watchedTables', () => {
      tracker.watch(db, 'users')
      tracker.unwatch(db, 'users')
      expect(tracker.watchedTables.has('users')).toBe(false)
    })

    it('is safe to call for an unwatched table', () => {
      expect(() => tracker.unwatch(db, 'users')).not.toThrow()
    })
  })

  describe('poll - INSERT events', () => {
    it('captures an INSERT with full row data as JSON', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice', 'alice@example.com', 30)

      const events = tracker.poll(db)
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

    it('captures multiple inserts in seq order', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice')
      insertUser(db, 'Bob')
      insertUser(db, 'Charlie')

      const events = tracker.poll(db)
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
    it('captures an UPDATE with both old and new row data', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice', 'alice@example.com', 30)
      tracker.poll(db)

      db.prepare('UPDATE users SET age = ? WHERE name = ?').run(31, 'Alice')

      const events = tracker.poll(db)
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
    it('captures a DELETE with old row data', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice', 'alice@example.com', 30)
      tracker.poll(db)

      db.prepare('DELETE FROM users WHERE name = ?').run('Alice')

      const events = tracker.poll(db)
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
    it('only returns new events on subsequent polls', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice')

      const first = tracker.poll(db)
      expect(first).toHaveLength(1)

      const second = tracker.poll(db)
      expect(second).toHaveLength(0)

      insertUser(db, 'Bob')
      const third = tracker.poll(db)
      expect(third).toHaveLength(1)
      expect(third[0].row.name).toBe('Bob')
    })
  })

  describe('poll - returns empty before watching', () => {
    it('returns empty array when no changes table exists', () => {
      const events = tracker.poll(db)
      expect(events).toEqual([])
    })
  })

  describe('poll - batch size', () => {
    it('limits the number of events per poll to pollBatchSize', () => {
      const tracker = new ChangeTracker({
        pollBatchSize: 2,
      })
      tracker.watch(db, 'users')

      insertUser(db, 'Alice')
      insertUser(db, 'Bob')
      insertUser(db, 'Charlie')
      insertUser(db, 'Dave')

      const first = tracker.poll(db)
      expect(first).toHaveLength(2)
      expect(first[0].row.name).toBe('Alice')
      expect(first[1].row.name).toBe('Bob')

      const second = tracker.poll(db)
      expect(second).toHaveLength(2)
      expect(second[0].row.name).toBe('Charlie')
      expect(second[1].row.name).toBe('Dave')

      const third = tracker.poll(db)
      expect(third).toHaveLength(0)
    })
  })

  describe('second instance detection', () => {
    it('a second tracker can poll the same changes table', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice')

      const tracker2 = new ChangeTracker()
      const events = tracker2.poll(db)
      expect(events).toHaveLength(1)
      expect(events[0].row.name).toBe('Alice')
    })
  })

  describe('multiple tables', () => {
    it('tracks changes across multiple watched tables', () => {
      db.exec(`
				CREATE TABLE posts (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					title TEXT NOT NULL,
					user_id INTEGER
				)
			`)

      tracker.watch(db, 'users')
      tracker.watch(db, 'posts')

      insertUser(db, 'Alice')
      db.prepare('INSERT INTO posts (title, user_id) VALUES (?, ?)').run('Hello', 1)

      const events = tracker.poll(db)
      expect(events).toHaveLength(2)
      expect(events[0].table).toBe('users')
      expect(events[1].table).toBe('posts')
    })
  })

  describe('WITHOUT ROWID tables', () => {
    it('tracks changes on WITHOUT ROWID tables using PK columns', () => {
      db.exec(`
				CREATE TABLE kv (
					key TEXT PRIMARY KEY,
					value TEXT
				) WITHOUT ROWID
			`)

      tracker.watch(db, 'kv')

      db.prepare('INSERT INTO kv (key, value) VALUES (?, ?)').run('greeting', 'hello')

      const events = tracker.poll(db)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      expect(events[0].row).toEqual({
        key: 'greeting',
        value: 'hello',
      })
    })

    it('tracks changes on tables with composite primary keys', () => {
      db.exec(`
				CREATE TABLE user_roles (
					user_id INTEGER,
					role_id INTEGER,
					granted_at TEXT,
					PRIMARY KEY (user_id, role_id)
				) WITHOUT ROWID
			`)

      tracker.watch(db, 'user_roles')

      db.prepare('INSERT INTO user_roles (user_id, role_id, granted_at) VALUES (?, ?, ?)').run(1, 10, '2025-01-01')

      const events = tracker.poll(db)
      expect(events).toHaveLength(1)
      expect(events[0].row).toEqual({
        user_id: 1,
        role_id: 10,
        granted_at: '2025-01-01',
      })
    })

    it('captures updates and deletes on WITHOUT ROWID tables', () => {
      db.exec(`
				CREATE TABLE kv (
					key TEXT PRIMARY KEY,
					value TEXT
				) WITHOUT ROWID
			`)

      tracker.watch(db, 'kv')

      db.prepare('INSERT INTO kv (key, value) VALUES (?, ?)').run('a', 'one')
      tracker.poll(db)

      db.prepare('UPDATE kv SET value = ? WHERE key = ?').run('two', 'a')
      const updateEvents = tracker.poll(db)
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

      db.prepare('DELETE FROM kv WHERE key = ?').run('a')
      const deleteEvents = tracker.poll(db)
      expect(deleteEvents).toHaveLength(1)
      expect(deleteEvents[0].type).toBe('delete')
      expect(deleteEvents[0].oldRow).toEqual({
        key: 'a',
        value: 'two',
      })
    })
  })

  describe('cleanup', () => {
    it('deletes entries older than the retention period', () => {
      const tracker = new ChangeTracker({
        retention: 1000,
      })
      tracker.watch(db, 'users')
      insertUser(db, 'Alice')

      const twoSecondsAgo = Date.now() / 1000 - 2
      db.prepare('UPDATE _sirannon_changes SET changed_at = ?').run(twoSecondsAgo)

      const deleted = tracker.cleanup(db)
      expect(deleted).toBe(1)

      const remaining = db.prepare('SELECT COUNT(*) as count FROM _sirannon_changes').get() as { count: number }
      expect(remaining.count).toBe(0)
    })

    it('preserves entries within the retention window', () => {
      const tracker = new ChangeTracker({
        retention: 60_000,
      })
      tracker.watch(db, 'users')
      insertUser(db, 'Alice')

      const deleted = tracker.cleanup(db)
      expect(deleted).toBe(0)
    })

    it('returns 0 when no changes table exists', () => {
      const deleted = tracker.cleanup(db)
      expect(deleted).toBe(0)
    })

    it('does not delete un-polled rows when poll has been used', () => {
      const tracker = new ChangeTracker({
        retention: 1000,
      })
      tracker.watch(db, 'users')

      insertUser(db, 'Alice')
      tracker.poll(db)

      insertUser(db, 'Bob')

      const twoSecondsAgo = Date.now() / 1000 - 2
      db.prepare('UPDATE _sirannon_changes SET changed_at = ?').run(twoSecondsAgo)

      const deleted = tracker.cleanup(db)
      expect(deleted).toBe(1)

      const events = tracker.poll(db)
      expect(events).toHaveLength(1)
      expect(events[0].row.name).toBe('Bob')
    })
  })

  describe('null and special values', () => {
    it('handles null column values in JSON snapshots', () => {
      tracker.watch(db, 'users')
      insertUser(db, 'Alice', null, null)

      const events = tracker.poll(db)
      expect(events[0].row).toEqual({
        id: 1,
        name: 'Alice',
        email: null,
        age: null,
      })
    })
  })

  describe('custom changes table name', () => {
    it('uses a custom table name when configured', () => {
      const custom = new ChangeTracker({
        changesTable: 'my_changelog',
      })
      custom.watch(db, 'users')

      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='my_changelog'").all()
      expect(tables).toHaveLength(1)

      insertUser(db, 'Alice', 'alice@example.com', 30)

      const events = custom.poll(db)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      expect(events[0].row.name).toBe('Alice')
    })
  })

  describe('watchedTables caching', () => {
    it('returns the same Set reference when no changes', () => {
      tracker.watch(db, 'users')
      const first = tracker.watchedTables
      const second = tracker.watchedTables
      expect(first).toBe(second)
    })

    it('returns a new Set reference after watch or unwatch', () => {
      tracker.watch(db, 'users')
      const first = tracker.watchedTables

      db.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY)')
      tracker.watch(db, 'posts')
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
  let db: Database.Database
  let tracker: ChangeTracker
  let manager: SubscriptionManager

  beforeEach(() => {
    db = createTestDb()
    tracker = new ChangeTracker()
    manager = new SubscriptionManager()
    tracker.watch(db, 'users')
  })

  afterEach(() => {
    db.close()
  })

  it('dispatches events on each polling interval', async () => {
    const received: ChangeEvent[] = []
    manager.subscribe('users', undefined, e => received.push(e))

    const stop = startPolling(db, tracker, manager, 20)

    insertUser(db, 'Alice')

    await new Promise(resolve => setTimeout(resolve, 60))

    stop()
    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.name).toBe('Alice')
  })

  it('stops polling when the stop function is called', async () => {
    const received: ChangeEvent[] = []
    manager.subscribe('users', undefined, e => received.push(e))

    const stop = startPolling(db, tracker, manager, 20)
    stop()

    insertUser(db, 'Alice')
    await new Promise(resolve => setTimeout(resolve, 60))

    expect(received).toHaveLength(0)
  })
})

describe('CDC integration', () => {
  let db: Database.Database
  let tracker: ChangeTracker
  let manager: SubscriptionManager

  beforeEach(() => {
    db = createTestDb()
    tracker = new ChangeTracker()
    manager = new SubscriptionManager()
  })

  afterEach(() => {
    db.close()
  })

  it('end-to-end: watch -> insert -> poll -> dispatch -> callback', () => {
    const received: ChangeEvent[] = []

    tracker.watch(db, 'users')
    manager.subscribe('users', undefined, e => received.push(e))

    insertUser(db, 'Alice', 'alice@example.com', 30)
    const events = tracker.poll(db)
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

  it('end-to-end: insert -> update -> delete cycle', () => {
    const received: ChangeEvent[] = []

    tracker.watch(db, 'users')
    manager.subscribe('users', undefined, e => received.push(e))

    insertUser(db, 'Alice', 'alice@example.com', 30)
    manager.dispatch(tracker.poll(db))

    db.prepare('UPDATE users SET age = ? WHERE name = ?').run(31, 'Alice')
    manager.dispatch(tracker.poll(db))

    db.prepare('DELETE FROM users WHERE name = ?').run('Alice')
    manager.dispatch(tracker.poll(db))

    expect(received).toHaveLength(3)
    expect(received[0].type).toBe('insert')
    expect(received[1].type).toBe('update')
    expect(received[1].oldRow?.age).toBe(30)
    expect(received[1].row.age).toBe(31)
    expect(received[2].type).toBe('delete')
    expect(received[2].oldRow?.name).toBe('Alice')
  })

  it('filtered subscription receives only matching events', () => {
    const aliceEvents: ChangeEvent[] = []
    const allEvents: ChangeEvent[] = []

    tracker.watch(db, 'users')
    manager.subscribe('users', { name: 'Alice' }, e => aliceEvents.push(e))
    manager.subscribe('users', undefined, e => allEvents.push(e))

    insertUser(db, 'Alice')
    insertUser(db, 'Bob')
    insertUser(db, 'Alice')

    manager.dispatch(tracker.poll(db))

    expect(allEvents).toHaveLength(3)
    expect(aliceEvents).toHaveLength(2)
  })

  it('unwatch stops capturing new changes', () => {
    tracker.watch(db, 'users')
    insertUser(db, 'Alice')
    tracker.poll(db)

    tracker.unwatch(db, 'users')
    insertUser(db, 'Bob')

    const events = tracker.poll(db)
    expect(events).toHaveLength(0)
  })
})
