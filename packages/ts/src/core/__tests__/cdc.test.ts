import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../cdc/change-tracker.js'
import {
	SubscriptionBuilderImpl,
	SubscriptionManager,
	startPolling,
} from '../cdc/subscription.js'
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
	const result = db
		.prepare('INSERT INTO users (name, email, age) VALUES (?, ?, ?)')
		.run(name, email, age)
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

			const tables = db
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='table' AND name='_sirannon_changes'",
				)
				.all()
			expect(tables).toHaveLength(1)
		})

		it('installs INSERT, UPDATE, and DELETE triggers', () => {
			tracker.watch(db, 'users')

			const triggers = db
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
				)
				.all() as { name: string }[]

			const names = triggers.map(t => t.name).sort()
			expect(names).toEqual([
				'_sirannon_trg_users_delete',
				'_sirannon_trg_users_insert',
				'_sirannon_trg_users_update',
			])
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
			expect(() => tracker.watch(db, 'nonexistent')).toThrow(
				"Table 'nonexistent' does not exist or has no columns",
			)
		})
	})

	describe('unwatch', () => {
		it('removes triggers for the table', () => {
			tracker.watch(db, 'users')
			tracker.unwatch(db, 'users')

			const triggers = db
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_users_%'",
				)
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
			db.prepare('INSERT INTO posts (title, user_id) VALUES (?, ?)').run(
				'Hello',
				1,
			)

			const events = tracker.poll(db)
			expect(events).toHaveLength(2)
			expect(events[0].table).toBe('users')
			expect(events[1].table).toBe('posts')
		})
	})

	describe('cleanup', () => {
		it('deletes entries older than the retention period', () => {
			const tracker = new ChangeTracker({ retention: 1000 })
			tracker.watch(db, 'users')
			insertUser(db, 'Alice')

			const twoSecondsAgo = Date.now() / 1000 - 2
			db.prepare('UPDATE _sirannon_changes SET changed_at = ?').run(
				twoSecondsAgo,
			)

			const deleted = tracker.cleanup(db)
			expect(deleted).toBe(1)

			const remaining = db
				.prepare('SELECT COUNT(*) as count FROM _sirannon_changes')
				.get() as { count: number }
			expect(remaining.count).toBe(0)
		})

		it('preserves entries within the retention window', () => {
			const tracker = new ChangeTracker({ retention: 60_000 })
			tracker.watch(db, 'users')
			insertUser(db, 'Alice')

			const deleted = tracker.cleanup(db)
			expect(deleted).toBe(0)
		})

		it('returns 0 when no changes table exists', () => {
			const deleted = tracker.cleanup(db)
			expect(deleted).toBe(0)
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
			const custom = new ChangeTracker({ changesTable: 'my_changelog' })
			custom.watch(db, 'users')

			const tables = db
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='table' AND name='my_changelog'",
				)
				.all()
			expect(tables).toHaveLength(1)

			insertUser(db, 'Alice', 'alice@example.com', 30)

			const events = custom.poll(db)
			expect(events).toHaveLength(1)
			expect(events[0].type).toBe('insert')
			expect(events[0].row.name).toBe('Alice')
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

			manager.subscribe('users', { name: 'Alice', age: 30 }, e =>
				received.push(e),
			)

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

		new SubscriptionBuilderImpl('users', manager)
			.filter({ name: 'Alice' })
			.subscribe(e => received.push(e))

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
