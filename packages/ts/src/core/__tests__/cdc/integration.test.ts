import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import { SubscriptionManager } from '../../cdc/subscription.js'
import type { SQLiteConnection } from '../../driver/types.js'
import type { ChangeEvent } from '../../types.js'
import { createTestDb, insertUser } from './_helpers.js'

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
