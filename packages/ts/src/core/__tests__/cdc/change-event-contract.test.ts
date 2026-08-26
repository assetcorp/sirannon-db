import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import { SubscriptionManager } from '../../cdc/subscription.js'
import type { SQLiteConnection } from '../../driver/types.js'
import type { ChangeEvent } from '../../types.js'
import { createTestDb, insertUser } from './_helpers.js'

describe('ChangeEvent contract', () => {
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

  async function collect(): Promise<ChangeEvent[]> {
    const received: ChangeEvent[] = []
    await tracker.watch(conn, 'users')
    manager.subscribe('users', undefined, event => received.push(event))
    return received
  }

  it('reports timestamp as milliseconds since the Unix epoch', async () => {
    const received = await collect()
    const before = Date.now()

    await insertUser(conn, 'Ada', 'ada@example.com', 36)
    manager.dispatch(await tracker.poll(conn))

    const after = Date.now()
    expect(received).toHaveLength(1)
    expect(received[0].timestamp).toBeGreaterThanOrEqual(before - 1000)
    expect(received[0].timestamp).toBeLessThanOrEqual(after + 1000)
  })

  it('reports a whole number of milliseconds', async () => {
    const received = await collect()

    await insertUser(conn, 'Grace', 'grace@example.com', 45)
    manager.dispatch(await tracker.poll(conn))

    expect(Number.isInteger(received[0].timestamp)).toBe(true)
  })

  it('carries the previous row in oldRow and an empty row on a delete', async () => {
    const received = await collect()

    const id = await insertUser(conn, 'Alan', 'alan@example.com', 41)
    manager.dispatch(await tracker.poll(conn))

    const remove = await conn.prepare('DELETE FROM users WHERE id = ?')
    await remove.run(id)
    manager.dispatch(await tracker.poll(conn))

    const deletion = received[received.length - 1]
    expect(deletion.type).toBe('delete')
    expect(deletion.row).toEqual({})
    expect(deletion.oldRow).toEqual({ id, name: 'Alan', email: 'alan@example.com', age: 41 })
  })
})
