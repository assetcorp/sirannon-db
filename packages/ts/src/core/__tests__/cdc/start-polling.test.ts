import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import { SubscriptionManager, startPolling } from '../../cdc/subscription.js'
import type { SQLiteConnection } from '../../driver/types.js'
import type { ChangeEvent } from '../../types.js'
import { createTestDb, insertUser } from './_helpers.js'

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
