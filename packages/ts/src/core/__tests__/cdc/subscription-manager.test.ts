import { describe, expect, it } from 'vitest'
import { SubscriptionManager } from '../../cdc/subscription.js'
import type { ChangeEvent } from '../../types.js'

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
