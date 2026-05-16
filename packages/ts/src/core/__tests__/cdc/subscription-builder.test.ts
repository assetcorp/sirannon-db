import { describe, expect, it } from 'vitest'
import { SubscriptionBuilderImpl, SubscriptionManager } from '../../cdc/subscription.js'
import type { ChangeEvent } from '../../types.js'

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
