import { describe, expect, it } from 'vitest'
import { SubscriptionBuilderImpl, SubscriptionManager } from '../../cdc/subscription.js'
import type { ChangeEvent } from '../../types.js'

interface Note {
  id: number
  body: string
}

describe('typed change subscriptions', () => {
  it('types row and oldRow from the subscribe type parameter', () => {
    const manager = new SubscriptionManager()
    const builder = new SubscriptionBuilderImpl('notes', manager)
    const bodies: string[] = []

    builder.subscribe<Note>(event => {
      bodies.push(event.row.body)
    })

    const event: ChangeEvent = {
      type: 'insert',
      table: 'notes',
      row: { id: 1, body: 'first' },
      seq: 1n,
      timestamp: Date.now(),
    }
    manager.dispatch([event])

    expect(bodies).toEqual(['first'])
  })

  it('keeps an untyped subscription on the open row shape', () => {
    const manager = new SubscriptionManager()
    const builder = new SubscriptionBuilderImpl('notes', manager)
    const seen: Array<Record<string, unknown>> = []

    builder.subscribe(event => {
      seen.push(event.row)
    })

    manager.dispatch([
      { type: 'insert', table: 'notes', row: { id: 2, body: 'second' }, seq: 2n, timestamp: Date.now() },
    ])

    expect(seen).toEqual([{ id: 2, body: 'second' }])
  })
})
