import { describe, expect, it } from 'vitest'
import { SubscriptionManager } from '../../cdc/subscription.js'
import type { ChangeEvent } from '../../types.js'

function collect(filter: Record<string, unknown> | undefined, events: ChangeEvent[]): ChangeEvent[] {
  const manager = new SubscriptionManager()
  const received: ChangeEvent[] = []
  manager.subscribe('tickets', filter, event => received.push(event))
  manager.dispatch(events)
  return received
}

const openFilter = { status: 'open' }

describe('filter boundaries', () => {
  it('delivers a row updated out of the filter as a delete', () => {
    const received = collect(openFilter, [
      {
        type: 'update',
        table: 'tickets',
        row: { id: 1, status: 'closed', title: 'Broken link' },
        oldRow: { id: 1, status: 'open', title: 'Broken link' },
        seq: 7n,
        timestamp: 1,
        rowId: '1',
      },
    ])

    expect(received).toHaveLength(1)
    expect(received[0].type).toBe('delete')
    expect(received[0].oldRow).toEqual({ id: 1, status: 'open', title: 'Broken link' })
    expect(received[0].row).toEqual({})
    expect(received[0].seq).toBe(7n)
  })

  it('delivers a row updated into the filter as an insert', () => {
    const received = collect(openFilter, [
      {
        type: 'update',
        table: 'tickets',
        row: { id: 2, status: 'open', title: 'Slow query' },
        oldRow: { id: 2, status: 'closed', title: 'Slow query' },
        seq: 8n,
        timestamp: 1,
      },
    ])

    expect(received).toHaveLength(1)
    expect(received[0].type).toBe('insert')
    expect(received[0].row).toEqual({ id: 2, status: 'open', title: 'Slow query' })
    expect(received[0].oldRow).toBeUndefined()
  })

  it('delivers a row changed inside the filter as an update', () => {
    const event: ChangeEvent = {
      type: 'update',
      table: 'tickets',
      row: { id: 3, status: 'open', title: 'Renamed' },
      oldRow: { id: 3, status: 'open', title: 'Original' },
      seq: 9n,
      timestamp: 1,
    }

    const received = collect(openFilter, [event])

    expect(received).toHaveLength(1)
    expect(received[0]).toBe(event)
  })

  it('delivers nothing for a change outside the filter in both states', () => {
    const received = collect(openFilter, [
      {
        type: 'update',
        table: 'tickets',
        row: { id: 4, status: 'closed', title: 'After' },
        oldRow: { id: 4, status: 'archived', title: 'Before' },
        seq: 10n,
        timestamp: 1,
      },
    ])

    expect(received).toHaveLength(0)
  })

  it('leaves inserts and deletes on the filter boundary untouched', () => {
    const insert: ChangeEvent = {
      type: 'insert',
      table: 'tickets',
      row: { id: 5, status: 'open' },
      seq: 11n,
      timestamp: 1,
    }
    const remove: ChangeEvent = {
      type: 'delete',
      table: 'tickets',
      row: {},
      oldRow: { id: 5, status: 'open' },
      seq: 12n,
      timestamp: 1,
    }

    const received = collect(openFilter, [insert, remove])

    expect(received).toEqual([insert, remove])
    expect(received[0]).toBe(insert)
    expect(received[1]).toBe(remove)
  })

  it('drops an insert outside the filter and a delete of a row that never matched', () => {
    const received = collect(openFilter, [
      { type: 'insert', table: 'tickets', row: { id: 6, status: 'closed' }, seq: 13n, timestamp: 1 },
      {
        type: 'delete',
        table: 'tickets',
        row: {},
        oldRow: { id: 6, status: 'closed' },
        seq: 14n,
        timestamp: 1,
      },
    ])

    expect(received).toHaveLength(0)
  })

  it('delivers every event untouched when the subscription carries no filter', () => {
    const events: ChangeEvent[] = [
      {
        type: 'update',
        table: 'tickets',
        row: { id: 7, status: 'closed' },
        oldRow: { id: 7, status: 'open' },
        seq: 15n,
        timestamp: 1,
      },
      { type: 'insert', table: 'tickets', row: { id: 8, status: 'closed' }, seq: 16n, timestamp: 1 },
    ]

    const received = collect(undefined, events)

    expect(received[0]).toBe(events[0])
    expect(received[1]).toBe(events[1])
  })

  it('does not mutate the event shared by every subscriber on the table', () => {
    const manager = new SubscriptionManager()
    const filtered: ChangeEvent[] = []
    const unfiltered: ChangeEvent[] = []
    manager.subscribe('tickets', openFilter, event => filtered.push(event))
    manager.subscribe('tickets', undefined, event => unfiltered.push(event))

    const event: ChangeEvent = {
      type: 'update',
      table: 'tickets',
      row: { id: 9, status: 'closed' },
      oldRow: { id: 9, status: 'open' },
      seq: 17n,
      timestamp: 1,
    }
    manager.dispatch([event])

    expect(filtered[0].type).toBe('delete')
    expect(unfiltered[0]).toBe(event)
    expect(event.type).toBe('update')
    expect(event.row).toEqual({ id: 9, status: 'closed' })
  })

  it('evaluates both states of a multi-column filter', () => {
    const received = collect({ status: 'open', ownerId: 4 }, [
      {
        type: 'update',
        table: 'tickets',
        row: { id: 10, status: 'open', ownerId: 5 },
        oldRow: { id: 10, status: 'open', ownerId: 4 },
        seq: 18n,
        timestamp: 1,
      },
    ])

    expect(received).toHaveLength(1)
    expect(received[0].type).toBe('delete')
    expect(received[0].oldRow).toEqual({ id: 10, status: 'open', ownerId: 4 })
  })
})
