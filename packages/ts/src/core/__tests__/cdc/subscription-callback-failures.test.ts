import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SubscriptionBuilderImpl, SubscriptionManager } from '../../cdc/subscription.js'
import type { ChangeEvent } from '../../types.js'

function changeOn(table: string): ChangeEvent {
  return { type: 'insert', table, row: { id: 1 }, seq: 1n, timestamp: Date.now() }
}

async function settleRejections(): Promise<void> {
  await new Promise(resolve => setImmediate(resolve))
  await new Promise(resolve => setImmediate(resolve))
}

describe('subscription callback failures', () => {
  let unhandled: unknown[]
  let capture: (reason: unknown) => void

  beforeEach(() => {
    unhandled = []
    capture = reason => unhandled.push(reason)
    process.on('unhandledRejection', capture)
  })

  afterEach(() => {
    process.off('unhandledRejection', capture)
  })

  it('leaves no unhandled rejection when an async callback rejects', async () => {
    const manager = new SubscriptionManager()
    manager.subscribe('orders', undefined, async () => {
      throw new Error('handler failed')
    })

    manager.dispatch([changeOn('orders')])
    await settleRejections()

    expect(unhandled).toEqual([])
  })

  it('delivers to every other subscriber when one callback throws', () => {
    const manager = new SubscriptionManager()
    const delivered: string[] = []

    manager.subscribe('orders', undefined, () => {
      throw new Error('handler failed')
    })
    manager.subscribe('orders', undefined, () => {
      delivered.push('second')
    })

    manager.dispatch([changeOn('orders')])

    expect(delivered).toEqual(['second'])
  })

  it('passes a synchronous throw to onError', () => {
    const manager = new SubscriptionManager()
    const reported: string[] = []

    manager.subscribe(
      'orders',
      undefined,
      () => {
        throw new Error('handler failed')
      },
      { onError: error => reported.push(error.message) },
    )

    manager.dispatch([changeOn('orders')])

    expect(reported).toEqual(['handler failed'])
  })

  it('passes an async rejection to onError', async () => {
    const manager = new SubscriptionManager()
    const reported: string[] = []

    manager.subscribe('orders', undefined, async () => await Promise.reject(new Error('async failed')), {
      onError: error => reported.push(error.message),
    })

    manager.dispatch([changeOn('orders')])
    await settleRejections()

    expect(reported).toEqual(['async failed'])
    expect(unhandled).toEqual([])
  })

  it('drops an error thrown by onError', () => {
    const manager = new SubscriptionManager()

    manager.subscribe(
      'orders',
      undefined,
      () => {
        throw new Error('handler failed')
      },
      {
        onError: () => {
          throw new Error('reporter failed')
        },
      },
    )

    expect(() => manager.dispatch([changeOn('orders')])).not.toThrow()
  })

  it('carries onError through the subscription builder', () => {
    const manager = new SubscriptionManager()
    const reported: string[] = []
    const builder = new SubscriptionBuilderImpl('orders', manager)

    builder.subscribe(
      () => {
        throw new Error('handler failed')
      },
      { onError: error => reported.push(error.message) },
    )

    manager.dispatch([changeOn('orders')])

    expect(reported).toEqual(['handler failed'])
  })

  it('reports a poll failure that stops the loop to every active subscription', () => {
    const manager = new SubscriptionManager()
    const reported: string[] = []

    manager.subscribe('orders', undefined, () => {}, { onError: error => reported.push(error.message) })
    manager.subscribe('invoices', undefined, () => {}, { onError: error => reported.push(error.message) })

    manager.reportError(new Error('change log unreadable'))

    expect(reported).toEqual(['change log unreadable', 'change log unreadable'])
  })
})
