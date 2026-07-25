import { describe, expect, it, vi } from 'vitest'
import { RemoteSubscriptionBuilderImpl } from '../subscription.js'

describe('RemoteSubscriptionBuilder', () => {
  it('passes filter conditions to the transport subscribe', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      batch: vi.fn(),
      load: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('users', transport)
    const callback = () => {}
    await builder.filter({ name: 'Alice' }).subscribe(callback)

    expect(subscribeFn).toHaveBeenCalledWith('users', { name: 'Alice' }, callback, undefined)
  })

  it('merges multiple filter calls', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      batch: vi.fn(),
      load: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('users', transport)
    await builder
      .filter({ name: 'Alice' })
      .filter({ age: 30 })
      .subscribe(() => {})

    expect(subscribeFn).toHaveBeenCalledWith('users', { name: 'Alice', age: 30 }, expect.any(Function), undefined)
  })

  it('passes undefined filter when no conditions are set', async () => {
    const subscribeFn = vi.fn().mockResolvedValue({ unsubscribe: () => {} })
    const transport = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      batch: vi.fn(),
      load: vi.fn(),
      subscribe: subscribeFn,
      close: vi.fn(),
    }

    const builder = new RemoteSubscriptionBuilderImpl('orders', transport)
    await builder.subscribe(() => {})

    expect(subscribeFn).toHaveBeenCalledWith('orders', undefined, expect.any(Function), undefined)
  })
})
