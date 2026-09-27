import type { ChangeEvent } from '../core/types.js'
import type { RemoteSubscription, RemoteSubscriptionBuilder, SubscribeOptions, Transport } from './types.js'

/**
 * Builds a remote CDC subscription with optional row-level filters.
 * It has the same methods as the core `SubscriptionBuilder`, but `subscribe()`
 * returns a promise, because the server has to confirm the subscription first.
 *
 * @internal
 */
export class RemoteSubscriptionBuilderImpl implements RemoteSubscriptionBuilder {
  private conditions: Record<string, unknown> = {}

  constructor(
    private readonly table: string,
    private readonly transport: Transport,
  ) {}

  filter(conditions: Record<string, unknown>): RemoteSubscriptionBuilder {
    this.conditions = { ...this.conditions, ...conditions }
    return this
  }

  subscribe<T = Record<string, unknown>>(
    callback: (event: ChangeEvent<T>) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription> {
    const filter = Object.keys(this.conditions).length > 0 ? this.conditions : undefined
    return this.transport.subscribe(this.table, filter, callback as (event: ChangeEvent) => void, options)
  }
}
