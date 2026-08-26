import type { ChangeEvent } from '../core/types.js'
import type { RemoteSubscription, RemoteSubscriptionBuilder, SubscribeOptions, Transport } from './types.js'

/**
 * Builds a remote CDC subscription with optional row-level filters.
 * Mirrors the core `SubscriptionBuilder` interface but returns
 * a promise from `subscribe()` since confirming the subscription
 * requires a server round-trip.
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
