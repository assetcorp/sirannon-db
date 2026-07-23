import type { ChangeEvent, ReadConcernLevel } from '../core/types.js'
import type { TopologyRouting } from './cluster-routing.js'
import { shouldRefreshRouting } from './cluster-routing.js'
import type { RemoteSubscription, SubscribeOptions, Transport } from './types.js'
import { RemoteError } from './types.js'

interface TrackedRemoteSubscription {
  id: number
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
  onReset: (() => void) | undefined
  remote: RemoteSubscription | null
  active: boolean
}

export class TopologySubscriptionSet {
  private transport: Transport | null = null
  private transportRequest: Promise<Transport> | null = null
  private operation: Promise<void> = Promise.resolve()
  private readonly subscriptions = new Map<number, TrackedRemoteSubscription>()
  private nextSubscriptionId = 0
  private currentUrl = ''

  constructor(
    private readonly databaseId: string,
    private readonly routing: TopologyRouting,
    private readonly assertOpen: () => void,
  ) {}

  get size(): number {
    return this.subscriptions.size
  }

  async subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription> {
    try {
      return await this.subscribeOnCurrentEndpoint(table, filter, callback, options)
    } catch (err) {
      if (this.routing._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        const hadActiveSubscriptions = this.subscriptions.size > 0
        if (!hadActiveSubscriptions) {
          this.closeTransport()
        }
        await this.routing._refreshClusterRouting(this.databaseId)
        if (!hadActiveSubscriptions) {
          this.closeTransport()
        }
        return this.subscribeOnCurrentEndpoint(table, filter, callback, options)
      }
      throw err
    }
  }

  async handleRoutingChanged(): Promise<void> {
    if (!this.routing._usesCoordinatorDiscovery()) return
    if (this.subscriptions.size === 0) return
    await this.withOperation(() => this.migrateToCurrentEndpoint())
  }

  closeAll(): void {
    for (const subscription of this.subscriptions.values()) {
      subscription.active = false
      subscription.remote = null
    }
    this.subscriptions.clear()
    this.closeTransport()
  }

  private async subscribeOnCurrentEndpoint(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription> {
    return this.withOperation(async () => {
      const transport = await this.getTransport(this.routing._getReadConcern())
      const remote = await transport.subscribe(table, filter, callback, options)
      const subscription: TrackedRemoteSubscription = {
        id: ++this.nextSubscriptionId,
        table,
        filter,
        callback,
        onReset: options?.onReset,
        remote,
        active: true,
      }
      this.subscriptions.set(subscription.id, subscription)
      return this.createSubscriptionHandle(subscription)
    })
  }

  private createSubscriptionHandle(subscription: TrackedRemoteSubscription): RemoteSubscription {
    return {
      unsubscribe: () => {
        if (!subscription.active) return
        subscription.active = false
        this.subscriptions.delete(subscription.id)
        const remote = subscription.remote
        subscription.remote = null
        remote?.unsubscribe()
      },
    }
  }

  private async migrateToCurrentEndpoint(): Promise<void> {
    this.assertOpen()
    const subscriptions = [...this.subscriptions.values()].filter(subscription => subscription.active)
    if (subscriptions.length === 0) return

    const readConcern = this.routing._getReadConcern()
    const endpoint = await this.routing._getReadEndpoint(this.databaseId, readConcern)
    this.assertOpen()
    if (this.transport && this.currentUrl === endpoint) {
      return
    }

    const nextTransport = this.routing._createTransportForEndpoint(endpoint, this.databaseId)
    const nextSubscriptions = new Map<number, RemoteSubscription>()
    try {
      for (const subscription of subscriptions) {
        if (!subscription.active) continue
        const remote = await nextTransport.subscribe(subscription.table, subscription.filter, subscription.callback, {
          onReset: subscription.onReset,
        })
        if (!subscription.active) {
          remote.unsubscribe()
          continue
        }
        nextSubscriptions.set(subscription.id, remote)
      }
    } catch (err) {
      for (const remote of nextSubscriptions.values()) {
        remote.unsubscribe()
      }
      nextTransport.close()
      throw toSubscriptionRoutingError(err, this.databaseId)
    }

    const oldTransport = this.transport
    this.transport = nextTransport
    this.currentUrl = endpoint

    for (const subscription of subscriptions) {
      const nextRemote = nextSubscriptions.get(subscription.id)
      if (!nextRemote) continue
      const previousRemote = subscription.remote
      if (!subscription.active) {
        nextRemote.unsubscribe()
        continue
      }
      subscription.remote = nextRemote
      previousRemote?.unsubscribe()
    }

    if (oldTransport) {
      oldTransport.close()
    }
  }

  private async getTransport(readConcern?: ReadConcernLevel): Promise<Transport> {
    this.assertOpen()

    if (this.transport) {
      return this.transport
    }

    while (this.transportRequest) {
      await this.transportRequest.catch(() => undefined)
      this.assertOpen()
      if (this.transport) {
        return this.transport
      }
    }

    const request = this.resolveTransport(readConcern)
    this.transportRequest = request
    try {
      return await request
    } finally {
      if (this.transportRequest === request) {
        this.transportRequest = null
      }
    }
  }

  private async resolveTransport(readConcern?: ReadConcernLevel): Promise<Transport> {
    if (this.transport) {
      return this.transport
    }

    const endpoint = await this.routing._getReadEndpoint(this.databaseId, readConcern)
    this.assertOpen()

    if (this.transport) {
      return this.transport
    }

    this.transport = this.routing._createTransportForEndpoint(endpoint, this.databaseId)
    this.currentUrl = endpoint
    return this.transport
  }

  private closeTransport(): void {
    if (this.transport) {
      this.transport.close()
      this.transport = null
    }
    this.currentUrl = ''
  }

  private async withOperation<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.operation
    let release = () => {}
    this.operation = new Promise<void>(resolve => {
      release = resolve
    })
    await previous.catch(() => undefined)
    try {
      return await operation()
    } finally {
      release()
    }
  }
}

function toSubscriptionRoutingError(err: unknown, databaseId: string): RemoteError {
  return new RemoteError(
    'ROUTING_ERROR',
    `Could not re-establish active subscriptions on refreshed routing for database '${databaseId}': ${
      err instanceof Error ? err.message : String(err)
    }`,
  )
}
