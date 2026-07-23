import type { BulkLoadDurability, ChangeEvent, Params, ReadConcernLevel, WriteConcern } from '../core/types.js'
import type {
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from '../server/protocol.js'
import type { TopologyRouting } from './cluster-routing.js'
import { shouldRefreshRouting } from './cluster-routing.js'
import { TopologySubscriptionSet } from './topology-subscriptions.js'
import type { RemoteSubscription, SubscribeOptions, Transport } from './types.js'
import { RemoteError } from './types.js'

export class TopologyAwareTransport implements Transport {
  private closed = false
  private readTransport: Transport | null = null
  private writeTransport: Transport | null = null
  private readTransportRequest: Promise<Transport> | null = null
  private writeTransportRequest: Promise<Transport> | null = null
  private readonly subscriptionSet: TopologySubscriptionSet
  private currentReadUrl = ''
  private currentWriteUrl = ''

  constructor(
    private readonly databaseId: string,
    private readonly routing: TopologyRouting,
    private readonly onClose: (transport: TopologyAwareTransport) => void,
  ) {
    this.subscriptionSet = new TopologySubscriptionSet(databaseId, routing, () => this.assertOpen())
  }

  async query(sql: string, params?: Params): Promise<QueryResponse> {
    const readConcern = this.routing._getReadConcern()
    const transport = await this.getReadTransport(readConcern)
    const endpointUsed = this.currentReadUrl
    try {
      return await transport.query(sql, params, readConcern ? { level: readConcern } : undefined)
    } catch (err) {
      if (this.routing._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.routing._refreshClusterRouting(this.databaseId)
        this.readTransport = null
        this.currentReadUrl = ''
        const refreshed = await this.getReadTransport(readConcern)
        return refreshed.query(sql, params, readConcern ? { level: readConcern } : undefined)
      }
      const isTransportError =
        err instanceof Error && (err.name !== 'RemoteError' || (err as { code?: string }).code === 'CONNECTION_ERROR')
      const writeEndpoint = await this.routing._getWriteEndpoint(this.databaseId)
      if (isTransportError && endpointUsed && endpointUsed !== writeEndpoint) {
        this.routing._removeReplica(endpointUsed)
        if (this.currentReadUrl === endpointUsed) {
          this.readTransport = null
          this.currentReadUrl = ''
        }
        const fallback = await this.getReadTransport()
        return fallback.query(sql, params)
      }
      throw err
    }
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    return this.onWriteTransport(transport => transport.execute(sql, params))
  }

  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse> {
    return this.onWriteTransport(transport => transport.transaction(statements))
  }

  async batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse> {
    return this.onWriteTransport(transport => transport.batch(sql, paramsBatch, writeConcern))
  }

  async load(
    sql: string,
    paramsBatch: Params[],
    durability?: BulkLoadDurability,
    checkpoint?: boolean,
  ): Promise<LoadResponse> {
    return this.onWriteTransport(transport => transport.load(sql, paramsBatch, durability, checkpoint))
  }

  private async onWriteTransport<T>(operation: (transport: Transport) => Promise<T>): Promise<T> {
    const transport = await this.getWriteTransport()
    try {
      return await operation(transport)
    } catch (err) {
      if (this.routing._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.routing._refreshClusterRouting(this.databaseId)
        this.writeTransport = null
        this.currentWriteUrl = ''
      }
      throw err
    }
  }

  async subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription> {
    return this.subscriptionSet.subscribe(table, filter, callback, options)
  }

  async _handleClusterRoutingChanged(): Promise<void> {
    await this.subscriptionSet.handleRoutingChanged()
  }

  close(): void {
    this.closed = true
    const sameTransport = this.readTransport !== null && this.readTransport === this.writeTransport
    this.subscriptionSet.closeAll()
    if (this.readTransport) {
      this.readTransport.close()
      this.readTransport = null
    }
    if (this.writeTransport && !sameTransport) {
      this.writeTransport.close()
    }
    this.writeTransport = null
    this.onClose(this)
  }

  private async getReadTransport(readConcern?: ReadConcernLevel): Promise<Transport> {
    this.assertOpen()

    while (this.readTransportRequest) {
      await this.readTransportRequest.catch(() => undefined)
      this.assertOpen()
    }

    const request = this.resolveReadTransport(readConcern)
    this.readTransportRequest = request
    try {
      return await request
    } finally {
      if (this.readTransportRequest === request) {
        this.readTransportRequest = null
      }
    }
  }

  private async resolveReadTransport(readConcern?: ReadConcernLevel): Promise<Transport> {
    const endpoint = await this.routing._getReadEndpoint(this.databaseId, readConcern)
    this.assertOpen()

    if (this.readTransport && this.currentReadUrl === endpoint) {
      return this.readTransport
    }

    const nextTransport = this.routing._createTransportForEndpoint(endpoint, this.databaseId)
    const oldTransport = this.readTransport
    this.readTransport = nextTransport
    this.currentReadUrl = endpoint

    if (oldTransport) {
      oldTransport.close()
    }

    return this.readTransport
  }

  private async getWriteTransport(): Promise<Transport> {
    this.assertOpen()

    while (this.writeTransportRequest) {
      await this.writeTransportRequest.catch(() => undefined)
      this.assertOpen()
    }

    const request = this.resolveWriteTransport()
    this.writeTransportRequest = request
    try {
      return await request
    } finally {
      if (this.writeTransportRequest === request) {
        this.writeTransportRequest = null
      }
    }
  }

  private async resolveWriteTransport(): Promise<Transport> {
    const endpoint = await this.routing._getWriteEndpoint(this.databaseId)
    this.assertOpen()

    if (this.writeTransport && this.currentWriteUrl === endpoint) {
      return this.writeTransport
    }

    const nextTransport = this.routing._createTransportForEndpoint(endpoint, this.databaseId)
    const oldTransport = this.writeTransport
    this.writeTransport = nextTransport
    this.currentWriteUrl = endpoint

    if (oldTransport) {
      oldTransport.close()
    }

    return this.writeTransport
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new RemoteError('TRANSPORT_ERROR', 'Transport is closed')
    }
  }
}
