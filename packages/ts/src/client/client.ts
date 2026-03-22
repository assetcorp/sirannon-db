import type { ClientOptions } from '../core/types.js'
import { RemoteDatabase } from './database-proxy.js'
import { HttpTransport } from './transport/http.js'
import { WebSocketTransport } from './transport/ws.js'
import type { Transport } from './types.js'

export interface TopologyAwareClientOptions extends ClientOptions {
  primary?: string
  replicas?: string[]
  readPreference?: 'primary' | 'replica' | 'nearest'
}

interface EndpointLatency {
  url: string
  latencyMs: number
  reachable: boolean
}

function isTopologyConfig(urlOrOpts: string | TopologyAwareClientOptions): urlOrOpts is TopologyAwareClientOptions {
  return typeof urlOrOpts === 'object'
}

function toBaseUrl(url: string): string {
  return url.replace(/\/$/, '')
}

function toWsUrl(baseUrl: string): string {
  return baseUrl.replace(/^http:\/\//i, 'ws://').replace(/^https:\/\//i, 'wss://')
}

export class SirannonClient {
  private readonly baseUrl: string
  private readonly wsBaseUrl: string
  private readonly transport: 'websocket' | 'http'
  private readonly headers: Record<string, string> | undefined
  private readonly autoReconnect: boolean
  private readonly reconnectInterval: number
  private readonly databases = new Map<string, RemoteDatabase>()
  private closed = false

  private readonly topologyEnabled: boolean
  private readonly primaryUrl: string | undefined
  private readonly replicaUrls: string[]
  private readonly readPreference: 'primary' | 'replica' | 'nearest'
  private latencies: EndpointLatency[] = []
  private latencyMeasuredAt = 0
  private latencyMeasuring: Promise<void> | null = null
  private readonly LATENCY_TTL_MS = 60_000
  private removedReplicas = new Set<string>()

  constructor(url: string, options?: ClientOptions)
  constructor(options: TopologyAwareClientOptions)
  constructor(urlOrOpts: string | TopologyAwareClientOptions, options?: ClientOptions) {
    if (isTopologyConfig(urlOrOpts)) {
      const topoOpts = urlOrOpts
      this.topologyEnabled = true
      this.primaryUrl = topoOpts.primary ? toBaseUrl(topoOpts.primary) : undefined
      this.replicaUrls = (topoOpts.replicas ?? []).map(toBaseUrl)
      this.readPreference = topoOpts.readPreference ?? 'primary'

      this.baseUrl = this.primaryUrl ?? this.replicaUrls[0] ?? ''
      this.wsBaseUrl = toWsUrl(this.baseUrl)
      this.transport = topoOpts.transport ?? 'websocket'
      this.headers = topoOpts.headers
      this.autoReconnect = topoOpts.autoReconnect ?? true
      this.reconnectInterval = topoOpts.reconnectInterval ?? 1000
    } else {
      this.topologyEnabled = false
      this.primaryUrl = undefined
      this.replicaUrls = []
      this.readPreference = 'primary'

      this.baseUrl = toBaseUrl(urlOrOpts)
      this.wsBaseUrl = toWsUrl(this.baseUrl)
      this.transport = options?.transport ?? 'websocket'
      this.headers = options?.headers
      this.autoReconnect = options?.autoReconnect ?? true
      this.reconnectInterval = options?.reconnectInterval ?? 1000
    }
  }

  database(id: string): RemoteDatabase {
    if (this.closed) {
      throw new Error('Client is closed')
    }

    const existing = this.databases.get(id)
    if (existing) {
      return existing
    }

    const transport = this.createTransport(id)
    const db = new RemoteDatabase(id, transport, () => {
      this.databases.delete(id)
    })
    this.databases.set(id, db)
    return db
  }

  close(): void {
    this.closed = true
    const openDatabases = [...this.databases.values()]
    this.databases.clear()
    for (const db of openDatabases) {
      db.close()
    }
  }

  private createTransport(databaseId: string): Transport {
    if (!this.topologyEnabled) {
      return this.createTransportForUrl(this.baseUrl, this.wsBaseUrl, databaseId)
    }

    return new TopologyAwareTransport(databaseId, this)
  }

  private createTransportForUrl(baseUrl: string, wsBaseUrl: string, databaseId: string): Transport {
    const encodedId = encodeURIComponent(databaseId)

    if (this.transport === 'http') {
      return new HttpTransport(`${baseUrl}/db/${encodedId}`, this.headers)
    }

    return new WebSocketTransport(`${wsBaseUrl}/db/${encodedId}`, {
      autoReconnect: this.autoReconnect,
      reconnectInterval: this.reconnectInterval,
    })
  }

  _createTransportForEndpoint(url: string, databaseId: string): Transport {
    const base = toBaseUrl(url)
    const ws = toWsUrl(base)
    return this.createTransportForUrl(base, ws, databaseId)
  }

  async _getReadEndpoint(): Promise<string> {
    if (this.readPreference === 'primary') {
      return this.primaryUrl ?? this.baseUrl
    }

    const availableReplicas = this.replicaUrls.filter(r => !this.removedReplicas.has(r))

    if (this.readPreference === 'replica') {
      if (availableReplicas.length === 0) {
        return this.primaryUrl ?? this.baseUrl
      }
      const idx = Math.floor(Math.random() * availableReplicas.length)
      return availableReplicas[idx]
    }

    if (this.readPreference === 'nearest') {
      await this.ensureLatencyMeasured()
      const reachable = this.latencies.filter(l => l.reachable && !this.removedReplicas.has(l.url))
      if (reachable.length === 0) {
        return this.primaryUrl ?? this.baseUrl
      }
      reachable.sort((a, b) => a.latencyMs - b.latencyMs)
      return reachable[0].url
    }

    return this.primaryUrl ?? this.baseUrl
  }

  _getWriteEndpoint(): string {
    return this.primaryUrl ?? this.baseUrl
  }

  _removeReplica(url: string): void {
    this.removedReplicas.add(url)
  }

  private async ensureLatencyMeasured(): Promise<void> {
    const now = Date.now()
    if (this.latencies.length > 0 && now - this.latencyMeasuredAt < this.LATENCY_TTL_MS) {
      return
    }

    if (this.latencyMeasuring) {
      await this.latencyMeasuring
      return
    }

    this.latencyMeasuring = this.measureLatencies()
    try {
      await this.latencyMeasuring
    } finally {
      this.latencyMeasuring = null
    }
  }

  private async measureLatencies(): Promise<void> {
    const allEndpoints: string[] = []
    if (this.primaryUrl) {
      allEndpoints.push(this.primaryUrl)
    }
    for (const r of this.replicaUrls) {
      allEndpoints.push(r)
    }

    const results = await Promise.all(
      allEndpoints.map(async (url): Promise<EndpointLatency> => {
        const start = performance.now()
        try {
          const controller = new AbortController()
          const timeout = setTimeout(() => controller.abort(), 5_000)
          timeout.unref()
          await fetch(`${url}/health`, { signal: controller.signal })
          clearTimeout(timeout)
          return { url, latencyMs: performance.now() - start, reachable: true }
        } catch {
          return { url, latencyMs: Number.MAX_SAFE_INTEGER, reachable: false }
        }
      }),
    )

    this.latencies = results
    this.latencyMeasuredAt = Date.now()
  }
}

import type { ChangeEvent, Params } from '../core/types.js'
import type { ExecuteResponse, QueryResponse, TransactionResponse } from '../server/protocol.js'
import type { RemoteSubscription } from './types.js'

class TopologyAwareTransport implements Transport {
  private readonly databaseId: string
  private readonly client: SirannonClient
  private closed = false

  private readTransport: Transport | null = null
  private writeTransport: Transport | null = null
  private currentReadUrl = ''
  private currentWriteUrl = ''

  constructor(databaseId: string, client: SirannonClient) {
    this.databaseId = databaseId
    this.client = client
  }

  async query(sql: string, params?: Params): Promise<QueryResponse> {
    const transport = await this.getReadTransport()
    try {
      return await transport.query(sql, params)
    } catch (err) {
      if (this.currentReadUrl && this.currentReadUrl !== this.client._getWriteEndpoint()) {
        this.client._removeReplica(this.currentReadUrl)
        this.readTransport = null
        this.currentReadUrl = ''
        const fallback = await this.getReadTransport()
        return fallback.query(sql, params)
      }
      throw err
    }
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    const transport = this.getWriteTransport()
    return transport.execute(sql, params)
  }

  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse> {
    const transport = this.getWriteTransport()
    return transport.transaction(statements)
  }

  async subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
  ): Promise<RemoteSubscription> {
    const transport = await this.getReadTransport()
    return transport.subscribe(table, filter, callback)
  }

  close(): void {
    this.closed = true
    const sameTransport = this.readTransport !== null && this.readTransport === this.writeTransport
    if (this.readTransport) {
      this.readTransport.close()
      this.readTransport = null
    }
    if (this.writeTransport && !sameTransport) {
      this.writeTransport.close()
    }
    this.writeTransport = null
  }

  private async getReadTransport(): Promise<Transport> {
    if (this.closed) {
      const { RemoteError } = await import('./types.js')
      throw new RemoteError('TRANSPORT_ERROR', 'Transport is closed')
    }

    const endpoint = await this.client._getReadEndpoint()
    if (this.readTransport && this.currentReadUrl === endpoint) {
      return this.readTransport
    }

    if (this.readTransport) {
      this.readTransport.close()
    }

    this.currentReadUrl = endpoint
    this.readTransport = this.client._createTransportForEndpoint(endpoint, this.databaseId)
    return this.readTransport
  }

  private getWriteTransport(): Transport {
    if (this.closed) {
      throw new Error('Transport is closed')
    }

    const endpoint = this.client._getWriteEndpoint()
    if (this.writeTransport && this.currentWriteUrl === endpoint) {
      return this.writeTransport
    }

    if (this.writeTransport) {
      this.writeTransport.close()
    }

    this.currentWriteUrl = endpoint
    this.writeTransport = this.client._createTransportForEndpoint(endpoint, this.databaseId)
    return this.writeTransport
  }
}
