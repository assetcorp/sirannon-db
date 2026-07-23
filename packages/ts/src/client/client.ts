import type { ClientOptions, ReadConcernLevel } from '../core/types.js'
import type { ClusterStatusResponse } from '../server/protocol.js'
import type { ClusterRoutingState, TopologyRouting } from './cluster-routing.js'
import { clusterRoutingChanged, parseClusterRouting } from './cluster-routing.js'
import { RemoteDatabase } from './database-proxy.js'
import { toBaseUrl, toServerBaseUrl, toWsUrl } from './endpoint-urls.js'
import { TopologyAwareTransport } from './topology-transport.js'
import { HttpTransport } from './transport/http.js'
import { WebSocketTransport } from './transport/ws.js'
import { RemoteError, type Transport } from './types.js'

export interface TopologyAwareClientOptions extends ClientOptions {
  endpoints?: string[]
  primary?: string
  replicas?: string[]
  readPreference?: 'primary' | 'replica' | 'nearest'
  discovery?: 'static' | 'coordinator'
  readConcern?: ReadConcernLevel
}

interface EndpointLatency {
  url: string
  latencyMs: number
  reachable: boolean
}

const CLUSTER_DISCOVERY_FETCH_TIMEOUT_MS = 2_000

function isTopologyConfig(urlOrOpts: string | TopologyAwareClientOptions): urlOrOpts is TopologyAwareClientOptions {
  if (typeof urlOrOpts !== 'object') {
    return false
  }
  return (
    'primary' in urlOrOpts ||
    ('replicas' in urlOrOpts && Array.isArray(urlOrOpts.replicas) && urlOrOpts.replicas.length > 0) ||
    ('endpoints' in urlOrOpts && Array.isArray(urlOrOpts.endpoints) && urlOrOpts.endpoints.length > 0) ||
    urlOrOpts.discovery === 'coordinator'
  )
}

export class SirannonClient implements TopologyRouting {
  private readonly baseUrl: string
  private readonly wsBaseUrl: string
  private readonly transport: 'websocket' | 'http'
  private readonly headers: Record<string, string> | undefined
  private readonly webSocketProtocols: string | string[] | undefined
  private readonly autoReconnect: boolean
  private readonly reconnectInterval: number
  private readonly requestTimeout: number | undefined
  private readonly databases = new Map<string, RemoteDatabase>()
  private closed = false

  private readonly topologyEnabled: boolean
  private readonly primaryUrl: string | undefined
  private readonly replicaUrls: string[]
  private readonly readPreference: 'primary' | 'replica' | 'nearest'
  private readonly discovery: 'static' | 'coordinator'
  private readonly readConcern: ReadConcernLevel | undefined
  private readonly starterEndpoints: string[]
  private readonly clusterRouting = new Map<string, ClusterRoutingState>()
  private readonly topologyTransports = new Map<string, TopologyAwareTransport>()
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
      this.discovery = topoOpts.discovery ?? 'static'
      this.readConcern = topoOpts.readConcern
      this.starterEndpoints = (topoOpts.endpoints ?? []).map(toBaseUrl)

      this.baseUrl = this.primaryUrl ?? this.replicaUrls[0] ?? this.starterEndpoints[0] ?? ''
      this.wsBaseUrl = toWsUrl(this.baseUrl)
      this.transport = topoOpts.transport ?? 'websocket'
      this.headers = topoOpts.headers
      this.webSocketProtocols = topoOpts.webSocketProtocols
      this.autoReconnect = topoOpts.autoReconnect ?? true
      this.reconnectInterval = topoOpts.reconnectInterval ?? 1000
      this.requestTimeout = topoOpts.requestTimeout
    } else {
      this.topologyEnabled = false
      this.primaryUrl = undefined
      this.replicaUrls = []
      this.readPreference = 'primary'
      this.discovery = 'static'
      this.readConcern = undefined
      this.starterEndpoints = []

      this.baseUrl = toBaseUrl(urlOrOpts)
      this.wsBaseUrl = toWsUrl(this.baseUrl)
      this.transport = options?.transport ?? 'websocket'
      this.headers = options?.headers
      this.webSocketProtocols = options?.webSocketProtocols
      this.autoReconnect = options?.autoReconnect ?? true
      this.reconnectInterval = options?.reconnectInterval ?? 1000
      this.requestTimeout = options?.requestTimeout
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

    const transport = new TopologyAwareTransport(databaseId, this, closing =>
      this._unregisterTopologyTransport(databaseId, closing),
    )
    this.topologyTransports.set(databaseId, transport)
    return transport
  }

  private createTransportForUrl(baseUrl: string, wsBaseUrl: string, databaseId: string): Transport {
    const encodedId = encodeURIComponent(databaseId)

    if (this.transport === 'http') {
      return new HttpTransport(`${baseUrl}/db/${encodedId}`, this.headers)
    }

    return new WebSocketTransport(`${wsBaseUrl}/db/${encodedId}`, {
      autoReconnect: this.autoReconnect,
      reconnectInterval: this.reconnectInterval,
      protocols: this.webSocketProtocols,
      requestTimeout: this.requestTimeout,
    })
  }

  _createTransportForEndpoint(url: string, databaseId: string): Transport {
    const base = toBaseUrl(url)
    const ws = toWsUrl(base)
    return this.createTransportForUrl(base, ws, databaseId)
  }

  async _getReadEndpoint(databaseId?: string, readConcern?: ReadConcernLevel): Promise<string> {
    if (this.discovery === 'coordinator' && databaseId) {
      const routing = await this.ensureClusterRouting(databaseId)
      const concern = readConcern ?? this.readConcern ?? 'majority'
      if (concern === 'linearizable') {
        if (routing.currentPrimary) return routing.currentPrimary
        throw new RemoteError('NO_SAFE_PRIMARY', 'No current primary is available for linearizable reads')
      }
      const readable = routing.readEndpoints.filter(endpoint => endpoint.readConcerns.includes(concern))
      const preferredReadable =
        this.readPreference === 'replica' && routing.currentPrimary
          ? readable.filter(endpoint => endpoint.url !== routing.currentPrimary)
          : readable
      if (this.readPreference !== 'primary' && preferredReadable.length > 0) {
        if (this.readPreference === 'nearest') {
          return preferredReadable[0].url
        }
        const idx = Math.floor(Math.random() * preferredReadable.length)
        return preferredReadable[idx].url
      }
      if (routing.currentPrimary) return routing.currentPrimary
      const localReadable = routing.readEndpoints.find(endpoint => endpoint.readConcerns.includes('local'))
      if (localReadable) return localReadable.url
      throw new RemoteError('ROUTING_ERROR', 'No usable read endpoint is available')
    }

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

  async _getWriteEndpoint(databaseId?: string): Promise<string> {
    if (this.discovery === 'coordinator' && databaseId) {
      const routing = await this.ensureClusterRouting(databaseId)
      if (!routing.currentPrimary) {
        throw new RemoteError('NO_SAFE_PRIMARY', 'No current primary is available')
      }
      return routing.currentPrimary
    }
    return this.primaryUrl ?? this.baseUrl
  }

  _getReadConcern(): ReadConcernLevel | undefined {
    return this.readConcern
  }

  _usesCoordinatorDiscovery(): boolean {
    return this.discovery === 'coordinator'
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
        const controller = new AbortController()
        const timeout = setTimeout(() => controller.abort(), 5_000)
        const unrefable = timeout as unknown as { unref?: () => void }
        if (typeof unrefable.unref === 'function') {
          unrefable.unref()
        }
        try {
          const init: RequestInit = { signal: controller.signal as RequestInit['signal'] }
          const response = await fetch(`${url}/health`, init)
          if (!response.ok) {
            return { url, latencyMs: Number.MAX_SAFE_INTEGER, reachable: false }
          }
          return { url, latencyMs: performance.now() - start, reachable: true }
        } catch {
          return { url, latencyMs: Number.MAX_SAFE_INTEGER, reachable: false }
        } finally {
          clearTimeout(timeout)
        }
      }),
    )

    this.latencies = results
    this.latencyMeasuredAt = Date.now()
  }

  async _refreshClusterRouting(databaseId: string): Promise<void> {
    const candidates = this.clusterDiscoveryCandidates(databaseId)
    const encodedId = encodeURIComponent(databaseId)
    for (const endpoint of candidates) {
      const base = toServerBaseUrl(endpoint, databaseId)
      const controller = new AbortController()
      const timeout = setTimeout(() => controller.abort(), CLUSTER_DISCOVERY_FETCH_TIMEOUT_MS)
      const unrefable = timeout as unknown as { unref?: () => void }
      unrefable.unref?.()
      let next: ClusterRoutingState | null = null
      try {
        const response = await fetch(`${base}/db/${encodedId}/cluster`, {
          headers: this.headers,
          signal: controller.signal,
        })
        if (!response.ok) {
          continue
        }
        const data = (await response.json()) as ClusterStatusResponse
        next = parseClusterRouting(data, databaseId)
      } catch (err) {
        if (err instanceof RemoteError && err.code === 'INVALID_RESPONSE') {
          throw err
        }
      } finally {
        clearTimeout(timeout)
      }
      if (!next) {
        continue
      }
      const previous = this.clusterRouting.get(databaseId)
      this.clusterRouting.set(databaseId, next)
      if (clusterRoutingChanged(previous, next)) {
        try {
          await this.notifyClusterRoutingChanged(databaseId)
        } catch (err) {
          if (previous) {
            this.clusterRouting.set(databaseId, previous)
          } else {
            this.clusterRouting.delete(databaseId)
          }
          throw err
        }
      }
      return
    }
    throw new RemoteError('ROUTING_ERROR', `Could not discover cluster routing for database '${databaseId}'`)
  }

  private async ensureClusterRouting(databaseId: string): Promise<ClusterRoutingState> {
    const existing = this.clusterRouting.get(databaseId)
    if (existing) return existing
    await this._refreshClusterRouting(databaseId)
    const refreshed = this.clusterRouting.get(databaseId)
    if (!refreshed) {
      throw new RemoteError('ROUTING_ERROR', `Could not discover cluster routing for database '${databaseId}'`)
    }
    return refreshed
  }

  private clusterDiscoveryCandidates(databaseId: string): string[] {
    const candidates = new Set<string>()
    for (const endpoint of this.starterEndpoints) candidates.add(endpoint)
    if (this.primaryUrl) candidates.add(this.primaryUrl)
    for (const endpoint of this.replicaUrls) candidates.add(endpoint)
    const existing = this.clusterRouting.get(databaseId)
    if (existing?.currentPrimary) candidates.add(existing.currentPrimary)
    for (const endpoint of existing?.readEndpoints ?? []) candidates.add(endpoint.url)
    if (this.baseUrl) candidates.add(this.baseUrl)
    return [...candidates]
  }

  private _unregisterTopologyTransport(databaseId: string, transport: TopologyAwareTransport): void {
    if (this.topologyTransports.get(databaseId) === transport) {
      this.topologyTransports.delete(databaseId)
    }
  }

  private async notifyClusterRoutingChanged(databaseId: string): Promise<void> {
    const transport = this.topologyTransports.get(databaseId)
    if (!transport) return
    await transport._handleClusterRoutingChanged()
  }
}
