import type { ClientOptions, ReadConcernLevel } from '../core/types.js'
import type { ClusterStatusResponse } from '../server/protocol.js'
import { RemoteDatabase } from './database-proxy.js'
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

interface ClusterRoutingState {
  currentPrimary: string | null
  primaryTerm: string | null
  readEndpoints: Array<{ url: string; readConcerns: ReadConcernLevel[] }>
}

const CLUSTER_DISCOVERY_FETCH_TIMEOUT_MS = 2_000

function clusterRoutingFingerprint(state: ClusterRoutingState): string {
  const readEndpoints = state.readEndpoints
    .map(endpoint => ({
      url: endpoint.url,
      readConcerns: [...endpoint.readConcerns].sort(),
    }))
    .sort((left, right) => left.url.localeCompare(right.url))
  return JSON.stringify({
    currentPrimary: state.currentPrimary,
    primaryTerm: state.primaryTerm,
    readEndpoints,
  })
}

function clusterRoutingChanged(previous: ClusterRoutingState | undefined, next: ClusterRoutingState): boolean {
  return previous === undefined || clusterRoutingFingerprint(previous) !== clusterRoutingFingerprint(next)
}

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

function toBaseUrl(url: string): string {
  return normaliseEndpointUrl(url)
}

function toServerBaseUrl(url: string, databaseId?: string): string {
  const base = toBaseUrl(url)
  if (!databaseId) return base.replace(/\/db\/[^/]+$/i, '')
  return base.replace(new RegExp(`/db/${escapeRegExp(encodeURIComponent(databaseId))}$`, 'i'), '')
}

function toWsUrl(baseUrl: string): string {
  return baseUrl.replace(/^http:\/\//i, 'ws://').replace(/^https:\/\//i, 'wss://')
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function normaliseEndpointUrl(rawUrl: string): string {
  let parsed: URL
  try {
    parsed = new URL(rawUrl)
  } catch {
    throw new TypeError(`Endpoint URL '${rawUrl}' is invalid`)
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new TypeError(`Endpoint URL '${rawUrl}' must use http or https`)
  }
  if (parsed.username || parsed.password) {
    throw new TypeError(`Endpoint URL '${rawUrl}' must not contain credentials`)
  }
  if (parsed.hash) {
    throw new TypeError(`Endpoint URL '${rawUrl}' must not contain a fragment`)
  }
  if (parsed.search) {
    throw new TypeError(`Endpoint URL '${rawUrl}' must not contain a query string`)
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, '')
  return parsed.toString().replace(/\/$/, '')
}

function isReadConcernLevel(value: unknown): value is ReadConcernLevel {
  return value === 'local' || value === 'majority' || value === 'linearizable'
}

function parseDiscoveredReadConcerns(value: unknown): ReadConcernLevel[] {
  if (!Array.isArray(value)) {
    throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata readConcerns must be an array')
  }
  const concerns: ReadConcernLevel[] = []
  for (const concern of value) {
    if (!isReadConcernLevel(concern)) {
      throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata contains an invalid read concern')
    }
    if (!concerns.includes(concern)) {
      concerns.push(concern)
    }
  }
  return concerns
}

function toDiscoveredServerBaseUrl(endpoint: string, databaseId: string): string {
  try {
    return toServerBaseUrl(endpoint, databaseId)
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    throw new RemoteError('INVALID_RESPONSE', `Cluster metadata contains an unsafe endpoint: ${message}`)
  }
}

function parseClusterRouting(data: unknown, databaseId: string): ClusterRoutingState {
  if (typeof data !== 'object' || data === null || Array.isArray(data)) {
    throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata must be an object')
  }
  const record = data as Record<string, unknown>
  if (record.databaseId !== databaseId) {
    throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata database id does not match the request')
  }
  if (record.primaryTerm !== undefined && typeof record.primaryTerm !== 'string') {
    throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata primaryTerm must be a string')
  }

  let currentPrimary: string | null = null
  if (record.currentPrimary !== undefined && record.currentPrimary !== null) {
    if (typeof record.currentPrimary !== 'object' || Array.isArray(record.currentPrimary)) {
      throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata currentPrimary must be an object or null')
    }
    const primary = record.currentPrimary as Record<string, unknown>
    if (typeof primary.endpoint !== 'string') {
      throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata currentPrimary.endpoint must be a string')
    }
    currentPrimary = toDiscoveredServerBaseUrl(primary.endpoint, databaseId)
  }

  const readEndpointsRaw = record.readEndpoints
  if (readEndpointsRaw !== undefined && !Array.isArray(readEndpointsRaw)) {
    throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata readEndpoints must be an array')
  }

  const readEndpoints = (readEndpointsRaw ?? []).map(endpointInfo => {
    if (typeof endpointInfo !== 'object' || endpointInfo === null || Array.isArray(endpointInfo)) {
      throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata read endpoint must be an object')
    }
    const endpoint = endpointInfo as Record<string, unknown>
    if (typeof endpoint.endpoint !== 'string') {
      throw new RemoteError('INVALID_RESPONSE', 'Cluster metadata read endpoint URL must be a string')
    }
    return {
      url: toDiscoveredServerBaseUrl(endpoint.endpoint, databaseId),
      readConcerns: parseDiscoveredReadConcerns(endpoint.readConcerns),
    }
  })

  return {
    currentPrimary,
    primaryTerm: record.primaryTerm ?? null,
    readEndpoints,
  }
}

export class SirannonClient {
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

    const transport = new TopologyAwareTransport(databaseId, this)
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

  _unregisterTopologyTransport(databaseId: string, transport: TopologyAwareTransport): void {
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

import type { BulkLoadDurability, ChangeEvent, Params, WriteConcern } from '../core/types.js'
import type {
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from '../server/protocol.js'
import type { RemoteSubscription } from './types.js'

interface TrackedRemoteSubscription {
  id: number
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
  remote: RemoteSubscription | null
  active: boolean
}

class TopologyAwareTransport implements Transport {
  private readonly databaseId: string
  private readonly client: SirannonClient
  private closed = false

  private readTransport: Transport | null = null
  private writeTransport: Transport | null = null
  private subscriptionTransport: Transport | null = null
  private readTransportRequest: Promise<Transport> | null = null
  private writeTransportRequest: Promise<Transport> | null = null
  private subscriptionTransportRequest: Promise<Transport> | null = null
  private subscriptionOperation: Promise<void> = Promise.resolve()
  private activeSubscriptions = new Map<number, TrackedRemoteSubscription>()
  private nextSubscriptionId = 0
  private currentReadUrl = ''
  private currentWriteUrl = ''
  private currentSubscriptionUrl = ''

  constructor(databaseId: string, client: SirannonClient) {
    this.databaseId = databaseId
    this.client = client
  }

  async query(sql: string, params?: Params): Promise<QueryResponse> {
    const readConcern = this.client._getReadConcern()
    const transport = await this.getReadTransport(readConcern)
    const endpointUsed = this.currentReadUrl
    try {
      return await transport.query(sql, params, readConcern ? { level: readConcern } : undefined)
    } catch (err) {
      if (this.client._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.client._refreshClusterRouting(this.databaseId)
        this.readTransport = null
        this.currentReadUrl = ''
        const refreshed = await this.getReadTransport(readConcern)
        return refreshed.query(sql, params, readConcern ? { level: readConcern } : undefined)
      }
      const isTransportError =
        err instanceof Error && (err.name !== 'RemoteError' || (err as { code?: string }).code === 'CONNECTION_ERROR')
      const writeEndpoint = await this.client._getWriteEndpoint(this.databaseId)
      if (isTransportError && endpointUsed && endpointUsed !== writeEndpoint) {
        this.client._removeReplica(endpointUsed)
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
    const transport = await this.getWriteTransport()
    try {
      return await transport.execute(sql, params)
    } catch (err) {
      if (this.client._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.client._refreshClusterRouting(this.databaseId)
        this.writeTransport = null
        this.currentWriteUrl = ''
      }
      throw err
    }
  }

  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse> {
    const transport = await this.getWriteTransport()
    try {
      return await transport.transaction(statements)
    } catch (err) {
      if (this.client._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.client._refreshClusterRouting(this.databaseId)
        this.writeTransport = null
        this.currentWriteUrl = ''
      }
      throw err
    }
  }

  async batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse> {
    const transport = await this.getWriteTransport()
    try {
      return await transport.batch(sql, paramsBatch, writeConcern)
    } catch (err) {
      if (this.client._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.client._refreshClusterRouting(this.databaseId)
        this.writeTransport = null
        this.currentWriteUrl = ''
      }
      throw err
    }
  }

  async load(sql: string, paramsBatch: Params[], durability?: BulkLoadDurability): Promise<LoadResponse> {
    const transport = await this.getWriteTransport()
    try {
      return await transport.load(sql, paramsBatch, durability)
    } catch (err) {
      if (this.client._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        await this.client._refreshClusterRouting(this.databaseId)
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
  ): Promise<RemoteSubscription> {
    try {
      return await this.subscribeOnCurrentEndpoint(table, filter, callback)
    } catch (err) {
      if (this.client._usesCoordinatorDiscovery() && shouldRefreshRouting(err)) {
        const hadActiveSubscriptions = this.activeSubscriptions.size > 0
        if (!hadActiveSubscriptions) {
          this.closeSubscriptionTransport()
        }
        await this.client._refreshClusterRouting(this.databaseId)
        if (!hadActiveSubscriptions) {
          this.closeSubscriptionTransport()
        }
        return this.subscribeOnCurrentEndpoint(table, filter, callback)
      }
      throw err
    }
  }

  async _handleClusterRoutingChanged(): Promise<void> {
    if (!this.client._usesCoordinatorDiscovery()) return
    if (this.activeSubscriptions.size === 0) return
    await this.withSubscriptionOperation(() => this.migrateSubscriptionsToCurrentEndpoint())
  }

  close(): void {
    this.closed = true
    const sameTransport = this.readTransport !== null && this.readTransport === this.writeTransport
    const subscriptionIsRead = this.subscriptionTransport !== null && this.subscriptionTransport === this.readTransport
    const subscriptionIsWrite =
      this.subscriptionTransport !== null && this.subscriptionTransport === this.writeTransport
    for (const subscription of this.activeSubscriptions.values()) {
      subscription.active = false
      subscription.remote = null
    }
    this.activeSubscriptions.clear()
    if (this.readTransport) {
      this.readTransport.close()
      this.readTransport = null
    }
    if (this.writeTransport && !sameTransport) {
      this.writeTransport.close()
    }
    this.writeTransport = null
    if (this.subscriptionTransport && !subscriptionIsRead && !subscriptionIsWrite) {
      this.subscriptionTransport.close()
    }
    this.subscriptionTransport = null
    this.currentSubscriptionUrl = ''
    this.client._unregisterTopologyTransport(this.databaseId, this)
  }

  private async subscribeOnCurrentEndpoint(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
  ): Promise<RemoteSubscription> {
    return this.withSubscriptionOperation(async () => {
      const transport = await this.getSubscriptionTransport(this.client._getReadConcern())
      const remote = await transport.subscribe(table, filter, callback)
      const subscription: TrackedRemoteSubscription = {
        id: ++this.nextSubscriptionId,
        table,
        filter,
        callback,
        remote,
        active: true,
      }
      this.activeSubscriptions.set(subscription.id, subscription)
      return this.createSubscriptionHandle(subscription)
    })
  }

  private createSubscriptionHandle(subscription: TrackedRemoteSubscription): RemoteSubscription {
    return {
      unsubscribe: () => {
        if (!subscription.active) return
        subscription.active = false
        this.activeSubscriptions.delete(subscription.id)
        const remote = subscription.remote
        subscription.remote = null
        remote?.unsubscribe()
      },
    }
  }

  private async migrateSubscriptionsToCurrentEndpoint(): Promise<void> {
    this.assertOpen()
    const subscriptions = [...this.activeSubscriptions.values()].filter(subscription => subscription.active)
    if (subscriptions.length === 0) return

    const readConcern = this.client._getReadConcern()
    const endpoint = await this.client._getReadEndpoint(this.databaseId, readConcern)
    this.assertOpen()
    if (this.subscriptionTransport && this.currentSubscriptionUrl === endpoint) {
      return
    }

    const nextTransport = this.client._createTransportForEndpoint(endpoint, this.databaseId)
    const nextSubscriptions = new Map<number, RemoteSubscription>()
    try {
      for (const subscription of subscriptions) {
        if (!subscription.active) continue
        const remote = await nextTransport.subscribe(subscription.table, subscription.filter, subscription.callback)
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

    const oldTransport = this.subscriptionTransport
    this.subscriptionTransport = nextTransport
    this.currentSubscriptionUrl = endpoint

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
    const endpoint = await this.client._getReadEndpoint(this.databaseId, readConcern)
    this.assertOpen()

    if (this.readTransport && this.currentReadUrl === endpoint) {
      return this.readTransport
    }

    const nextTransport = this.client._createTransportForEndpoint(endpoint, this.databaseId)
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
    const endpoint = await this.client._getWriteEndpoint(this.databaseId)
    this.assertOpen()

    if (this.writeTransport && this.currentWriteUrl === endpoint) {
      return this.writeTransport
    }

    const nextTransport = this.client._createTransportForEndpoint(endpoint, this.databaseId)
    const oldTransport = this.writeTransport
    this.writeTransport = nextTransport
    this.currentWriteUrl = endpoint

    if (oldTransport) {
      oldTransport.close()
    }

    return this.writeTransport
  }

  private async getSubscriptionTransport(readConcern?: ReadConcernLevel): Promise<Transport> {
    this.assertOpen()

    if (this.subscriptionTransport) {
      return this.subscriptionTransport
    }

    while (this.subscriptionTransportRequest) {
      await this.subscriptionTransportRequest.catch(() => undefined)
      this.assertOpen()
      if (this.subscriptionTransport) {
        return this.subscriptionTransport
      }
    }

    const request = this.resolveSubscriptionTransport(readConcern)
    this.subscriptionTransportRequest = request
    try {
      return await request
    } finally {
      if (this.subscriptionTransportRequest === request) {
        this.subscriptionTransportRequest = null
      }
    }
  }

  private async resolveSubscriptionTransport(readConcern?: ReadConcernLevel): Promise<Transport> {
    if (this.subscriptionTransport) {
      return this.subscriptionTransport
    }

    const endpoint = await this.client._getReadEndpoint(this.databaseId, readConcern)
    this.assertOpen()

    if (this.subscriptionTransport) {
      return this.subscriptionTransport
    }

    this.subscriptionTransport = this.client._createTransportForEndpoint(endpoint, this.databaseId)
    this.currentSubscriptionUrl = endpoint
    return this.subscriptionTransport
  }

  private closeSubscriptionTransport(): void {
    if (this.subscriptionTransport) {
      this.subscriptionTransport.close()
      this.subscriptionTransport = null
    }
    this.currentSubscriptionUrl = ''
  }

  private async withSubscriptionOperation<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.subscriptionOperation
    let release = () => {}
    this.subscriptionOperation = new Promise<void>(resolve => {
      release = resolve
    })
    await previous.catch(() => undefined)
    try {
      return await operation()
    } finally {
      release()
    }
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new RemoteError('TRANSPORT_ERROR', 'Transport is closed')
    }
  }
}

function toSubscriptionRoutingError(err: unknown, databaseId: string): RemoteError {
  const detail = err instanceof Error ? err.message : String(err)
  return new RemoteError(
    'ROUTING_ERROR',
    `Could not re-establish active subscriptions on refreshed routing for database '${databaseId}': ${detail}`,
  )
}

function shouldRefreshRouting(err: unknown): boolean {
  if (!(err instanceof RemoteError)) {
    return false
  }
  return (
    err.code === 'STALE_PRIMARY' ||
    err.code === 'AUTHORITY_LOST' ||
    err.code === 'COORDINATOR_UNAVAILABLE' ||
    err.code === 'NO_SAFE_PRIMARY' ||
    err.code === 'CONNECTION_ERROR'
  )
}
