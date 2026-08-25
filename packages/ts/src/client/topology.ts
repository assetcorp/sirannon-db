import type { ClientOptions, ReadConcernLevel } from '../core/types.js'
import type { ClusterStatusResponse } from '../server/protocol.js'
import { createEndpointTransport, DatabaseClient } from './client-base.js'
import type { ClusterRoutingState, TopologyRouting } from './cluster-routing.js'
import { clusterRoutingChanged, parseClusterRouting } from './cluster-routing.js'

export { parseClusterStatus } from './cluster-status.js'

import { toBaseUrl, toServerBaseUrl } from './endpoint-urls.js'
import { unrefTimer } from './http-json.js'
import { TopologyAwareTransport } from './topology-transport.js'
import { RemoteError, type Transport } from './types.js'

/**
 * @public
 *
 * Which nodes the client holds, how it finds the rest, and where it sends each read.
 */
export interface TopologyAwareClientOptions extends ClientOptions {
  /**
   * Nodes the client starts from when it discovers the group through the coordinator.
   */
  endpoints?: string[]
  /**
   * Address of the primary, for a group you list by hand.
   */
  primary?: string
  /**
   * Addresses of the replicas, for a group you list by hand.
   */
  replicas?: string[]
  /**
   * Where reads go. Default: 'primary'.
   */
  readPreference?: 'primary' | 'replica' | 'nearest'
  /**
   * Whether the client uses the nodes you listed or asks the group for them. Default: 'static'.
   */
  discovery?: 'static' | 'coordinator'
  /**
   * Currency every read requires, which the client uses to choose a node.
   */
  readConcern?: ReadConcernLevel
}

interface EndpointLatency {
  url: string
  latencyMs: number
  reachable: boolean
}

const CLUSTER_DISCOVERY_FETCH_TIMEOUT_MS = 2_000
const LATENCY_TTL_MS = 60_000
const LATENCY_PROBE_TIMEOUT_MS = 5_000

/**
 * @public
 *
 * Connects to a replication group rather than one server: it routes each read to a node that meets its read concern, and each write to the primary.
 */
export class TopologyAwareClient extends DatabaseClient implements TopologyRouting {
  private readonly baseUrl: string
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
  private readonly removedReplicas = new Set<string>()

  constructor(options: TopologyAwareClientOptions) {
    super(options)
    this.primaryUrl = options.primary ? toBaseUrl(options.primary) : undefined
    this.replicaUrls = (options.replicas ?? []).map(toBaseUrl)
    this.readPreference = options.readPreference ?? 'primary'
    this.discovery = options.discovery ?? 'static'
    this.readConcern = options.readConcern
    this.starterEndpoints = (options.endpoints ?? []).map(toBaseUrl)
    this.baseUrl = this.primaryUrl ?? this.replicaUrls[0] ?? this.starterEndpoints[0] ?? ''
  }

  /**
   * Builds the routing transport a database's requests travel over.
   *
   * @param databaseId - Identifier of the database.
   * @returns A transport that picks a node per request.
   */
  protected createTransport(databaseId: string): Transport {
    const transport = new TopologyAwareTransport(databaseId, this, closing =>
      this.unregisterTopologyTransport(databaseId, closing),
    )
    this.topologyTransports.set(databaseId, transport)
    return transport
  }

  /** @internal */
  _createTransportForEndpoint(url: string, databaseId: string): Transport {
    return createEndpointTransport(this.settings, url, databaseId)
  }

  /**
   * Returns the address writes are sent to, which is the current primary.
   *
   * @param databaseId - Identifier of the database.
   * @returns Address of the node that accepts writes.
   */
  protected async resolveServerUrl(databaseId: string): Promise<string> {
    return this._getWriteEndpoint(databaseId)
  }

  /** @internal */
  async _getReadEndpoint(databaseId?: string, readConcern?: ReadConcernLevel): Promise<string> {
    if (this.discovery === 'coordinator' && databaseId) {
      return this.routedReadEndpoint(databaseId, readConcern)
    }

    if (this.readPreference === 'primary') {
      return this.primaryUrl ?? this.baseUrl
    }

    const availableReplicas = this.replicaUrls.filter(url => !this.removedReplicas.has(url))

    if (this.readPreference === 'replica') {
      if (availableReplicas.length === 0) {
        return this.primaryUrl ?? this.baseUrl
      }
      return availableReplicas[Math.floor(Math.random() * availableReplicas.length)]
    }

    await this.ensureLatencyMeasured()
    const reachable = this.latencies.filter(entry => entry.reachable && !this.removedReplicas.has(entry.url))
    if (reachable.length === 0) {
      return this.primaryUrl ?? this.baseUrl
    }
    reachable.sort((left, right) => left.latencyMs - right.latencyMs)
    return reachable[0].url
  }

  private async routedReadEndpoint(databaseId: string, readConcern?: ReadConcernLevel): Promise<string> {
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
      return preferredReadable[Math.floor(Math.random() * preferredReadable.length)].url
    }
    if (routing.currentPrimary) return routing.currentPrimary

    const localReadable = routing.readEndpoints.find(endpoint => endpoint.readConcerns.includes('local'))
    if (localReadable) return localReadable.url
    throw new RemoteError('ROUTING_ERROR', 'No usable read endpoint is available')
  }

  /** @internal */
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

  /** @internal */
  _getReadConcern(): ReadConcernLevel | undefined {
    return this.readConcern
  }

  /** @internal */
  _usesCoordinatorDiscovery(): boolean {
    return this.discovery === 'coordinator'
  }

  /** @internal */
  _removeReplica(url: string): void {
    this.removedReplicas.add(url)
  }

  /** @internal */
  async _refreshClusterRouting(databaseId: string): Promise<void> {
    const encodedId = encodeURIComponent(databaseId)
    for (const endpoint of this.clusterDiscoveryCandidates(databaseId)) {
      const next = await this.fetchClusterRouting(toServerBaseUrl(endpoint, databaseId), encodedId, databaseId)
      if (!next) continue

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

  private async fetchClusterRouting(
    base: string,
    encodedId: string,
    databaseId: string,
  ): Promise<ClusterRoutingState | null> {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), CLUSTER_DISCOVERY_FETCH_TIMEOUT_MS)
    unrefTimer(timeout)
    try {
      const response = await fetch(`${base}/db/${encodedId}/cluster`, {
        headers: this.settings.headers,
        signal: controller.signal,
      })
      if (!response.ok) return null
      return parseClusterRouting((await response.json()) as ClusterStatusResponse, databaseId)
    } catch (err) {
      if (err instanceof RemoteError && err.code === 'INVALID_RESPONSE') {
        throw err
      }
      return null
    } finally {
      clearTimeout(timeout)
    }
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

  private async ensureLatencyMeasured(): Promise<void> {
    if (this.latencies.length > 0 && Date.now() - this.latencyMeasuredAt < LATENCY_TTL_MS) {
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
    for (const replica of this.replicaUrls) {
      allEndpoints.push(replica)
    }

    this.latencies = await Promise.all(allEndpoints.map(url => measureEndpointLatency(url)))
    this.latencyMeasuredAt = Date.now()
  }

  private unregisterTopologyTransport(databaseId: string, transport: TopologyAwareTransport): void {
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

async function measureEndpointLatency(url: string): Promise<EndpointLatency> {
  const start = performance.now()
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), LATENCY_PROBE_TIMEOUT_MS)
  unrefTimer(timeout)
  try {
    const response = await fetch(`${url}/health`, { signal: controller.signal as RequestInit['signal'] })
    if (!response.ok) {
      return { url, latencyMs: Number.MAX_SAFE_INTEGER, reachable: false }
    }
    return { url, latencyMs: performance.now() - start, reachable: true }
  } catch {
    return { url, latencyMs: Number.MAX_SAFE_INTEGER, reachable: false }
  } finally {
    clearTimeout(timeout)
  }
}
