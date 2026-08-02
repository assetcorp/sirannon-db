import type { ReadConcernLevel } from '../core/types.js'
import { toServerBaseUrl } from './endpoint-urls.js'
import { RemoteError, type Transport } from './types.js'

export interface ClusterRoutingState {
  currentPrimary: string | null
  primaryTerm: string | null
  readEndpoints: Array<{ url: string; readConcerns: ReadConcernLevel[] }>
}

export interface TopologyRouting {
  _getReadEndpoint(databaseId?: string, readConcern?: ReadConcernLevel): Promise<string>
  _getWriteEndpoint(databaseId?: string): Promise<string>
  _getReadConcern(): ReadConcernLevel | undefined
  _usesCoordinatorDiscovery(): boolean
  _removeReplica(url: string): void
  _refreshClusterRouting(databaseId: string): Promise<void>
  _createTransportForEndpoint(url: string, databaseId: string): Transport
}

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

export function clusterRoutingChanged(previous: ClusterRoutingState | undefined, next: ClusterRoutingState): boolean {
  return previous === undefined || clusterRoutingFingerprint(previous) !== clusterRoutingFingerprint(next)
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

export function parseClusterRouting(data: unknown, databaseId: string): ClusterRoutingState {
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

export function shouldRefreshRouting(err: unknown): boolean {
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
