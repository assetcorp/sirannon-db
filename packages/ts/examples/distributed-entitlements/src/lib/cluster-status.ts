import { getMajorityWriteAvailability } from './cluster-readiness'
import type { ClusterNode } from './schemas'
import { clusterNodeSchema } from './schemas'
import { clusterEndpointsFromEnv, DATABASE_ID, DEFAULT_CLUSTER_TOKEN, toServerBaseUrl } from './sql'

interface ClusterStatusResponse {
  databaseId?: unknown
  role?: unknown
  currentPrimary?: unknown
  primaryTerm?: unknown
  readEndpoints?: unknown
  health?: unknown
}

export async function fetchClusterNodes(): Promise<ClusterNode[]> {
  const token = process.env.SIRANNON_CLUSTER_TOKEN ?? DEFAULT_CLUSTER_TOKEN
  const endpoints = clusterEndpointsFromEnv(process.env.SIRANNON_CLUSTER_ENDPOINTS)
  const results = await Promise.all(endpoints.map(endpoint => fetchClusterNode(endpoint, token)))
  return results.map(result => clusterNodeSchema.parse(result))
}

export async function assertMajorityWriteAvailable(): Promise<void> {
  const availability = getMajorityWriteAvailability(await fetchClusterNodes())
  if (!availability.available) {
    throw new Error(`Write blocked: ${availability.reason}`)
  }
}

async function fetchClusterNode(endpoint: string, token: string): Promise<ClusterNode> {
  const baseUrl = toServerBaseUrl(endpoint)
  const nodeId = nodeIdFromEndpoint(baseUrl)

  try {
    const response = await fetch(`${baseUrl}/db/${DATABASE_ID}/cluster`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      signal: AbortSignal.timeout(2_000),
    })
    if (!response.ok) {
      return {
        nodeId,
        endpoint: baseUrl,
        reachable: false,
        currentPrimary: null,
        primaryTerm: null,
        readEndpoints: 0,
        error: `HTTP ${response.status}`,
      }
    }

    const data = (await response.json()) as ClusterStatusResponse
    return {
      nodeId,
      endpoint: baseUrl,
      reachable: true,
      role: typeof data.role === 'string' ? data.role : undefined,
      health: parseHealth(data.health),
      currentPrimary: parseCurrentPrimary(data.currentPrimary),
      primaryTerm: data.primaryTerm === undefined || data.primaryTerm === null ? null : String(data.primaryTerm),
      readEndpoints: Array.isArray(data.readEndpoints) ? data.readEndpoints.length : 0,
      error: null,
    }
  } catch (error) {
    return {
      nodeId,
      endpoint: baseUrl,
      reachable: false,
      currentPrimary: null,
      primaryTerm: null,
      readEndpoints: 0,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

function nodeIdFromEndpoint(endpoint: string): string {
  if (endpoint.includes('7301')) return 'node-a'
  if (endpoint.includes('7302')) return 'node-b'
  if (endpoint.includes('7303')) return 'node-c'
  return endpoint
}

function parseCurrentPrimary(value: unknown): string | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null
  }
  const nodeId = (value as Record<string, unknown>).nodeId
  return typeof nodeId === 'string' ? nodeId : null
}

function parseHealth(value: unknown): ClusterNode['health'] {
  if (
    value === 'healthy' ||
    value === 'degraded' ||
    value === 'failing_over' ||
    value === 'unavailable' ||
    value === 'repairing' ||
    value === 'syncing'
  ) {
    return value
  }
  return undefined
}
