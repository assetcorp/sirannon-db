import { parseClusterStatus } from '@delali/sirannon-db/client/topology'
import { clusterEndpointsFromEnv, DATABASE_ID, DEFAULT_CLUSTER_TOKEN, toServerBaseUrl } from './cluster-config'
import { getMajorityWriteAvailability } from './cluster-readiness'
import type { ClusterNode } from './schemas'
import { clusterNodeSchema } from './schemas'

const CLUSTER_STATUS_TIMEOUT_MS = 2_000

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

function unreachable(nodeId: string, endpoint: string, error: string): ClusterNode {
  return {
    nodeId,
    endpoint,
    reachable: false,
    currentPrimary: null,
    primaryTerm: null,
    readEndpoints: 0,
    error,
  }
}

async function fetchClusterNode(endpoint: string, token: string): Promise<ClusterNode> {
  const baseUrl = toServerBaseUrl(endpoint)
  const nodeId = nodeIdFromEndpoint(baseUrl)

  try {
    const response = await fetch(`${baseUrl}/db/${DATABASE_ID}/cluster`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(CLUSTER_STATUS_TIMEOUT_MS),
    })
    if (!response.ok) {
      return unreachable(nodeId, baseUrl, `HTTP ${response.status}`)
    }

    const status = parseClusterStatus(await response.json(), DATABASE_ID)
    return {
      nodeId,
      endpoint: baseUrl,
      reachable: true,
      role: status.role,
      health: status.health,
      healthReason: status.healthReason,
      currentPrimary: status.currentPrimary?.nodeId ?? null,
      primaryTerm: status.primaryTerm === undefined ? null : status.primaryTerm.toString(),
      readEndpoints: status.readEndpoints?.length ?? 0,
      error: null,
    }
  } catch (error) {
    return unreachable(nodeId, baseUrl, error instanceof Error ? error.message : String(error))
  }
}

function nodeIdFromEndpoint(endpoint: string): string {
  if (endpoint.includes('7301')) return 'node-a'
  if (endpoint.includes('7302')) return 'node-b'
  if (endpoint.includes('7303')) return 'node-c'
  return endpoint
}
