import type { ClusterReadEndpointInfo, ClusterStatusInfo, NodeHealthReason, NodeHealthState } from '../core/types.js'
import { NODE_HEALTH_REASONS, NODE_HEALTH_STATES } from '../core/types.js'
import { RemoteError } from './types.js'

function invalid(reason: string): RemoteError {
  return new RemoteError('INVALID_RESPONSE', `Cluster status ${reason}`)
}

function readRecord(value: unknown, reason: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw invalid(reason)
  return value as Record<string, unknown>
}

function readPrimary(value: unknown): ClusterStatusInfo['currentPrimary'] {
  if (value === undefined || value === null) return null

  const primary = readRecord(value, 'currentPrimary must be an object or null')
  if (typeof primary.nodeId !== 'string') throw invalid('currentPrimary.nodeId must be a string')

  return {
    nodeId: primary.nodeId,
    endpoint: typeof primary.endpoint === 'string' ? primary.endpoint : undefined,
  }
}

function readReadEndpoints(value: unknown): ClusterReadEndpointInfo[] | undefined {
  if (value === undefined) return undefined
  if (!Array.isArray(value)) throw invalid('readEndpoints must be an array')

  return value.map(entry => {
    const endpoint = readRecord(entry, 'read endpoint must be an object')
    if (typeof endpoint.nodeId !== 'string') throw invalid('read endpoint nodeId must be a string')
    if (typeof endpoint.endpoint !== 'string') throw invalid('read endpoint URL must be a string')
    if (!Array.isArray(endpoint.readConcerns)) throw invalid('read endpoint concerns must be an array')
    return {
      nodeId: endpoint.nodeId,
      endpoint: endpoint.endpoint,
      readConcerns: endpoint.readConcerns as ClusterReadEndpointInfo['readConcerns'],
    }
  })
}

function readPrimaryTerm(value: unknown): bigint | undefined {
  if (value === undefined || value === null) return undefined
  if (typeof value !== 'string' || !/^\d+$/.test(value)) throw invalid('primaryTerm must be a decimal string')
  return BigInt(value)
}

/**
 * Reads the body of `GET /db/{id}/cluster` into the status the server reported.
 *
 * The route answers with a decimal string for `primaryTerm`, so that a term beyond
 * the safe integer range survives JSON, and this returns it as a bigint. A caller
 * building a cluster view gets the health, the reason behind it, the node the group
 * names as primary, and the readable endpoints, each checked against the values the
 * engine reports.
 *
 * @param data - The parsed JSON body of the response.
 * @param databaseId - The database the caller asked about.
 * @returns The status the server reported.
 * @throws A remote error with code `INVALID_RESPONSE` where the body is malformed or names another database.
 *
 * @public
 */
export function parseClusterStatus(data: unknown, databaseId: string): ClusterStatusInfo {
  const record = readRecord(data, 'must be an object')

  if (record.databaseId !== databaseId) throw invalid('names another database')

  const health = record.health as NodeHealthState
  if (!NODE_HEALTH_STATES.includes(health))
    throw invalid(`health '${String(record.health)}' is not a state the engine reports`)

  const healthReason = record.healthReason as NodeHealthReason
  if (!NODE_HEALTH_REASONS.includes(healthReason)) {
    throw invalid(`healthReason '${String(record.healthReason)}' is not a reason the engine reports`)
  }

  const role = record.role
  if (role !== undefined && role !== 'primary' && role !== 'replica') throw invalid('role must be primary or replica')

  return {
    databaseId,
    role,
    health,
    healthReason,
    currentPrimary: readPrimary(record.currentPrimary),
    primaryTerm: readPrimaryTerm(record.primaryTerm),
    readEndpoints: readReadEndpoints(record.readEndpoints),
    replicationGroupId: typeof record.replicationGroupId === 'string' ? record.replicationGroupId : undefined,
  }
}
