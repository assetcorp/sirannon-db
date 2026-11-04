import type { Namespace } from 'etcd3'
import { CoordinatorError } from '../errors.js'
import { parseNodeSession, type SerializedNodeSession } from './etcd-codec.js'
import { nodeSessionKey, ttlMsToSeconds } from './etcd-connection.js'
import { type EtcdLeaseRegistry, revokeLeaseQuietly } from './etcd-lease-registry.js'
import { cloneCompatibility, cloneMetadata } from './group-rules.js'
import type { CoordinatorNodeSession, RegisterNodeSessionInput } from './types.js'

const SESSION_REGISTRATION_ATTEMPTS = 3

/**
 * Returns a new etcd session that records one node as a live member of its cluster, and replaces any session that an
 * earlier process of the same node left behind. The function grants a lease, writes the session under that lease
 * only while the stored session is the one that it read, and tracks the lease so that the coordinator can renew it.
 *
 * @param namespace - The etcd namespace that holds the coordinator's keys.
 * @param leases - The registry of leases that this coordinator granted.
 * @param input - The node and the addresses, groups, and versions to record.
 * @returns The session as etcd stores it.
 * @throws CoordinatorError when concurrent writes to the session key outlast three attempts.
 */
export async function registerEtcdNodeSession(
  namespace: Namespace,
  leases: EtcdLeaseRegistry,
  input: RegisterNodeSessionInput,
): Promise<CoordinatorNodeSession> {
  const key = nodeSessionKey(input.clusterId, input.nodeId)
  const lease = namespace.lease(ttlMsToSeconds(input.ttlMs))
  const leaseId = await lease.grant()
  const grantedAtMs = Date.now()
  const session: SerializedNodeSession = {
    clusterId: input.clusterId,
    nodeId: input.nodeId,
    lease: {
      id: leaseId,
      kind: 'node-session',
      clusterId: input.clusterId,
      holderId: input.nodeId,
      ttlMs: input.ttlMs,
      grantedAtMs,
      expiresAtMs: grantedAtMs + input.ttlMs,
      metadata: cloneMetadata(input.metadata),
    },
    endpoint: input.endpoint,
    replicationEndpoint: input.replicationEndpoint,
    groupIds: [...(input.groupIds ?? [])],
    dataBearing: input.dataBearing ?? true,
    voting: input.voting ?? true,
    compatibility: cloneCompatibility(input.compatibility),
    metadata: cloneMetadata(input.metadata),
  }

  const rawSession = JSON.stringify(session)
  let registered = false
  for (let attempt = 0; attempt < SESSION_REGISTRATION_ATTEMPTS && !registered; attempt++) {
    const existingRawSession = await namespace.get(key).string()
    const transaction = existingRawSession
      ? namespace.if(key, 'Value', '==', existingRawSession)
      : namespace.if(key, 'Create', '==', 0)
    const result = await transaction.then(namespace.put(key).value(rawSession).lease(leaseId)).commit()
    registered = result.succeeded === true
  }
  if (!registered) {
    await revokeLeaseQuietly(lease)
    throw new CoordinatorError(`Node session '${input.nodeId}' registration conflicted with a concurrent write`)
  }

  await leases.discardSuperseded(key, leaseId)
  leases.track(lease, {
    leaseId,
    key,
    ttlMs: input.ttlMs,
    ttlSeconds: ttlMsToSeconds(input.ttlMs),
    kind: 'node-session',
    clusterId: input.clusterId,
    holderId: input.nodeId,
    metadata: cloneMetadata(input.metadata),
    nodeSession: {
      clusterId: session.clusterId,
      nodeId: session.nodeId,
      endpoint: session.endpoint,
      replicationEndpoint: session.replicationEndpoint,
      groupIds: [...session.groupIds],
      dataBearing: session.dataBearing,
      voting: session.voting,
      compatibility: cloneCompatibility(session.compatibility),
      metadata: cloneMetadata(session.metadata),
    },
  })

  return parseNodeSession(rawSession)
}
