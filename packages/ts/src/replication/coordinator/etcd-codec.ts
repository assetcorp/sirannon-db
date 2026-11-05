import * as v from 'valibot'
import { CoordinatorError } from '../errors.js'
import { cloneCompatibility, cloneMetadata } from './group-rules.js'
import type {
  CoordinatorCompatibilityMetadata,
  CoordinatorLease,
  CoordinatorNodeSession,
  CoordinatorPrimary,
  ReplicationGroupState,
} from './types.js'

export type LeaseKind = 'controller' | 'node-session'

export interface SerializedLease {
  id: string
  kind: LeaseKind
  clusterId: string
  holderId: string
  ttlMs: number
  grantedAtMs: number
  expiresAtMs: number
  metadata?: Record<string, unknown>
}

export interface SerializedNodeSession {
  clusterId: string
  nodeId: string
  lease: SerializedLease
  endpoint?: string
  replicationEndpoint?: string
  groupIds: string[]
  dataBearing: boolean
  voting: boolean
  compatibility?: CoordinatorCompatibilityMetadata
  metadata?: Record<string, unknown>
}

export interface SerializedReplicationGroupState {
  clusterId: string
  groupId: string
  votingDataBearingNodeIds: string[]
  currentPrimary: CoordinatorPrimary | null
  primaryTerm: string
  durabilityPointSeq?: string
  inSyncNodeIds: string[]
  drainingNodeIds: string[]
  repairingNodeIds: string[]
  faultedNodeIds: string[]
  pendingVoterNodeIds?: string[]
  compatibility?: CoordinatorCompatibilityMetadata
  updatedAtMs: number
}

export function serializeLease(lease: SerializedLease): string {
  return JSON.stringify(lease)
}

export function parseLease(raw: string): CoordinatorLease {
  const value = JSON.parse(raw) as SerializedLease
  return {
    ...value,
    metadata: cloneMetadata(value.metadata),
  }
}

export function parseNodeSession(raw: string): CoordinatorNodeSession {
  const value = JSON.parse(raw) as SerializedNodeSession
  return {
    ...value,
    lease: parseLease(JSON.stringify(value.lease)),
    groupIds: [...value.groupIds],
    compatibility: cloneCompatibility(value.compatibility),
    metadata: cloneMetadata(value.metadata),
  }
}

export function parseLeaseIdForEntry(kind: LeaseKind, raw: string): string | null {
  try {
    if (kind === 'node-session') {
      return parseNodeSession(raw).lease.id
    }
    return parseLease(raw).id
  } catch {
    return null
  }
}

export function serializeGroupState(state: ReplicationGroupState): string {
  const { pendingVoterNodeIds, ...rest } = state
  const serialized: SerializedReplicationGroupState = {
    ...rest,
    currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
    votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
    primaryTerm: state.primaryTerm.toString(),
    durabilityPointSeq: state.durabilityPointSeq.toString(),
    inSyncNodeIds: [...state.inSyncNodeIds],
    drainingNodeIds: [...state.drainingNodeIds],
    repairingNodeIds: [...state.repairingNodeIds],
    faultedNodeIds: [...state.faultedNodeIds],
    ...(pendingVoterNodeIds.length > 0 ? { pendingVoterNodeIds: [...pendingVoterNodeIds] } : {}),
    compatibility: cloneCompatibility(state.compatibility),
  }
  return JSON.stringify(serialized)
}

const NODE_IDS_SCHEMA = v.array(v.string())
const SEQUENCE_SCHEMA = v.pipe(v.string(), v.regex(/^\d+$/))

const SERIALIZED_GROUP_STATE_SCHEMA = v.object({
  clusterId: v.string(),
  groupId: v.string(),
  votingDataBearingNodeIds: NODE_IDS_SCHEMA,
  currentPrimary: v.nullable(v.object({ nodeId: v.string(), endpoint: v.optional(v.string()) })),
  primaryTerm: SEQUENCE_SCHEMA,
  durabilityPointSeq: v.optional(SEQUENCE_SCHEMA),
  inSyncNodeIds: NODE_IDS_SCHEMA,
  drainingNodeIds: NODE_IDS_SCHEMA,
  repairingNodeIds: NODE_IDS_SCHEMA,
  faultedNodeIds: v.optional(NODE_IDS_SCHEMA),
  pendingVoterNodeIds: v.optional(NODE_IDS_SCHEMA),
  compatibility: v.optional(
    v.object({
      packageVersion: v.optional(v.string()),
      specVersion: v.optional(v.string()),
      protocolVersion: v.optional(v.string()),
    }),
  ),
  updatedAtMs: v.number(),
})

/**
 * Reads a replication-group record that the etcd coordinator stored, in this release's format or an earlier one's.
 *
 * @param raw - The JSON text that etcd holds for the group.
 * @returns The group state, with no pending voters, no faulted nodes, and a durability point of 0 where the record
 * omits them.
 * @throws CoordinatorError when the text is not a group record that this release can read.
 *
 * @internal
 */
export function parseGroupState(raw: string): ReplicationGroupState {
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch (err: unknown) {
    throw new CoordinatorError('etcd holds a replication-group record that is not JSON', {
      cause: err instanceof Error ? err.message : String(err),
    })
  }
  const result = v.safeParse(SERIALIZED_GROUP_STATE_SCHEMA, parsed)
  if (!result.success) {
    throw new CoordinatorError('etcd holds a replication-group record in a format that this release cannot read', {
      issues: result.issues.map(issue => `${v.getDotPath(issue) ?? '(record)'}: ${issue.message}`),
    })
  }
  const value = result.output
  return {
    ...value,
    currentPrimary: value.currentPrimary ? { ...value.currentPrimary } : null,
    votingDataBearingNodeIds: [...value.votingDataBearingNodeIds],
    primaryTerm: BigInt(value.primaryTerm),
    durabilityPointSeq: value.durabilityPointSeq !== undefined ? BigInt(value.durabilityPointSeq) : 0n,
    inSyncNodeIds: [...value.inSyncNodeIds],
    drainingNodeIds: [...value.drainingNodeIds],
    repairingNodeIds: [...value.repairingNodeIds],
    faultedNodeIds: [...(value.faultedNodeIds ?? [])],
    pendingVoterNodeIds: [...(value.pendingVoterNodeIds ?? [])],
    compatibility: cloneCompatibility(value.compatibility),
  }
}
