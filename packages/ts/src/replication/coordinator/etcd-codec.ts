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
  const serialized: SerializedReplicationGroupState = {
    ...state,
    currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
    votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
    primaryTerm: state.primaryTerm.toString(),
    durabilityPointSeq: state.durabilityPointSeq.toString(),
    inSyncNodeIds: [...state.inSyncNodeIds],
    drainingNodeIds: [...state.drainingNodeIds],
    repairingNodeIds: [...state.repairingNodeIds],
    faultedNodeIds: [...state.faultedNodeIds],
    compatibility: cloneCompatibility(state.compatibility),
  }
  return JSON.stringify(serialized)
}

export function parseGroupState(raw: string): ReplicationGroupState {
  const value = JSON.parse(raw) as SerializedReplicationGroupState
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
    compatibility: cloneCompatibility(value.compatibility),
  }
}
