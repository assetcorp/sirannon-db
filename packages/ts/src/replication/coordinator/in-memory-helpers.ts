import {
  cloneCompatibility,
  cloneMetadata,
  cloneReplicationGroupState,
  isEligiblePromotionSession,
  markDisplacedPrimaryForRepair,
} from './group-rules.js'
import type { CoordinatorLease, CoordinatorNodeSession, ReplicationGroupState } from './types.js'

class ClusterWatchers<T> {
  private readonly byKey = new Map<string, Set<(value: T) => void>>()

  constructor(private readonly copy: (value: T) => T) {}

  add(key: string, watcher: (value: T) => void): () => void {
    let watchers = this.byKey.get(key)
    if (!watchers) {
      watchers = new Set()
      this.byKey.set(key, watchers)
    }
    watchers.add(watcher)

    return () => {
      const current = this.byKey.get(key)
      if (!current) return
      current.delete(watcher)
      if (current.size === 0) {
        this.byKey.delete(key)
      }
    }
  }

  notify(key: string, value: T, onError?: (error: Error) => void): void {
    const watchers = this.byKey.get(key)
    if (!watchers) return

    for (const watcher of watchers) {
      try {
        watcher(this.copy(value))
      } catch (err: unknown) {
        onError?.(err instanceof Error ? err : new Error(String(err)))
      }
    }
  }
}

export class NodeSessionWatchers extends ClusterWatchers<readonly string[]> {
  constructor() {
    super(liveNodeIds => [...liveNodeIds])
  }
}

export class ControllerLeaseWatchers extends ClusterWatchers<CoordinatorLease | null> {
  constructor() {
    super(lease => (lease === null ? null : cloneLease(lease)))
  }
}

export class ReplicationGroupWatchers extends ClusterWatchers<ReplicationGroupState> {
  constructor() {
    super(cloneReplicationGroupState)
  }
}

export function findLeaseIn(
  controllerLeases: Iterable<CoordinatorLease>,
  sessions: Iterable<CoordinatorNodeSession>,
  leaseId: string,
): CoordinatorLease | null {
  for (const lease of controllerLeases) {
    if (lease.id === leaseId) {
      return lease
    }
  }
  for (const session of sessions) {
    if (session.lease.id === leaseId) {
      return session.lease
    }
  }
  return null
}

/**
 * Returns true while a lease has not expired.
 *
 * @param lease - The lease to check.
 * @param nowMs - The time to check the lease against.
 * @returns True when the lease expires after `nowMs`.
 */
export function isLeaseLiveAt(lease: CoordinatorLease, nowMs: number): boolean {
  return lease.expiresAtMs > nowMs
}

export function liveNodeIdsIn(sessions: Iterable<CoordinatorNodeSession>, clusterId: string, nowMs: number): string[] {
  const live: string[] = []
  for (const session of sessions) {
    if (session.clusterId === clusterId && isLeaseLiveAt(session.lease, nowMs)) {
      live.push(session.nodeId)
    }
  }
  return live
}

export function cloneNodeSession(session: CoordinatorNodeSession): CoordinatorNodeSession {
  return {
    ...session,
    lease: cloneLease(session.lease),
    groupIds: [...session.groupIds],
    compatibility: cloneCompatibility(session.compatibility),
    metadata: cloneMetadata(session.metadata),
  }
}

export function cloneLease(lease: CoordinatorLease): CoordinatorLease {
  return {
    ...lease,
    metadata: cloneMetadata(lease.metadata),
  }
}

/**
 * Returns the first voter that the controller may promote in place of the current primary: one outside the excluded
 * nodes whose session is live and whose state makes it eligible.
 *
 * @param state - The group state to choose from.
 * @param excludedNodeIds - The nodes to skip, such as the primary that has just failed.
 * @param sessionFor - Returns the session that the coordinator holds for a node, if any.
 * @param nowMs - The time against which a session's lease counts as live.
 * @returns The voter's ID and client address, or null when no voter qualifies.
 */
export function findPromotablePrimary(
  state: ReplicationGroupState,
  excludedNodeIds: ReadonlySet<string>,
  sessionFor: (nodeId: string) => CoordinatorNodeSession | undefined,
  nowMs: number,
): { nodeId: string; endpoint?: string } | null {
  for (const nodeId of state.votingDataBearingNodeIds) {
    if (nodeId === state.currentPrimary?.nodeId || excludedNodeIds.has(nodeId)) continue
    const session = sessionFor(nodeId)
    if (!session || !isLeaseLiveAt(session.lease, nowMs)) continue
    if (!isEligiblePromotionSession(state, nodeId, session)) continue
    return session.endpoint ? { nodeId, endpoint: session.endpoint } : { nodeId }
  }
  return null
}

export function movePrimary(state: ReplicationGroupState, nextPrimary: { nodeId: string; endpoint?: string }): void {
  const displacedPrimaryId = state.currentPrimary?.nodeId
  state.primaryTerm += 1n
  state.durabilityPointSeq = 0n
  state.currentPrimary = { ...nextPrimary }
  markDisplacedPrimaryForRepair(state, displacedPrimaryId, nextPrimary.nodeId)
}

export function nodeSessionKey(clusterId: string, nodeId: string): string {
  return `${clusterId}\0${nodeId}`
}

export function replicationGroupKey(clusterId: string, groupId: string): string {
  return `${clusterId}\0${groupId}`
}
