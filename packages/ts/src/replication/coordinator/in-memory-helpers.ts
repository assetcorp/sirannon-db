import {
  cloneCompatibility,
  cloneMetadata,
  cloneReplicationGroupState,
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

export function liveNodeIdsIn(sessions: Iterable<CoordinatorNodeSession>, clusterId: string, nowMs: number): string[] {
  const live: string[] = []
  for (const session of sessions) {
    if (session.clusterId === clusterId && session.lease.expiresAtMs > nowMs) {
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

export function movePrimary(state: ReplicationGroupState, nextPrimary: { nodeId: string; endpoint?: string }): void {
  const displacedPrimaryId = state.currentPrimary?.nodeId
  state.primaryTerm += 1n
  state.currentPrimary = { ...nextPrimary }
  markDisplacedPrimaryForRepair(state, displacedPrimaryId, nextPrimary.nodeId)
}

export function nodeSessionKey(clusterId: string, nodeId: string): string {
  return `${clusterId}\0${nodeId}`
}

export function replicationGroupKey(clusterId: string, groupId: string): string {
  return `${clusterId}\0${groupId}`
}
