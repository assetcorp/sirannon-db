import { cloneCompatibility, cloneMetadata, markDisplacedPrimaryForRepair } from './group-rules.js'
import type { CoordinatorLease, CoordinatorNodeSession, NodeSessionWatcher, ReplicationGroupState } from './types.js'

export class NodeSessionWatchers {
  private readonly byCluster = new Map<string, Set<NodeSessionWatcher>>()

  add(clusterId: string, watcher: NodeSessionWatcher): () => void {
    let watchers = this.byCluster.get(clusterId)
    if (!watchers) {
      watchers = new Set()
      this.byCluster.set(clusterId, watchers)
    }
    watchers.add(watcher)

    return () => {
      const current = this.byCluster.get(clusterId)
      if (!current) return
      current.delete(watcher)
      if (current.size === 0) {
        this.byCluster.delete(clusterId)
      }
    }
  }

  notify(clusterId: string, liveNodeIds: readonly string[], onError?: (error: Error) => void): void {
    const watchers = this.byCluster.get(clusterId)
    if (!watchers) return

    for (const watcher of watchers) {
      try {
        watcher([...liveNodeIds])
      } catch (err: unknown) {
        onError?.(err instanceof Error ? err : new Error(String(err)))
      }
    }
  }
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
