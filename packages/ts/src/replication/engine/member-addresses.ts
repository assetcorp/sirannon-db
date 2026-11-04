import { groupMemberIds } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import type { ReplicationEngine } from './engine.js'

/**
 * Holds the addresses and session leases that this node last read from its group members' coordinator sessions.
 *
 * @internal
 */
export class MemberAddresses {
  /** Maps each member's node ID to the client address in its live session. */
  clientEndpoints: Record<string, string> = {}
  /** Maps each other member's node ID to the replication address in its live session. */
  replicationEndpoints = new Map<string, string>()
  /** Maps each member's node ID to the ID of the session lease that this node last saw for it. */
  readonly sessionLeaseIds = new Map<string, string>()
  /** Holds the group's members as of the last group state that this node read, so that a change marks the addresses stale. */
  memberKey = ''
  /** Is true when a session or the group's members changed after the last refresh. */
  stale = true
  /** Is true while a refresh reads the sessions, so that one refresh runs at a time. */
  refreshing = false
  /** Holds the time in milliseconds at which the last refresh started, or 0 before the first one. */
  refreshedAtMs = 0

  /**
   * Marks the addresses stale, so that the next check reads the members' sessions again.
   */
  markStale(): void {
    this.stale = true
  }

  /**
   * Marks the addresses stale when a group state holds other members than the last one that this node read.
   *
   * @param state - The group state that this node read.
   */
  noteGroupState(state: ReplicationGroupState): void {
    const memberKey = groupMemberIds(state).sort().join('\0')
    if (memberKey === this.memberKey) return
    this.memberKey = memberKey
    this.stale = true
  }
}

/**
 * Reads every member's live coordinator session and records its client and replication addresses. A member without a
 * live session keeps the replication address that this node last read for it. The transport receives the replication
 * addresses on every refresh, so that it dials them again after it reconnects, and a member whose session lease
 * changed proves its position again before its acknowledgements count, because a new session can belong to a process
 * with another file.
 *
 * @param engine - The node's engine.
 * @returns A promise that settles once the refresh finishes, or at once outside coordinator mode, before the node
 * reads a group state, and while another refresh runs.
 *
 * @internal
 */
export async function refreshMemberAddresses(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  const state = engine.coordinatorState
  const addresses = engine.memberAddresses
  if (!config || !state || addresses.refreshing) return
  addresses.refreshing = true
  addresses.stale = false
  addresses.refreshedAtMs = Date.now()
  try {
    const memberIds = groupMemberIds(state)
    const clientEndpoints: Record<string, string> = {}
    const replicationEndpoints = new Map<string, string>()
    for (const nodeId of memberIds) {
      if (nodeId === engine.nodeId) {
        if (config.endpoint) clientEndpoints[nodeId] = config.endpoint
        continue
      }
      const session = await config.coordinator.getLiveNodeSession(config.clusterId, nodeId)
      if (!session) {
        const lastKnown = addresses.replicationEndpoints.get(nodeId)
        if (lastKnown !== undefined) replicationEndpoints.set(nodeId, lastKnown)
        continue
      }
      const previousLeaseId = addresses.sessionLeaseIds.get(nodeId)
      if (previousLeaseId !== undefined && previousLeaseId !== session.lease.id) {
        engine.peerTracker.forgetConnectionAcks(nodeId)
      }
      addresses.sessionLeaseIds.set(nodeId, session.lease.id)
      if (session.endpoint) clientEndpoints[nodeId] = session.endpoint
      if (session.replicationEndpoint) replicationEndpoints.set(nodeId, session.replicationEndpoint)
    }
    for (const nodeId of addresses.sessionLeaseIds.keys()) {
      if (!memberIds.includes(nodeId)) addresses.sessionLeaseIds.delete(nodeId)
    }
    addresses.clientEndpoints = clientEndpoints
    addresses.replicationEndpoints = replicationEndpoints
    if (engine.running) engine.config.transport.setPeerEndpoints?.(replicationEndpoints)
  } catch (err: unknown) {
    addresses.stale = true
    throw err
  } finally {
    addresses.refreshing = false
  }
}
