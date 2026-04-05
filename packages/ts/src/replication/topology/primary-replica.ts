import type { Topology, TopologyRole } from '../types.js'

/**
 * Single-writer topology with one primary and one or more read-only replicas.
 *
 * Only the primary node accepts writes; replicas reject writes with a
 * TopologyError (or forward them when writeForwarding is enabled on the
 * engine). The primary replicates outbound batches only to peers whose role
 * is 'replica', and replicas only accept inbound batches from a peer whose
 * role is 'primary'. Conflict resolution is not required because a single
 * writer eliminates concurrent write conflicts by design.
 */
export class PrimaryReplicaTopology implements Topology {
  readonly role: TopologyRole

  constructor(role: 'primary' | 'replica') {
    this.role = role
  }

  canWrite(): boolean {
    return this.role === 'primary'
  }

  shouldReplicateTo(_peerId: string, peerRole: TopologyRole): boolean {
    return this.role === 'primary' && peerRole === 'replica'
  }

  shouldAcceptFrom(_peerId: string, peerRole: TopologyRole): boolean {
    return this.role === 'replica' && peerRole === 'primary'
  }

  requiresConflictResolution(): boolean {
    return false
  }
}
