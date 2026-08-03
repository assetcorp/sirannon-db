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
 *
 * @public
 */
export class PrimaryReplicaTopology implements Topology {
  /**
   * Whether this node accepts writes or serves reads.
   */
  readonly role: TopologyRole

  constructor(role: 'primary' | 'replica') {
    this.role = role
  }

  /**
   * Reports whether this node accepts writes, which only the primary does.
   *
   * @returns True on the primary.
   */
  canWrite(): boolean {
    return this.role === 'primary'
  }

  /**
   * Reports whether this node sends its changes to a given peer.
   *
   * @param _peerId - Identifier of the peer, which this topology ignores.
   * @param peerRole - Role of the peer.
   * @returns True when this node is the primary and the peer is a replica.
   */
  shouldReplicateTo(_peerId: string, peerRole: TopologyRole): boolean {
    return this.role === 'primary' && peerRole === 'replica'
  }

  /**
   * Reports whether this node applies changes arriving from a given peer.
   *
   * @param _peerId - Identifier of the peer, which this topology ignores.
   * @param peerRole - Role of the peer.
   * @returns True when this node is a replica and the peer is the primary.
   */
  shouldAcceptFrom(_peerId: string, peerRole: TopologyRole): boolean {
    return this.role === 'replica' && peerRole === 'primary'
  }

  /**
   * Reports whether incoming changes need a resolver, which a single writer removes the need for.
   *
   * @returns False.
   */
  requiresConflictResolution(): boolean {
    return false
  }
}
