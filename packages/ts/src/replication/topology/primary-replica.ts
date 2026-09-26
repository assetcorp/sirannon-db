import type { Topology, TopologyRole } from '../types.js'

/**
 * Sets the replication rules for a group with one writable primary and one or more read-only replicas.
 *
 * Only the primary accepts writes. A replica rejects a write with a
 * `TopologyError`, or forwards it to the primary when you turn on
 * `writeForwarding` in the engine's config. The primary sends batches only to
 * peers whose role is 'replica', and a replica applies batches only from a peer
 * whose role is 'primary'. Since only one node writes, no two nodes can change
 * the same row at once, so {@link PrimaryReplicaTopology.requiresConflictResolution}
 * returns false.
 *
 * @public
 */
export class PrimaryReplicaTopology implements Topology {
  /**
   * Records whether this node is the primary, which accepts writes, or a replica, which serves reads.
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
   * Reports whether this node applies the changes that a given peer sends.
   *
   * @param _peerId - Identifier of the peer, which this topology ignores.
   * @param peerRole - Role of the peer.
   * @returns True when this node is a replica and the peer is the primary.
   */
  shouldAcceptFrom(_peerId: string, peerRole: TopologyRole): boolean {
    return this.role === 'replica' && peerRole === 'primary'
  }

  /**
   * Reports whether the engine has to resolve conflicts for this topology. Only the primary writes, so the answer is
   * always false.
   *
   * @returns False.
   */
  requiresConflictResolution(): boolean {
    return false
  }
}
