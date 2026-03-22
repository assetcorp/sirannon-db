import type { Topology, TopologyRole } from '../types.js'

/**
 * Multi-primary (peer-to-peer) topology where every node accepts writes.
 *
 * All nodes replicate to all connected peers and accept inbound batches from
 * any peer. Because concurrent writes to the same row are possible, a
 * ConflictResolver is required; `requiresConflictResolution` returns true to
 * signal this to the engine during startup validation.
 */
export class MultiPrimaryTopology implements Topology {
  readonly role: TopologyRole = 'peer'

  canWrite(): boolean {
    return true
  }

  shouldReplicateTo(_peerId: string, _peerRole: TopologyRole): boolean {
    return true
  }

  shouldAcceptFrom(_peerId: string, _peerRole: TopologyRole): boolean {
    return true
  }

  requiresConflictResolution(): boolean {
    return true
  }
}
