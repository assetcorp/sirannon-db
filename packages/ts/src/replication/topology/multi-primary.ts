import type { Topology, TopologyRole } from '../types.js'

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
