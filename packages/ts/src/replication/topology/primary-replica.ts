import type { Topology, TopologyRole } from '../types.js'

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
