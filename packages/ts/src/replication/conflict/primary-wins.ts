import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'
import { LWWResolver } from './lww.js'

/**
 * Conflict resolver that unconditionally favors the designated primary node.
 *
 * If the remote change originated from the primary, it is accepted. If the
 * local change originated from the primary, the remote is rejected. When
 * neither side is the primary (e.g., two replicas syncing through a relay),
 * the decision falls back to LWW ordering. This resolver is designed for
 * primary-replica topologies where the primary is the authoritative source of
 * truth and replica-side writes should never override it.
 */
export class PrimaryWinsResolver implements ConflictResolver {
  private readonly primaryNodeId: string
  private readonly lww = new LWWResolver()

  constructor(primaryNodeId: string) {
    this.primaryNodeId = primaryNodeId
  }

  resolve(ctx: ConflictContext): ConflictResolution {
    if (ctx.remoteChange.nodeId === this.primaryNodeId) {
      return { action: 'accept_remote' }
    }

    if (ctx.localChange?.nodeId === this.primaryNodeId) {
      return { action: 'keep_local' }
    }

    return this.lww.resolve(ctx)
  }
}
