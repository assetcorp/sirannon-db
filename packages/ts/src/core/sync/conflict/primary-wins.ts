import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'
import { LWWResolver } from './lww.js'

/**
 * Resolves a conflict in favour of the node that you name as primary.
 *
 * The resolver accepts a remote change that the primary wrote, and it keeps a
 * local change that the primary wrote. When neither change comes from the
 * primary, as with two replicas that sync through a relay, the resolver falls
 * back to {@link LWWResolver}. Use it in a primary-replica topology where the
 * primary's write always wins over a replica's.
 *
 * @public
 */
export class PrimaryWinsResolver implements ConflictResolver {
  private readonly primaryNodeId: string
  private readonly lww = new LWWResolver()

  constructor(primaryNodeId: string) {
    this.primaryNodeId = primaryNodeId
  }

  /**
   * Returns the version that the primary node wrote, or the last-writer-wins result when neither version comes from the primary.
   *
   * @param ctx - The local and incoming versions of one row.
   * @returns Which version to write.
   */
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
