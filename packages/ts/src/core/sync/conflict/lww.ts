import { HLC } from '../hlc.js'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'

/**
 * Last-Writer-Wins conflict resolver.
 *
 * Compares the remote HLC against the local HLC. The higher timestamp wins.
 * When both timestamps are equal (concurrent writes within the same
 * millisecond and logical tick), the tie is broken deterministically by
 * comparing node IDs lexicographically, so that every node reaches the same
 * resolution without coordination. This is the default resolver and the
 * fallback used by PrimaryWinsResolver and FieldMergeResolver when they
 * cannot make a more specific decision.
 */
export class LWWResolver implements ConflictResolver {
  resolve(ctx: ConflictContext): ConflictResolution {
    if (ctx.localHlc === null) {
      return { action: 'accept_remote' }
    }

    const cmp = HLC.compare(ctx.remoteHlc, ctx.localHlc)

    if (cmp > 0) {
      return { action: 'accept_remote' }
    }

    if (cmp < 0) {
      return { action: 'keep_local' }
    }

    if (ctx.remoteChange.nodeId > (ctx.localChange?.nodeId ?? '')) {
      return { action: 'accept_remote' }
    }

    return { action: 'keep_local' }
  }
}
