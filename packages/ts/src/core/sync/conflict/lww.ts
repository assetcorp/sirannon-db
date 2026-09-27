import { HLC } from '../hlc.js'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'

/**
 * Resolves a conflict by keeping the write with the higher hybrid logical clock (HLC) timestamp.
 *
 * The resolver accepts every remote delete without comparing timestamps, so a
 * delete wins over a concurrent update. It also accepts the remote write
 * when the local row has no timestamp. When the two timestamps are equal, the
 * resolver accepts the remote write only if its node ID sorts higher, so that
 * every node reaches the same resolution without coordination. Sirannon uses
 * this resolver by default, and {@link PrimaryWinsResolver} and
 * {@link FieldMergeResolver} fall back to it.
 *
 * @public
 */
export class LWWResolver implements ConflictResolver {
  /**
   * Returns the incoming row when its stamp is higher, and the local row otherwise.
   *
   * @param ctx - The local and incoming versions of one row.
   * @returns Which version to write.
   */
  resolve(ctx: ConflictContext): ConflictResolution {
    if (ctx.remoteChange.operation === 'delete') {
      return { action: 'accept_remote' }
    }

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
