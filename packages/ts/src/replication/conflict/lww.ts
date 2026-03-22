import { HLC } from '../hlc.js'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'

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
