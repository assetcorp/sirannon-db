import type { ConflictContext, ConflictResolution, ConflictResolver } from '../types.js'
import { LWWResolver } from './lww.js'

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
