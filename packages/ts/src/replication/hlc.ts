import { ReplicationError } from './errors.js'
import type { HLCTimestamp } from './types.js'

const MAX_LOGICAL = 0xffff

export class HLC {
  private wallMs: number
  private logical: number
  private readonly nodeId: string

  constructor(nodeId: string) {
    this.nodeId = nodeId
    this.wallMs = 0
    this.logical = 0
  }

  now(): string {
    const physicalMs = Date.now()

    if (physicalMs > this.wallMs) {
      this.wallMs = physicalMs
      this.logical = 0
    } else {
      this.logical += 1
      if (this.logical > MAX_LOGICAL) {
        throw new ReplicationError('HLC logical counter overflow')
      }
    }

    return this.encode(this.wallMs, this.logical, this.nodeId)
  }

  receive(remote: string): string {
    const r = HLC.decode(remote)
    const physicalMs = Date.now()

    if (physicalMs > this.wallMs && physicalMs > r.wallMs) {
      this.wallMs = physicalMs
      this.logical = 0
    } else if (r.wallMs > this.wallMs) {
      this.wallMs = r.wallMs
      this.logical = r.logical + 1
    } else if (this.wallMs > r.wallMs) {
      this.logical += 1
    } else {
      this.logical = Math.max(this.logical, r.logical) + 1
    }

    if (this.logical > MAX_LOGICAL) {
      throw new ReplicationError('HLC logical counter overflow')
    }

    return this.encode(this.wallMs, this.logical, this.nodeId)
  }

  static compare(a: string, b: string): number {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }

  static decode(hlc: string): HLCTimestamp {
    const parts = hlc.split('-')
    if (parts.length < 3) {
      throw new ReplicationError(`Invalid HLC format: ${hlc}`)
    }
    return {
      wallMs: Number.parseInt(parts[0], 16),
      logical: Number.parseInt(parts[1], 16),
      nodeId: parts.slice(2).join('-'),
    }
  }

  private encode(wallMs: number, logical: number, nodeId: string): string {
    const wallHex = wallMs.toString(16).padStart(12, '0')
    const logicalHex = logical.toString(16).padStart(4, '0')
    return `${wallHex}-${logicalHex}-${nodeId}`
  }
}
