import { ReplicationError } from './errors.js'
import type { HLCTimestamp } from './types.js'

const MAX_LOGICAL = 0xffff

/**
 * Hybrid Logical Clock (HLC) for causal ordering of events across nodes.
 *
 * Each timestamp is encoded as `{wallMs hex}-{logical hex}-{nodeId}`, which
 * is directly comparable with string comparison while preserving both
 * wall-clock proximity and causal ordering guarantees. The wall-clock
 * component tracks real time (milliseconds since epoch), the logical counter
 * disambiguates events that occur within the same millisecond, and the nodeId
 * ties the timestamp to its origin.
 *
 * Call `now()` before persisting a local write to generate a monotonically
 * increasing timestamp. Call `receive(remote)` when processing a remote
 * change to merge the remote clock into the local state, ensuring the local
 * clock is never behind any observed timestamp.
 */
export class HLC {
  private wallMs: number
  private logical: number
  private readonly nodeId: string

  constructor(nodeId: string) {
    this.nodeId = nodeId
    this.wallMs = 0
    this.logical = 0
  }

  /** Generate a new HLC timestamp, advancing the clock. */
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

  /** Merge a remote HLC timestamp into the local clock and return the updated value. */
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

  /** Lexicographic comparison of two encoded HLC strings. Returns -1, 0, or 1. */
  static compare(a: string, b: string): number {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }

  /** Parse an encoded HLC string into its wall-clock, logical, and nodeId components. */
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
