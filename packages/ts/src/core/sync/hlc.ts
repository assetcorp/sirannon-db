import { ReplicationError } from './errors.js'
import type { HLCTimestamp } from './types.js'

const MAX_LOGICAL = 0xffff

/**
 * Generates hybrid logical clock (HLC) timestamps that order events across nodes.
 *
 * Each timestamp has the form `{wallMs hex}-{logical hex}-{nodeId}`, so a plain
 * string comparison orders two timestamps by wall-clock time, then by logical
 * counter, then by node ID. The wall-clock part holds milliseconds since the
 * Unix epoch, and the logical counter orders events within one millisecond.
 *
 * Call `now()` to stamp a local write with a timestamp higher than every
 * earlier one from this clock. Call `receive(remote)` for each remote change,
 * so that the clock moves ahead of every timestamp that it receives.
 *
 * @public
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

  /**
   * Returns a new timestamp and advances the clock.
   *
   * @internal
   */
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

    return HLC.encode(this.wallMs, this.logical, this.nodeId)
  }

  /**
   * Merges a remote timestamp into the clock and returns the new local timestamp.
   *
   * @internal
   */
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

    return HLC.encode(this.wallMs, this.logical, this.nodeId)
  }

  /** Compares two encoded timestamps as strings and returns -1, 0, or 1. */
  static compare(a: string, b: string): number {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }

  /** Parses an encoded timestamp into its wall-clock, logical, and node ID parts. */
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

  /**
   * Returns the string form of a clock reading, padded so that string order matches clock order.
   *
   * @param wallMs - The wall-clock milliseconds since the Unix epoch.
   * @param logical - The counter that orders events within one millisecond.
   * @param nodeId - The ID of the node that takes the reading.
   * @returns The encoded stamp.
   */
  static encode(wallMs: number, logical: number, nodeId: string): string {
    const wallHex = wallMs.toString(16).padStart(12, '0')
    const logicalHex = logical.toString(16).padStart(4, '0')
    return `${wallHex}-${logicalHex}-${nodeId}`
  }
}
