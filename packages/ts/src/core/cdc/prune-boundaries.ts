export type PruneBoundarySource = 'device-sync' | 'replication' | 'device-cursors'

export class PruneBoundaries {
  private readonly bySource = new Map<PruneBoundarySource, bigint>()

  set(source: PruneBoundarySource, seq: bigint): void {
    this.bySource.set(source, seq)
  }

  clear(source: PruneBoundarySource): void {
    this.bySource.delete(source)
  }

  lowest(): bigint | null {
    let lowest: bigint | null = null
    for (const boundary of this.bySource.values()) {
      if (lowest === null || boundary < lowest) {
        lowest = boundary
      }
    }
    return lowest
  }
}

export function seqBoundFor(lowest: bigint | null, lastSeq: bigint): bigint {
  if (lastSeq > 0n && lowest !== null) {
    return lastSeq < lowest ? lastSeq : lowest
  }
  if (lowest !== null) {
    return lowest
  }
  return lastSeq
}
