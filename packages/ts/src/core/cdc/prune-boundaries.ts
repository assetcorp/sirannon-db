export type PruneBoundarySource = 'device-sync' | 'device-push-cursor' | 'replication' | 'device-cursors'

export class PruneBoundaries {
  private readonly bySource = new Map<PruneBoundarySource, bigint>()
  private readonly listeners = new Set<() => void>()

  set(source: PruneBoundarySource, seq: bigint): void {
    if (this.bySource.get(source) === seq) return
    this.bySource.set(source, seq)
    this.notify()
  }

  clear(source: PruneBoundarySource): void {
    if (this.bySource.delete(source)) this.notify()
  }

  onChange(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  private notify(): void {
    for (const listener of this.listeners) {
      try {
        listener()
      } catch {}
    }
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
