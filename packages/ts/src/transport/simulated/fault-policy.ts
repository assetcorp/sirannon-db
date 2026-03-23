import type { SeededPRNG } from './prng.js'

export type EventKind = 'batch' | 'ack' | 'raft' | 'forward_request' | 'forward_response'

export interface FaultPolicyConfig {
  dropRate?: number
  latencyMin?: number
  latencyMax?: number
  partitions?: Array<[string, string]>
}

function partitionKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`
}

export class FaultPolicy {
  private readonly prng: SeededPRNG
  private readonly partitionSet = new Set<string>()
  private dropRate: number
  private latencyMin: number
  private latencyMax: number

  constructor(config: FaultPolicyConfig, prng: SeededPRNG) {
    this.prng = prng
    this.dropRate = config.dropRate ?? 0
    this.latencyMin = config.latencyMin ?? 1
    this.latencyMax = config.latencyMax ?? 5

    if (config.partitions) {
      for (const [a, b] of config.partitions) {
        this.partitionSet.add(partitionKey(a, b))
      }
    }
  }

  shouldDrop(from: string, to: string, _kind: EventKind): boolean {
    if (this.partitionSet.has(partitionKey(from, to))) {
      return true
    }
    if (this.dropRate > 0 && this.prng.nextBool(this.dropRate)) {
      return true
    }
    return false
  }

  sampleLatency(_from: string, _to: string, _kind: EventKind): number {
    if (this.latencyMin >= this.latencyMax) {
      return this.latencyMin
    }
    return this.prng.nextInt(this.latencyMin, this.latencyMax)
  }

  addPartition(nodeA: string, nodeB: string): void {
    this.partitionSet.add(partitionKey(nodeA, nodeB))
  }

  removePartition(nodeA: string, nodeB: string): void {
    this.partitionSet.delete(partitionKey(nodeA, nodeB))
  }

  isPartitioned(nodeA: string, nodeB: string): boolean {
    return this.partitionSet.has(partitionKey(nodeA, nodeB))
  }

  setDropRate(rate: number): void {
    this.dropRate = Math.max(0, Math.min(1, rate))
  }

  setLatencyRange(min: number, max: number): void {
    this.latencyMin = Math.max(0, min)
    this.latencyMax = Math.max(this.latencyMin, max)
  }

  currentDropRate(): number {
    return this.dropRate
  }

  currentLatencyRange(): { min: number; max: number } {
    return { min: this.latencyMin, max: this.latencyMax }
  }
}
