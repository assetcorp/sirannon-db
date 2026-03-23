export class SeededPRNG {
  private state: number

  constructor(seed: number) {
    this.state = seed | 0
  }

  next(): number {
    this.state = (this.state + 0x6d2b79f5) | 0
    let t = Math.imul(this.state ^ (this.state >>> 15), 1 | this.state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 0x100000000
  }

  nextInt(min: number, max: number): number {
    if (min >= max) return min
    return min + Math.floor(this.next() * (max - min))
  }

  nextBool(probability: number): boolean {
    return this.next() < probability
  }
}
