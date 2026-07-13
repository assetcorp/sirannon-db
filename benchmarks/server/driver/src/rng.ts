// Seeded randomness so a run reproduces the same operation stream given the same seed. Both
// engines are driven by this one generator, so the two sides answer an identical request stream.
// mulberry32 is a small, fast, fully deterministic PRNG; an integer seed fixes the whole sequence.

const ALPHABET = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

export class SeededRng {
  private state: number

  constructor(seed: number) {
    this.state = seed >>> 0
  }

  fraction(): number {
    this.state = (this.state + 0x6d2b79f5) | 0
    let t = this.state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }

  below(exclusiveUpper: number): number {
    return Math.floor(this.fraction() * exclusiveUpper)
  }

  text(length: number): string {
    const chars = new Array<string>(length)
    for (let i = 0; i < length; i++) {
      chars[i] = ALPHABET.charAt(Math.floor(this.fraction() * ALPHABET.length))
    }
    return chars.join('')
  }
}

// YCSB-compatible Zipfian sampler (Gray et al., SIGMOD 1994). theta fixes the skew; the YCSB
// default of 0.99 concentrates traffic on a few hot keys. The zeta constants are precomputed once
// so each sample is O(1).
export class ZipfianGenerator {
  private readonly items: number
  private readonly theta: number
  private readonly zetaN: number
  private readonly alpha: number
  private readonly eta: number

  constructor(items: number, theta = 0.99) {
    if (items < 1) {
      throw new Error(`zipfian item count must be positive, got ${items}`)
    }
    this.items = items
    this.theta = theta
    this.zetaN = ZipfianGenerator.computeZeta(items, theta)
    const zeta2 = ZipfianGenerator.computeZeta(2, theta)
    this.alpha = 1.0 / (1.0 - theta)
    this.eta = (1.0 - (2.0 / items) ** (1.0 - theta)) / (1.0 - zeta2 / this.zetaN)
  }

  next(rng: SeededRng): number {
    const u = rng.fraction()
    const uz = u * this.zetaN
    if (uz < 1.0) {
      return 0
    }
    if (uz < 1.0 + 0.5 ** this.theta) {
      return 1
    }
    return Math.min(this.items - 1, Math.trunc(this.items * (this.eta * u - this.eta + 1.0) ** this.alpha))
  }

  private static computeZeta(n: number, theta: number): number {
    let total = 0.0
    for (let i = 0; i < n; i++) {
      total += 1.0 / (i + 1) ** theta
    }
    return total
  }
}
