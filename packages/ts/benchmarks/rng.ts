let globalRng: SeededRng | null = null

export class SeededRng {
  private s0: bigint
  private s1: bigint
  private s2: bigint
  private s3: bigint

  constructor(seed: bigint) {
    let s = seed
    this.s0 = splitMix64(s)
    s += 0x9e3779b97f4a7c15n
    this.s1 = splitMix64(s)
    s += 0x9e3779b97f4a7c15n
    this.s2 = splitMix64(s)
    s += 0x9e3779b97f4a7c15n
    this.s3 = splitMix64(s)
  }

  next(): number {
    const result = (rotl(this.s1 * 5n, 7n) * 9n) & 0xffffffffffffffffn
    const t = (this.s1 << 17n) & 0xffffffffffffffffn

    this.s2 = (this.s2 ^ this.s0) & 0xffffffffffffffffn
    this.s3 = (this.s3 ^ this.s1) & 0xffffffffffffffffn
    this.s1 = (this.s1 ^ this.s2) & 0xffffffffffffffffn
    this.s0 = (this.s0 ^ this.s3) & 0xffffffffffffffffn
    this.s2 = (this.s2 ^ t) & 0xffffffffffffffffn
    this.s3 = rotl(this.s3, 45n)

    return Number((result >> 11n) & 0x1fffffffffffffn) / 2 ** 53
  }

  nextInt(max: number): number {
    return Math.floor(this.next() * max)
  }

  nextBytes(length: number): Uint8Array {
    const bytes = new Uint8Array(length)
    for (let i = 0; i < length; i++) {
      bytes[i] = this.nextInt(256)
    }
    return bytes
  }
}

function rotl(x: bigint, k: bigint): bigint {
  const masked = x & 0xffffffffffffffffn
  return ((masked << k) | (masked >> (64n - k))) & 0xffffffffffffffffn
}

function splitMix64(seed: bigint): bigint {
  let z = (seed + 0x9e3779b97f4a7c15n) & 0xffffffffffffffffn
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & 0xffffffffffffffffn
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & 0xffffffffffffffffn
  return (z ^ (z >> 31n)) & 0xffffffffffffffffn
}

export function getGlobalRng(): SeededRng {
  if (!globalRng) {
    const seed = BigInt(process.env.BENCH_SEED ?? '42')
    globalRng = new SeededRng(seed)
  }
  return globalRng
}

export function resetGlobalRng(seed?: bigint): void {
  const s = seed ?? BigInt(process.env.BENCH_SEED ?? '42')
  globalRng = new SeededRng(s)
}
