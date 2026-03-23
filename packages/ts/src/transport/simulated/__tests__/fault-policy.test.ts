import { describe, expect, it } from 'vitest'
import { FaultPolicy } from '../fault-policy.js'
import { SeededPRNG } from '../prng.js'

describe('SeededPRNG', () => {
  it('produces the same sequence for the same seed', () => {
    const a = new SeededPRNG(42)
    const b = new SeededPRNG(42)
    for (let i = 0; i < 100; i++) {
      expect(a.next()).toBe(b.next())
    }
  })

  it('produces different sequences for different seeds', () => {
    const a = new SeededPRNG(1)
    const b = new SeededPRNG(2)
    const valuesA = Array.from({ length: 10 }, () => a.next())
    const valuesB = Array.from({ length: 10 }, () => b.next())
    expect(valuesA).not.toEqual(valuesB)
  })

  it('returns values in [0, 1)', () => {
    const prng = new SeededPRNG(99)
    for (let i = 0; i < 1000; i++) {
      const v = prng.next()
      expect(v).toBeGreaterThanOrEqual(0)
      expect(v).toBeLessThan(1)
    }
  })

  it('nextInt returns values in [min, max)', () => {
    const prng = new SeededPRNG(7)
    for (let i = 0; i < 200; i++) {
      const v = prng.nextInt(5, 10)
      expect(v).toBeGreaterThanOrEqual(5)
      expect(v).toBeLessThan(10)
    }
  })

  it('nextInt returns min when min equals max', () => {
    const prng = new SeededPRNG(1)
    expect(prng.nextInt(5, 5)).toBe(5)
  })

  it('nextBool respects probability 0 and 1', () => {
    const prng = new SeededPRNG(1)
    for (let i = 0; i < 50; i++) {
      expect(prng.nextBool(0)).toBe(false)
      expect(prng.nextBool(1)).toBe(true)
    }
  })
})

describe('FaultPolicy', () => {
  it('drops nothing with default config', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({}, prng)
    for (let i = 0; i < 100; i++) {
      expect(policy.shouldDrop('a', 'b', 'batch')).toBe(false)
    }
  })

  it('drops all messages with dropRate 1.0', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({ dropRate: 1.0 }, prng)
    for (let i = 0; i < 50; i++) {
      expect(policy.shouldDrop('a', 'b', 'batch')).toBe(true)
    }
  })

  it('drops approximately the configured rate over many samples', () => {
    const prng = new SeededPRNG(42)
    const policy = new FaultPolicy({ dropRate: 0.3 }, prng)
    let drops = 0
    const total = 10_000
    for (let i = 0; i < total; i++) {
      if (policy.shouldDrop('a', 'b', 'batch')) drops++
    }
    const rate = drops / total
    expect(rate).toBeGreaterThan(0.25)
    expect(rate).toBeLessThan(0.35)
  })

  it('drops messages between partitioned nodes', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({ partitions: [['nodeA', 'nodeB']] }, prng)
    expect(policy.shouldDrop('nodeA', 'nodeB', 'batch')).toBe(true)
    expect(policy.shouldDrop('nodeB', 'nodeA', 'batch')).toBe(true)
    expect(policy.shouldDrop('nodeA', 'nodeC', 'batch')).toBe(false)
  })

  it('adds and removes partitions at runtime', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({}, prng)

    expect(policy.shouldDrop('x', 'y', 'ack')).toBe(false)

    policy.addPartition('x', 'y')
    expect(policy.shouldDrop('x', 'y', 'ack')).toBe(true)
    expect(policy.shouldDrop('y', 'x', 'ack')).toBe(true)

    policy.removePartition('y', 'x')
    expect(policy.shouldDrop('x', 'y', 'ack')).toBe(false)
  })

  it('samples latency within configured range', () => {
    const prng = new SeededPRNG(5)
    const policy = new FaultPolicy({ latencyMin: 10, latencyMax: 50 }, prng)
    for (let i = 0; i < 200; i++) {
      const l = policy.sampleLatency('a', 'b', 'batch')
      expect(l).toBeGreaterThanOrEqual(10)
      expect(l).toBeLessThan(50)
    }
  })

  it('returns latencyMin when min equals max', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({ latencyMin: 20, latencyMax: 20 }, prng)
    expect(policy.sampleLatency('a', 'b', 'batch')).toBe(20)
  })

  it('clamps dropRate to [0, 1]', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({}, prng)

    policy.setDropRate(2.0)
    expect(policy.currentDropRate()).toBe(1.0)

    policy.setDropRate(-0.5)
    expect(policy.currentDropRate()).toBe(0)
  })

  it('clamps latency range to non-negative and min <= max', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({}, prng)

    policy.setLatencyRange(-5, 10)
    expect(policy.currentLatencyRange()).toEqual({ min: 0, max: 10 })

    policy.setLatencyRange(20, 5)
    expect(policy.currentLatencyRange()).toEqual({ min: 20, max: 20 })
  })

  it('reports partition state via isPartitioned', () => {
    const prng = new SeededPRNG(1)
    const policy = new FaultPolicy({}, prng)

    expect(policy.isPartitioned('a', 'b')).toBe(false)
    policy.addPartition('a', 'b')
    expect(policy.isPartitioned('a', 'b')).toBe(true)
    expect(policy.isPartitioned('b', 'a')).toBe(true)
  })
})
