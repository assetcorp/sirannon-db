import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ReplicationError } from '../errors.js'
import { HLC } from '../hlc.js'

describe('HLC', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(1000000)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('produces monotonically increasing timestamps', () => {
    const hlc = new HLC('node1')
    const t1 = hlc.now()
    const t2 = hlc.now()
    const t3 = hlc.now()

    expect(HLC.compare(t1, t2)).toBeLessThan(0)
    expect(HLC.compare(t2, t3)).toBeLessThan(0)
  })

  it('advances the clock on receive', () => {
    const hlcA = new HLC('nodeA')
    const hlcB = new HLC('nodeB')

    vi.setSystemTime(1000000)
    const t1 = hlcA.now()

    vi.setSystemTime(900000)
    const t2 = hlcB.receive(t1)

    expect(HLC.compare(t2, t1)).toBeGreaterThan(0)
  })

  it('throws on logical counter overflow', () => {
    const hlc = new HLC('node1')

    vi.setSystemTime(1000000)

    for (let i = 0; i <= 0xffff; i++) {
      hlc.now()
    }

    expect(() => hlc.now()).toThrow(ReplicationError)
  })

  it('roundtrips encode and decode', () => {
    vi.useRealTimers()
    const hlc = new HLC('abcdef1234567890abcdef1234567890')
    const encoded = hlc.now()
    const decoded = HLC.decode(encoded)

    expect(decoded.nodeId).toBe('abcdef1234567890abcdef1234567890')
    expect(decoded.wallMs).toBeGreaterThan(0)
    expect(decoded.logical).toBe(0)
  })

  it('maintains lexicographic ordering for different wall times', () => {
    const hlc = new HLC('node1')

    vi.setSystemTime(1000)
    const earlier = hlc.now()

    vi.setSystemTime(2000)
    const later = hlc.now()

    expect(earlier < later).toBe(true)
  })

  it('remains monotonic when system clock goes backward', () => {
    const hlc = new HLC('node1')

    vi.setSystemTime(2000000)
    const t1 = hlc.now()

    vi.setSystemTime(1000000)
    const t2 = hlc.now()

    expect(HLC.compare(t1, t2)).toBeLessThan(0)
  })

  it('compare returns correct ordering', () => {
    expect(HLC.compare('aaa', 'bbb')).toBeLessThan(0)
    expect(HLC.compare('bbb', 'aaa')).toBeGreaterThan(0)
    expect(HLC.compare('aaa', 'aaa')).toBe(0)
  })

  it('decodes HLC with dashes in nodeId', () => {
    const decoded = HLC.decode('0000000f4240-0000-node-with-dashes')
    expect(decoded.nodeId).toBe('node-with-dashes')
  })
})
