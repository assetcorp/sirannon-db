import { beforeEach, describe, expect, it, vi } from 'vitest'
import { WriteConcernError } from '../errors.js'
import { PeerTracker } from '../peer-tracker.js'

describe('PeerTracker', () => {
  let tracker: PeerTracker

  beforeEach(() => {
    tracker = new PeerTracker()
  })

  it('tracks connected peer count', () => {
    expect(tracker.connectedPeerCount()).toBe(0)
    tracker.addPeer('a')
    tracker.addPeer('b')
    tracker.addPeer('c')
    expect(tracker.connectedPeerCount()).toBe(3)
  })

  it('marks peers as disconnected on remove', () => {
    tracker.addPeer('a')
    tracker.addPeer('b')
    tracker.removePeer('a')
    expect(tracker.connectedPeerCount()).toBe(1)
    const state = tracker.getPeerState('a')
    expect(state?.connected).toBe(false)
  })

  it('re-adds a removed peer as connected', () => {
    tracker.addPeer('a')
    tracker.removePeer('a')
    tracker.addPeer('a')
    expect(tracker.getPeerState('a')?.connected).toBe(true)
  })

  it('updates acked seq on ACK', () => {
    tracker.addPeer('a')
    tracker.onAckReceived('a', 5n)
    expect(tracker.getPeerState('a')?.lastAckedSeq).toBe(5n)
  })

  it('does not decrease acked seq', () => {
    tracker.addPeer('a')
    tracker.onAckReceived('a', 10n)
    tracker.onAckReceived('a', 5n)
    expect(tracker.getPeerState('a')?.lastAckedSeq).toBe(10n)
  })

  it('lists all peer states', () => {
    tracker.addPeer('a')
    tracker.addPeer('b')
    const states = tracker.allPeerStates()
    expect(states).toHaveLength(2)
  })

  describe('waitForMajority', () => {
    it('resolves immediately when enough ACKs already present', async () => {
      tracker.addPeer('a')
      tracker.addPeer('b')
      tracker.addPeer('c')
      tracker.onAckReceived('a', 10n)
      tracker.onAckReceived('b', 10n)

      await expect(tracker.waitForMajority(10n, 1000)).resolves.toBeUndefined()
    })

    it('resolves when ACKs arrive (3 peers, need 2)', async () => {
      tracker.addPeer('a')
      tracker.addPeer('b')
      tracker.addPeer('c')

      const promise = tracker.waitForMajority(5n, 5000)
      tracker.onAckReceived('a', 5n)
      tracker.onAckReceived('b', 5n)

      await expect(promise).resolves.toBeUndefined()
    })

    it('rejects on timeout', async () => {
      vi.useFakeTimers()
      tracker.addPeer('a')
      tracker.addPeer('b')
      tracker.addPeer('c')

      const promise = tracker.waitForMajority(5n, 100)
      vi.advanceTimersByTime(150)

      await expect(promise).rejects.toThrow(WriteConcernError)
      vi.useRealTimers()
    })
  })

  describe('waitForAll', () => {
    it('resolves when no peers are connected', async () => {
      await expect(tracker.waitForAll(5n, 1000)).resolves.toBeUndefined()
    })

    it('resolves when all peers ACK', async () => {
      tracker.addPeer('a')
      tracker.addPeer('b')

      const promise = tracker.waitForAll(5n, 5000)
      tracker.onAckReceived('a', 5n)
      tracker.onAckReceived('b', 5n)

      await expect(promise).resolves.toBeUndefined()
    })

    it('rejects on timeout if not all peers ACK', async () => {
      vi.useFakeTimers()
      tracker.addPeer('a')
      tracker.addPeer('b')

      const promise = tracker.waitForAll(5n, 100)
      tracker.onAckReceived('a', 5n)
      vi.advanceTimersByTime(150)

      await expect(promise).rejects.toThrow(WriteConcernError)
      vi.useRealTimers()
    })
  })
})
