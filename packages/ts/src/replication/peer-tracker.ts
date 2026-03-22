import { WriteConcernError } from './errors.js'
import type { PeerState } from './types.js'

interface Waiter {
  seq: bigint
  count: number
  resolve: () => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

/**
 * Maintains in-memory state for every peer the local node communicates with.
 *
 * For each peer, PeerTracker records the last acknowledged sequence number,
 * the last sent sequence number, pending batch count, and connection status.
 * This state drives two key mechanisms:
 *
 * - **Back-pressure**: the sender loop in ReplicationEngine checks
 *   `pendingBatches` against the configured max before queuing more work
 *   for a given peer.
 * - **Write concern**: callers can await `waitForMajority` or `waitForAll`
 *   with a sequence number and timeout. These methods resolve once enough
 *   peers have acknowledged that sequence, or reject with a
 *   WriteConcernError on timeout.
 */
export class PeerTracker {
  private readonly peers = new Map<string, PeerState>()
  private readonly waiters = new Set<Waiter>()

  addPeer(nodeId: string): void {
    if (this.peers.has(nodeId)) {
      const existing = this.peers.get(nodeId)
      if (existing) {
        existing.connected = true
      }
      return
    }
    this.peers.set(nodeId, {
      nodeId,
      lastAckedSeq: 0n,
      lastSentSeq: 0n,
      lastReceivedHlc: '',
      connected: true,
      pendingBatches: 0,
    })
  }

  removePeer(nodeId: string): void {
    const peer = this.peers.get(nodeId)
    if (peer) {
      peer.connected = false
    }
  }

  onAckReceived(nodeId: string, ackedSeq: bigint): void {
    const peer = this.peers.get(nodeId)
    if (peer && ackedSeq > peer.lastAckedSeq) {
      peer.lastAckedSeq = ackedSeq
      if (peer.pendingBatches > 0) {
        peer.pendingBatches -= 1
      }
    }
    this.checkWaiters()
  }

  getPeerState(nodeId: string): PeerState | undefined {
    return this.peers.get(nodeId)
  }

  connectedPeerCount(): number {
    let count = 0
    for (const peer of this.peers.values()) {
      if (peer.connected) {
        count += 1
      }
    }
    return count
  }

  waitForMajority(seq: bigint, timeoutMs: number): Promise<void> {
    const connected = this.connectedPeerCount()
    const needed = Math.floor(connected / 2) + 1

    if (this.countAcked(seq) >= needed) {
      return Promise.resolve()
    }

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(waiter)
        reject(new WriteConcernError(`Timed out waiting for majority ACK of seq ${seq}`))
      }, timeoutMs)
      timer.unref()

      const waiter: Waiter = { seq, count: needed, resolve, reject, timer }
      this.waiters.add(waiter)
    })
  }

  waitForAll(seq: bigint, timeoutMs: number): Promise<void> {
    const connected = this.connectedPeerCount()

    if (connected === 0 || this.countAcked(seq) >= connected) {
      return Promise.resolve()
    }

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(waiter)
        reject(new WriteConcernError(`Timed out waiting for all peers to ACK seq ${seq}`))
      }, timeoutMs)
      timer.unref()

      const waiter: Waiter = { seq, count: connected, resolve, reject, timer }
      this.waiters.add(waiter)
    })
  }

  allPeerStates(): PeerState[] {
    return Array.from(this.peers.values())
  }

  private countAcked(seq: bigint): number {
    let count = 0
    for (const peer of this.peers.values()) {
      if (peer.connected && peer.lastAckedSeq >= seq) {
        count += 1
      }
    }
    return count
  }

  private checkWaiters(): void {
    for (const waiter of this.waiters) {
      if (this.countAcked(waiter.seq) >= waiter.count) {
        clearTimeout(waiter.timer)
        this.waiters.delete(waiter)
        waiter.resolve()
      }
    }
  }
}
