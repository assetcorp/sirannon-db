import { HLC } from '../core/sync/hlc.js'
import { WriteConcernError } from './errors.js'
import type { InFlightBatch, PeerState } from './types.js'

interface Waiter {
  seq: bigint
  kind: 'majority' | 'all' | 'configured-majority' | 'configured-all'
  localNodeId?: string
  votingNodeIds?: string[]
  resolve: () => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

/**
 * Keeps in memory the replication state of every peer that this node connects to.
 *
 * For each peer, the tracker records the last sequence number that the peer
 * acknowledged, the last sequence number that this node sent, the number of
 * pending batches, the batches in flight, and whether the peer is connected.
 * The engine uses that state in two places.
 *
 * - Back-pressure: the sender loop skips a peer whose `pendingBatches` has
 *   reached the configured maximum.
 * - Write concern: a caller awaits `waitForMajority`, `waitForAll`,
 *   `waitForConfiguredMajority`, or `waitForConfiguredAll` with a sequence
 *   number and a timeout, and the promise resolves once enough peers
 *   acknowledge that sequence. It rejects with a `WriteConcernError` when the
 *   timeout expires, and a `waitForMajority` or `waitForAll` promise also
 *   rejects early when too few peers are connected to reach the count.
 *
 * @internal
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
      inFlightBatches: [],
    })
  }

  removePeer(nodeId: string): void {
    const peer = this.peers.get(nodeId)
    if (peer) {
      peer.connected = false
    }
    this.checkWaiters()
  }

  onAckReceived(nodeId: string, ackedSeq: bigint): void {
    const peer = this.peers.get(nodeId)
    if (peer && ackedSeq > peer.lastAckedSeq) {
      peer.lastAckedSeq = ackedSeq
      const ackedCount = peer.inFlightBatches.filter(b => b.toSeq <= ackedSeq).length
      peer.inFlightBatches = peer.inFlightBatches.filter(b => b.toSeq > ackedSeq)
      peer.pendingBatches = Math.max(0, peer.pendingBatches - Math.max(1, ackedCount))
    }
    this.checkWaiters()
  }

  onBatchApplied(nodeId: string, highestHlc: string): void {
    const peer = this.peers.get(nodeId)
    if (peer && (peer.lastReceivedHlc === '' || HLC.compare(highestHlc, peer.lastReceivedHlc) > 0)) {
      peer.lastReceivedHlc = highestHlc
    }
  }

  recordInFlightBatch(nodeId: string, batch: InFlightBatch): void {
    const peer = this.peers.get(nodeId)
    if (peer) {
      peer.inFlightBatches.push(batch)
    }
  }

  expireTimedOutBatches(nodeId: string, nowMs: number, timeoutMs: number): boolean {
    const peer = this.peers.get(nodeId)
    if (!peer || peer.inFlightBatches.length === 0) return false

    const timedOut = peer.inFlightBatches.filter(b => nowMs - b.sentAt >= timeoutMs)
    if (timedOut.length === 0) return false

    let earliestFromSeq = timedOut[0].fromSeq
    for (let i = 1; i < timedOut.length; i++) {
      if (timedOut[i].fromSeq < earliestFromSeq) {
        earliestFromSeq = timedOut[i].fromSeq
      }
    }

    peer.inFlightBatches = peer.inFlightBatches.filter(b => nowMs - b.sentAt < timeoutMs)
    peer.pendingBatches = Math.max(0, peer.pendingBatches - timedOut.length)

    const resetTarget = earliestFromSeq > 0n ? earliestFromSeq - 1n : 0n
    if (peer.lastSentSeq > resetTarget) {
      peer.lastSentSeq = resetTarget
    }

    return true
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
      }, timeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
      timer.unref?.()

      const waiter: Waiter = { seq, kind: 'majority', resolve, reject, timer }
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
      }, timeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
      timer.unref?.()

      const waiter: Waiter = { seq, kind: 'all', resolve, reject, timer }
      this.waiters.add(waiter)
    })
  }

  waitForConfiguredMajority(
    seq: bigint,
    localNodeId: string,
    votingNodeIds: string[],
    timeoutMs: number,
  ): Promise<void> {
    const needed = Math.floor(votingNodeIds.length / 2) + 1

    if (this.countAckedConfigured(seq, localNodeId, votingNodeIds) >= needed) {
      return Promise.resolve()
    }

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(waiter)
        reject(new WriteConcernError(`Timed out waiting for configured majority ACK of seq ${seq}`))
      }, timeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
      timer.unref?.()

      const waiter: Waiter = {
        seq,
        kind: 'configured-majority',
        localNodeId,
        votingNodeIds: [...votingNodeIds],
        resolve,
        reject,
        timer,
      }
      this.waiters.add(waiter)
    })
  }

  waitForConfiguredAll(seq: bigint, localNodeId: string, votingNodeIds: string[], timeoutMs: number): Promise<void> {
    if (this.countAckedConfigured(seq, localNodeId, votingNodeIds) >= votingNodeIds.length) {
      return Promise.resolve()
    }

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(waiter)
        reject(new WriteConcernError(`Timed out waiting for all configured voters to ACK seq ${seq}`))
      }, timeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
      timer.unref?.()

      const waiter: Waiter = {
        seq,
        kind: 'configured-all',
        localNodeId,
        votingNodeIds: [...votingNodeIds],
        resolve,
        reject,
        timer,
      }
      this.waiters.add(waiter)
    })
  }

  ackedConfiguredNodeIds(seq: bigint, localNodeId: string, votingNodeIds: string[]): string[] {
    const acked = new Set<string>()
    if (votingNodeIds.includes(localNodeId)) {
      acked.add(localNodeId)
    }
    for (const nodeId of votingNodeIds) {
      const peer = this.peers.get(nodeId)
      if (peer && peer.lastAckedSeq >= seq) {
        acked.add(nodeId)
      }
    }
    return votingNodeIds.filter(nodeId => acked.has(nodeId))
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

  private countAckedConfigured(seq: bigint, localNodeId: string, votingNodeIds: string[]): number {
    return this.ackedConfiguredNodeIds(seq, localNodeId, votingNodeIds).length
  }

  private checkWaiters(): void {
    const connected = this.connectedPeerCount()
    for (const waiter of this.waiters) {
      let needed: number
      if (waiter.kind === 'majority') {
        needed = Math.floor(connected / 2) + 1
      } else if (waiter.kind === 'configured-majority') {
        const votingNodeIds = waiter.votingNodeIds ?? []
        const localNodeId = waiter.localNodeId ?? ''
        needed = Math.floor(votingNodeIds.length / 2) + 1
        if (this.countAckedConfigured(waiter.seq, localNodeId, votingNodeIds) >= needed) {
          clearTimeout(waiter.timer)
          this.waiters.delete(waiter)
          waiter.resolve()
        }
        continue
      } else if (waiter.kind === 'configured-all') {
        const votingNodeIds = waiter.votingNodeIds ?? []
        const localNodeId = waiter.localNodeId ?? ''
        needed = votingNodeIds.length
        if (this.countAckedConfigured(waiter.seq, localNodeId, votingNodeIds) >= needed) {
          clearTimeout(waiter.timer)
          this.waiters.delete(waiter)
          waiter.resolve()
        }
        continue
      } else {
        needed = connected
      }
      if (connected === 0 || this.countAcked(waiter.seq) >= needed) {
        clearTimeout(waiter.timer)
        this.waiters.delete(waiter)
        waiter.resolve()
        continue
      }
      if (waiter.kind === 'all' && connected < this.peers.size) {
        clearTimeout(waiter.timer)
        this.waiters.delete(waiter)
        waiter.reject(
          new WriteConcernError(
            `Cannot satisfy 'all' write concern: only ${connected}/${this.peers.size} peers connected`,
          ),
        )
        continue
      }
      const totalNodes = this.peers.size + 1
      const majorityNeeded = Math.floor(totalNodes / 2) + 1
      if (waiter.kind === 'majority' && connected + 1 < majorityNeeded) {
        clearTimeout(waiter.timer)
        this.waiters.delete(waiter)
        waiter.reject(
          new WriteConcernError(
            `Cannot satisfy 'majority' write concern: only ${connected + 1}/${totalNodes} nodes reachable`,
          ),
        )
      }
    }
  }
}
