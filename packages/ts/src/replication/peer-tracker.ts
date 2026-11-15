import { HLC } from '../core/sync/hlc.js'
import { WriteConcernError } from './errors.js'
import type { InFlightBatch, PeerState } from './types.js'

interface Waiter {
  seq: bigint
  kind: 'majority' | 'all' | 'configured-majority' | 'configured-all' | 'condition'
  localNodeId?: string
  votingNodeIds?: string[]
  met?: () => boolean
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
 *   timeout expires. `waitForMajority` and `waitForAll` count only the peers
 *   connected at each check, so the peers that remain connected can meet the
 *   concern after another peer disconnects. `waitForConfiguredMajority`,
 *   `waitForConfiguredAll`, and `ackedConfiguredNodeIds` count only the
 *   acknowledgements that a peer sent over its current connection, so that a
 *   peer that reconnects, possibly with a replaced file, proves its position
 *   again before it counts.
 *
 * @internal
 */
export class PeerTracker {
  private readonly peers = new Map<string, PeerState>()
  private readonly connectionAcks = new Map<string, bigint>()
  private readonly waiters = new Set<Waiter>()

  addPeer(nodeId: string): void {
    this.connectionAcks.set(nodeId, 0n)
    const existing = this.peers.get(nodeId)
    if (existing) {
      existing.connected = true
      this.resumeFrom(nodeId, existing.lastAckedSeq)
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
    this.connectionAcks.delete(nodeId)
    this.checkWaiters()
  }

  /**
   * Discards the acknowledgements that a peer sent over its current connection, so that the peer counts towards a
   * write concern or an admission only after it acknowledges again. Call it when the peer's coordinator session
   * changes, because a new session can belong to a process with another file, and when the peer starts a full copy,
   * because the copy replaces the rows that those acknowledgements describe.
   *
   * @param nodeId - The peer's node ID.
   */
  forgetConnectionAcks(nodeId: string): void {
    if (this.connectionAcks.has(nodeId)) this.connectionAcks.set(nodeId, 0n)
  }

  onAckReceived(nodeId: string, ackedSeq: bigint): void {
    const connectionAcked = this.connectionAcks.get(nodeId)
    if (connectionAcked !== undefined && ackedSeq > connectionAcked) this.connectionAcks.set(nodeId, ackedSeq)
    const peer = this.peers.get(nodeId)
    if (peer && ackedSeq > peer.lastAckedSeq) {
      peer.lastAckedSeq = ackedSeq
      const ackedCount = peer.inFlightBatches.filter(b => b.toSeq <= ackedSeq).length
      peer.inFlightBatches = peer.inFlightBatches.filter(b => b.toSeq > ackedSeq)
      peer.pendingBatches = Math.max(0, peer.pendingBatches - Math.max(1, ackedCount))
    }
    this.checkWaiters()
  }

  resumeFrom(nodeId: string, appliedSeq: bigint): void {
    const peer = this.peers.get(nodeId)
    if (!peer) return
    peer.lastSentSeq = appliedSeq
    peer.inFlightBatches = []
    peer.pendingBatches = 0
  }

  restartProgress(sentSeq: bigint): void {
    for (const nodeId of this.connectionAcks.keys()) this.connectionAcks.set(nodeId, 0n)
    for (const peer of this.peers.values()) {
      peer.lastAckedSeq = 0n
      peer.lastSentSeq = sentSeq
      peer.inFlightBatches = []
      peer.pendingBatches = 0
    }
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

  /**
   * Returns once a condition holds, checking it now, after every acknowledgement, and after every call to
   * {@link PeerTracker.recheckWaiters}. The condition runs synchronously, so that a caller can record its outcome in
   * the same step in which it holds.
   *
   * @param met - Returns true once the wait is over.
   * @param timeoutMs - How many milliseconds to wait before the promise rejects.
   * @param timeoutMessage - The message of the error that the promise rejects with.
   * @returns A promise that resolves once `met` returns true.
   * @throws WriteConcernError when `met` still returns false after `timeoutMs`.
   */
  waitUntil(met: () => boolean, timeoutMs: number, timeoutMessage: string): Promise<void> {
    if (met()) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(waiter)
        reject(new WriteConcernError(timeoutMessage))
      }, timeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
      timer.unref?.()
      const waiter: Waiter = { seq: 0n, kind: 'condition', met, resolve, reject, timer }
      this.waiters.add(waiter)
    })
  }

  /**
   * Checks every waiting condition again, for a change that is no acknowledgement, such as a new in-sync set.
   */
  recheckWaiters(): void {
    this.checkWaiters()
  }

  /**
   * Returns the highest sequence that a peer has acknowledged over its current connection.
   *
   * @param nodeId - The peer's node ID.
   * @returns That sequence, 0 before the peer acknowledges anything, or undefined while the peer is not connected.
   */
  connectionAckedSeq(nodeId: string): bigint | undefined {
    return this.connectionAcks.get(nodeId)
  }

  ackedConfiguredNodeIds(seq: bigint, localNodeId: string, votingNodeIds: string[]): string[] {
    const acked = new Set<string>()
    if (votingNodeIds.includes(localNodeId)) {
      acked.add(localNodeId)
    }
    for (const nodeId of votingNodeIds) {
      const connectionAcked = this.connectionAcks.get(nodeId)
      if (connectionAcked !== undefined && connectionAcked >= seq) {
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
      if (waiter.kind === 'condition') {
        if (waiter.met?.()) {
          clearTimeout(waiter.timer)
          this.waiters.delete(waiter)
          waiter.resolve()
        }
        continue
      }
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
      }
    }
  }
}
