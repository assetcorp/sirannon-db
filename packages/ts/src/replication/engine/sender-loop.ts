import { isGroupMember } from '../coordinator/group-rules.js'
import type { PeerState } from '../peer-types.js'
import { refusePlaintextPeer, withholdsDataFrom } from './encryption-refusal.js'
import type { ReplicationEngine } from './engine.js'
import { ensurePrimaryTermStart, historyOrigins } from './term-history.js'

/**
 * Sends the changes in this node's replication log to its peers in batches, on a timer. Each tick fills a peer's
 * window of `maxPendingBatches` unacknowledged batches, so that a peer that falls behind catches up while new writes
 * arrive. In coordinator mode, the
 * loop sends only while this node holds primary authority and has recorded the start of its term, only to the group's
 * members, and it sends every change of the group's history that the log holds, including the changes that this node
 * applied from earlier primaries; otherwise, the topology's `shouldReplicateTo` picks the peers.
 *
 * @internal
 */
export class SenderLoop {
  private senderTimer: ReturnType<typeof setTimeout> | null = null

  constructor(private readonly engine: ReplicationEngine) {}

  start(): void {
    if (this.senderTimer !== null) return
    this.scheduleTick()
  }

  private scheduleTick(): void {
    const engine = this.engine
    if (!engine.running) {
      this.senderTimer = null
      return
    }

    const timer = setTimeout(async () => {
      if (!engine.running) {
        this.senderTimer = null
        return
      }

      try {
        engine.lastSentSeq = await engine.log.getLocalSeq()
        await this.sendPendingBatches()
        await this.updatePruneBoundary()
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'sender-loop', recoverable: true })
      }

      this.scheduleTick()
    }, engine.batchIntervalMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
    this.senderTimer = timer
  }

  stop(): void {
    if (this.senderTimer !== null) {
      clearTimeout(this.senderTimer)
      this.senderTimer = null
    }
  }

  private async updatePruneBoundary(): Promise<void> {
    const engine = this.engine
    const minAcked = await engine.log.getMinAckedSeq()
    engine.database.holdChangeLogPruning('replication', minAcked)
    if (!engine.tracker) return
    if (minAcked === null) {
      engine.tracker.clearPruneBoundary('replication')
    } else {
      engine.tracker.setPruneBoundary('replication', minAcked)
    }
  }

  private async sendPendingBatches(): Promise<void> {
    const engine = this.engine
    const peers = engine.config.transport.peers()
    const nowMs = Date.now()
    const origins = engine.isCoordinatorMode() ? historyOrigins(engine) : undefined
    if (engine.coordinatorAuthority) await ensurePrimaryTermStart(engine)

    for (const [peerId, peerInfo] of peers) {
      if (!this.shouldReplicateTo(peerId, peerInfo.role)) {
        continue
      }
      if (withholdsDataFrom(engine, peerInfo)) {
        refusePlaintextPeer(engine, peerId)
        continue
      }

      const peerState = engine.peerTracker.getPeerState(peerId)
      if (!peerState) {
        await this.sendNextBatch(peerId, undefined, origins, nowMs)
        continue
      }
      engine.peerTracker.expireTimedOutBatches(peerId, nowMs, engine.ackTimeoutMs)
      while (peerState.pendingBatches < engine.maxPendingBatches) {
        if (!(await this.sendNextBatch(peerId, peerState, origins, nowMs))) break
      }
    }
  }

  private async sendNextBatch(
    peerId: string,
    peerState: PeerState | undefined,
    origins: readonly string[] | undefined,
    nowMs: number,
  ): Promise<boolean> {
    const engine = this.engine
    const fromSeq = peerState?.lastSentSeq ?? engine.lastSentSeq
    const rawBatch = await engine.log.readBatch(fromSeq, engine.batchSize, origins)
    if (!rawBatch) return false
    const batch = engine.decorate({ ...rawBatch, afterSeq: fromSeq })

    const previousSeq = peerState?.lastSentSeq ?? 0n
    if (peerState) {
      peerState.pendingBatches += 1
      peerState.lastSentSeq = batch.toSeq
      engine.peerTracker.recordInFlightBatch(peerId, {
        batchId: batch.batchId,
        fromSeq: batch.fromSeq,
        toSeq: batch.toSeq,
        sentAt: nowMs,
      })
    }

    engine.config.transport.send(peerId, batch).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'transport-send', peerId, recoverable: true })
      if (peerState) {
        const idx = peerState.inFlightBatches.findIndex(b => b.batchId === batch.batchId)
        if (idx >= 0) {
          peerState.inFlightBatches.splice(idx, 1)
        }
        if (peerState.pendingBatches > 0) {
          peerState.pendingBatches -= 1
        }
        if (previousSeq < peerState.lastSentSeq) {
          peerState.lastSentSeq = previousSeq
        }
      }
    })

    if (engine.config.flowControl?.maxLagSeconds && peerState) {
      const lagMs = Number(batch.toSeq - peerState.lastAckedSeq) * engine.batchIntervalMs
      const maxLagMs = engine.config.flowControl.maxLagSeconds * 1000
      if (lagMs > maxLagMs && engine.config.flowControl.onLagExceeded) {
        engine.config.flowControl.onLagExceeded(peerId, lagMs)
      }
    }
    return true
  }

  private shouldReplicateTo(peerId: string, peerRole: 'primary' | 'replica'): boolean {
    const engine = this.engine
    if (!engine.isCoordinatorMode()) {
      return engine.config.topology.shouldReplicateTo(peerId, peerRole)
    }
    const state = engine.coordinatorState
    if (!state || !engine.coordinatorAuthority || peerId === engine.nodeId) return false
    return engine.history.termStartTerm === state.primaryTerm && isGroupMember(state, peerId)
  }
}
