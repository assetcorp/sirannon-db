import type { ReplicationEngine } from './engine.js'

export class SenderLoop {
  private senderTimer: ReturnType<typeof setTimeout> | null = null

  constructor(private readonly engine: ReplicationEngine) {}

  start(): void {
    const engine = this.engine
    if (!engine.running) return

    const timer = setTimeout(async () => {
      if (!engine.running) return

      try {
        engine.lastSentSeq = await engine.log.getLocalSeq()
        await this.sendPendingBatches()
        await this.updatePruneBoundary()
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'sender-loop', recoverable: true })
      }

      this.start()
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
    if (!engine.tracker) return
    const minAcked = await engine.log.getMinAckedSeq()
    if (minAcked === null) {
      engine.tracker.clearPruneBoundary()
    } else {
      engine.tracker.setPruneBoundary(minAcked)
    }
  }

  private async sendPendingBatches(): Promise<void> {
    const engine = this.engine
    const peers = engine.config.transport.peers()
    const nowMs = Date.now()

    for (const [peerId, peerInfo] of peers) {
      if (!this.shouldReplicateTo(peerId, peerInfo.role)) {
        continue
      }

      const peerState = engine.peerTracker.getPeerState(peerId)
      if (peerState) {
        engine.peerTracker.expireTimedOutBatches(peerId, nowMs, engine.ackTimeoutMs)
      }

      if (peerState && peerState.pendingBatches >= engine.maxPendingBatches) {
        continue
      }

      const fromSeq = peerState?.lastSentSeq ?? engine.lastSentSeq
      const rawBatch = await engine.log.readBatch(fromSeq, engine.batchSize)
      if (!rawBatch) continue
      const batch = engine.decorateBatch(rawBatch)

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
    }
  }

  private shouldReplicateTo(peerId: string, peerRole: 'primary' | 'replica'): boolean {
    const engine = this.engine
    if (!engine.isCoordinatorMode()) {
      return engine.config.topology.shouldReplicateTo(peerId, peerRole)
    }
    return engine.coordinatorAuthority && peerId !== engine.nodeId
  }
}
