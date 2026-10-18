import { setForeignKeysEnabled } from '../../core/system-catalog/index.js'
import { SyncError } from '../errors.js'
import type { SyncAck } from '../types.js'
import type { ReplicationEngine } from './engine.js'
import { positionReport } from './position-report.js'

export const SYNC_RETRY_MIN_DELAY_MS = 1_000
export const SYNC_RETRY_MAX_DELAY_MS = 30_000

type UnreffableTimeout = ReturnType<typeof setTimeout> & { unref?: () => void }

function unreffed(timer: ReturnType<typeof setTimeout>): UnreffableTimeout {
  const unreffable = timer as UnreffableTimeout
  unreffable.unref?.()
  return unreffable
}

export class SyncRecovery {
  private activeRequestId: string | null = null
  private activeSourcePeerId: string | null = null
  private responseTimer: UnreffableTimeout | null = null
  private retryTimer: UnreffableTimeout | null = null
  private retryDelayMs = SYNC_RETRY_MIN_DELAY_MS

  constructor(
    private readonly engine: ReplicationEngine,
    private readonly retryAttempt: () => void,
  ) {}

  markRequestSent(requestId: string, sourcePeerId: string): void {
    this.activeRequestId = requestId
    this.activeSourcePeerId = sourcePeerId
    this.armResponseTimer(`The sync source sent no data within ${this.engine.syncAckTimeoutMs}ms`)
  }

  markRequestFailed(): void {
    this.activeRequestId = null
    this.activeSourcePeerId = null
    this.clearResponseTimer()
    this.scheduleRetry()
  }

  noteSourceResponded(requestId: string): void {
    if (requestId !== this.activeRequestId) return
    this.clearResponseTimer()
  }

  noteAcknowledged(requestId: string): void {
    if (requestId !== this.activeRequestId) return
    this.armResponseTimer(
      `The sync source sent nothing within ${this.engine.syncAckTimeoutMs}ms of the last acknowledgement`,
    )
  }

  noteCopyReceived(requestId: string): void {
    if (requestId !== this.activeRequestId) return
    this.clearResponseTimer()
  }

  markSyncReady(): void {
    this.activeRequestId = null
    this.activeSourcePeerId = null
    this.clearResponseTimer()
    this.cancelRetry()
    this.retryDelayMs = SYNC_RETRY_MIN_DELAY_MS
  }

  isSourceRejection(ack: SyncAck, fromPeerId: string): boolean {
    const engine = this.engine
    return (
      ack.success === false &&
      this.activeRequestId !== null &&
      ack.requestId === this.activeRequestId &&
      ack.joinerNodeId === engine.nodeId &&
      engine.syncState.phase === 'syncing' &&
      engine.syncState.sourcePeerId === fromPeerId
    )
  }

  handleSourceRejection(ack: SyncAck, fromPeerId: string): void {
    const reason = ack.error ?? 'The sync source refused the request'
    this.report(reason, ack.requestId, 'sync-refused', fromPeerId)
    void this.abandonAndRetry(reason)
  }

  stop(): void {
    this.clearResponseTimer()
    this.cancelRetry()
  }

  private report(reason: string, requestId: string, operation: string, peerId: string): void {
    this.engine.emitError({ error: new SyncError(reason, requestId), operation, peerId, recoverable: true })
  }

  private armResponseTimer(reason: string): void {
    const requestId = this.activeRequestId
    const sourcePeerId = this.activeSourcePeerId
    this.clearResponseTimer()
    if (requestId === null || sourcePeerId === null) return
    this.responseTimer = unreffed(
      setTimeout(() => {
        this.responseTimer = null
        if (!this.engine.running || this.activeRequestId !== requestId) return
        if (this.engine.syncState.phase !== 'syncing') return
        this.report(reason, requestId, 'sync-response-timeout', sourcePeerId)
        void this.abandonAndRetry(reason)
      }, this.engine.syncAckTimeoutMs),
    )
  }

  private async abandonAndRetry(reason: string): Promise<void> {
    this.activeRequestId = null
    this.activeSourcePeerId = null
    this.clearResponseTimer()
    await abandonActiveSync(this.engine, reason)
    if (!this.engine.running) return
    this.scheduleRetry()
  }

  private scheduleRetry(): void {
    this.cancelRetry()
    this.retryTimer = unreffed(
      setTimeout(() => {
        this.retryTimer = null
        this.retryAttempt()
      }, this.retryDelayMs),
    )
    this.retryDelayMs = Math.min(this.retryDelayMs * 2, SYNC_RETRY_MAX_DELAY_MS)
  }

  private cancelRetry(): void {
    if (this.retryTimer === null) return
    clearTimeout(this.retryTimer)
    this.retryTimer = null
  }

  private clearResponseTimer(): void {
    if (this.responseTimer === null) return
    clearTimeout(this.responseTimer)
    this.responseTimer = null
  }
}

/**
 * Tells the source of a finished first copy which sequence the copy holds, so that the source resends every change
 * after that sequence, including the changes that it sent while the joiner was still copying and so dropped.
 *
 * When the send fails, the joiner reports the failure and waits, because it reports its position again when it
 * reconnects to the source.
 *
 * @param engine - The joining node's engine, which sends the report and records a failed send.
 * @param sourcePeerId - The node that served the copy.
 * @param snapshotSeq - The source sequence that the copy holds.
 *
 * @internal
 */
export async function reportSnapshotPosition(
  engine: ReplicationEngine,
  sourcePeerId: string,
  snapshotSeq: bigint,
): Promise<void> {
  try {
    await engine.config.transport.sendAck(sourcePeerId, positionReport(engine, snapshotSeq))
  } catch (err: unknown) {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'sync-position-report', peerId: sourcePeerId, recoverable: true })
  }
}

async function abandonActiveSync(engine: ReplicationEngine, reason: string): Promise<void> {
  engine.syncState.phase = 'pending'
  engine.syncState.sourcePeerId = null
  engine.syncState.startedAt = null
  engine.syncState.error = reason
  engine.expectedBatchIndex.clear()
  engine.syncTableDigests.clear()

  try {
    await setForeignKeysEnabled(engine.writerConn, true)
  } catch (err: unknown) {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'sync-abandon-pragma-restore', recoverable: false })
  }

  try {
    await engine.log.setSyncMeta('pending')
  } catch (err: unknown) {
    const wrappedErr = err instanceof Error ? err : new Error(String(err))
    engine.emitError({ error: wrappedErr, operation: 'sync-meta-write', recoverable: false })
  }
}
