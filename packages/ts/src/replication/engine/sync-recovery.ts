import { setForeignKeysEnabled } from '../../core/system-catalog/index.js'
import { SyncError } from '../errors.js'
import type { SyncAck } from '../types.js'
import type { ReplicationEngine } from './engine.js'

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
  private responseTimer: UnreffableTimeout | null = null
  private retryTimer: UnreffableTimeout | null = null
  private retryDelayMs = SYNC_RETRY_MIN_DELAY_MS

  constructor(
    private readonly engine: ReplicationEngine,
    private readonly retryAttempt: () => void,
  ) {}

  markRequestSent(requestId: string, sourcePeerId: string): void {
    this.activeRequestId = requestId
    this.clearResponseTimer()
    this.responseTimer = unreffed(
      setTimeout(() => {
        this.responseTimer = null
        if (!this.engine.running || this.activeRequestId !== requestId) return
        const reason = `The sync source sent no data within ${this.engine.syncAckTimeoutMs}ms`
        this.report(reason, requestId, 'sync-response-timeout', sourcePeerId)
        void this.abandonAndRetry(reason)
      }, this.engine.syncAckTimeoutMs),
    )
  }

  markRequestFailed(): void {
    this.activeRequestId = null
    this.clearResponseTimer()
    this.scheduleRetry()
  }

  noteSourceResponded(requestId: string): void {
    if (requestId !== this.activeRequestId) return
    this.clearResponseTimer()
  }

  markSyncReady(): void {
    this.activeRequestId = null
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

  private async abandonAndRetry(reason: string): Promise<void> {
    this.activeRequestId = null
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
