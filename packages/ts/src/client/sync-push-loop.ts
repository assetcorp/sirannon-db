import type { DeviceSyncPort } from '../core/database-sync.js'
import { unrefTimer } from './http-json.js'
import type { MigrationSyncStatus } from './migration-sync.js'
import { pushSyncBatch } from './sync-push.js'
import { RemoteError } from './types.js'

export interface PushLoopConfig {
  baseUrl: string
  databaseId: string
  headers?: Record<string, string>
  requestTimeout?: number
  batchSize: number
  intervalMs: number
  maxRetryDelayMs: number
}

export interface PushLoopHooks {
  isRunning(): boolean
  port(): DeviceSyncPort | null
  schemaVersion(): number
  reconcileSchema(): Promise<MigrationSyncStatus>
  recordError(err: unknown): void
  onDrained(): void
}

/**
 * Drains this device's outbox to the server after the durable push cursor.
 *
 * A failure backs off exponentially to the configured cap. A push the server
 * refuses with `MIGRATION_REQUIRED` reconciles migrations and clears the
 * backoff so that a device that fell behind the server schema recovers on its own
 * rather than retrying a refusal it cannot satisfy.
 */
export class PushLoop {
  private timer: ReturnType<typeof setInterval> | null = null
  private pushing = false
  private consecutiveFailures = 0
  private nextAttemptAt = 0
  cursor = 0n

  constructor(
    private readonly config: PushLoopConfig,
    private readonly hooks: PushLoopHooks,
  ) {}

  start(): void {
    if (this.timer !== null) return
    this.timer = setInterval(() => {
      void this.drain()
    }, this.config.intervalMs)
    unrefTimer(this.timer)
  }

  stop(): void {
    if (this.timer === null) return
    clearInterval(this.timer)
    this.timer = null
  }

  /**
   * Empties the outbox before a snapshot replaces the database so that local work
   * reaches the server before the copy overwrites it. A schema refusal is
   * reconciled once; a device that still cannot push returns rather than
   * looping, leaving the snapshot to bring it back into line.
   */
  async drainFully(port: DeviceSyncPort): Promise<void> {
    let retriedAfterMigration = false
    for (;;) {
      try {
        if (!(await this.pushNextBatch(port))) return
      } catch (err) {
        if (!(err instanceof RemoteError) || err.code !== 'MIGRATION_REQUIRED') throw err
        if (retriedAfterMigration) return
        retriedAfterMigration = true
        const status = await this.hooks.reconcileSchema().catch(() => 'resync-required' as const)
        if (status === 'ahead') throw err
        if (status === 'resync-required') return
      }
    }
  }

  async drain(): Promise<void> {
    const port = this.hooks.port()
    if (this.pushing || !this.hooks.isRunning() || port === null) return
    if (Date.now() < this.nextAttemptAt) return
    this.pushing = true
    try {
      while (this.hooks.isRunning()) {
        const pushed = await this.pushNextBatch(port)
        if (!pushed) break
        this.consecutiveFailures = 0
        this.nextAttemptAt = 0
        this.hooks.onDrained()
      }
    } catch (err) {
      await this.handleFailure(err)
    } finally {
      this.pushing = false
    }
  }

  private async handleFailure(err: unknown): Promise<void> {
    this.hooks.recordError(err)
    this.consecutiveFailures += 1
    const delay = Math.min(this.config.intervalMs * 2 ** this.consecutiveFailures, this.config.maxRetryDelayMs)
    this.nextAttemptAt = Date.now() + delay
    if (!(err instanceof RemoteError) || err.code !== 'MIGRATION_REQUIRED' || !this.hooks.isRunning()) return
    try {
      const status = await this.hooks.reconcileSchema()
      if (status === 'migrated' || status === 'in-sync') {
        this.consecutiveFailures = 0
        this.nextAttemptAt = 0
      }
    } catch (reconcileErr) {
      this.hooks.recordError(reconcileErr)
    }
  }

  private async pushNextBatch(port: DeviceSyncPort): Promise<boolean> {
    const batch = await port.readOutboxBatch(this.cursor, this.config.batchSize)
    if (batch === null) return false
    await pushSyncBatch(
      this.config.baseUrl,
      this.config.databaseId,
      batch,
      this.config.headers,
      this.config.requestTimeout,
      this.hooks.schemaVersion(),
    )
    this.cursor = batch.toSeq
    await port.setPushCursor(batch.toSeq)
    port.protectUnpushedChanges(batch.toSeq)
    return true
  }
}
