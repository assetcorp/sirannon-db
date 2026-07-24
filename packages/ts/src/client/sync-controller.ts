import type { Database } from '../core/database.js'
import type { DeviceSyncPort } from '../core/database-sync.js'
import type { ChangeEvent } from '../core/types.js'
import { toBaseUrl, toWsUrl } from './endpoint-urls.js'
import { unrefTimer } from './http-json.js'
import type { SnapshotProgress } from './snapshot-loader.js'
import { downloadDatabaseSnapshot } from './snapshot-loader.js'
import { pushSyncBatch } from './sync-push.js'
import { WebSocketTransport } from './transport/ws.js'
import type { RemoteSubscription } from './types.js'

const DEFAULT_BATCH_SIZE = 100
const DEFAULT_PUSH_INTERVAL_MS = 1_000
const DEFAULT_ACK_INTERVAL_MS = 2_000
const DEFAULT_MAX_PUSH_RETRY_DELAY_MS = 30_000
const DEFAULT_SNAPSHOT_RETRY_DELAY_MS = 5_000
const DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS = 300_000

export interface SyncControllerOptions {
  url: string
  databaseId: string
  tables: readonly string[]
  headers?: Record<string, string>
  batchSize?: number
  pushIntervalMs?: number
  ackIntervalMs?: number
  maxPushRetryDelayMs?: number
  requestTimeout?: number
  autoResync?: boolean
  snapshotRetryDelayMs?: number
  maxSnapshotRetryDelayMs?: number
  snapshotPageSize?: number
  onChange?: (event: ChangeEvent) => void
  onResyncRequired?: () => void
  onSnapshotProgress?: (progress: SnapshotProgress) => void
}

export type SyncState = 'stopped' | 'starting' | 'running' | 'paused' | 'snapshotting'

export interface SnapshotOptions {
  pageSize?: number
  onProgress?: (progress: SnapshotProgress) => void
}

export interface SyncStatus {
  state: SyncState
  deviceId: string | null
  pendingPushCount: number
  lastPushedSeq: bigint
  lastPulledSeq: bigint | null
  pushCaughtUp: boolean
  resyncRequired: boolean
  lastError: { code: string; message: string } | null
}

export class SyncController {
  private readonly baseUrl: string
  private readonly wsBaseUrl: string
  private readonly batchSize: number
  private readonly pushIntervalMs: number
  private readonly ackIntervalMs: number
  private readonly maxPushRetryDelayMs: number

  private port: DeviceSyncPort | null = null
  private deviceId: string | null = null
  private state: SyncState = 'stopped'
  private pushCursor = 0n
  private pullSeq: bigint | null = null
  private pullEpoch: string | undefined
  private lastAckedSeq: bigint | null = null
  private transport: WebSocketTransport | null = null
  private subscriptions: RemoteSubscription[] = []
  private pushTimer: ReturnType<typeof setInterval> | null = null
  private ackTimer: ReturnType<typeof setTimeout> | null = null
  private pushing = false
  private consecutivePushFailures = 0
  private nextPushAttemptAt = 0
  private pushCaughtUp = false
  private resyncRequired = false
  private resyncTimer: ReturnType<typeof setTimeout> | null = null
  private consecutiveResyncFailures = 0
  private lastError: { code: string; message: string } | null = null

  constructor(
    private readonly db: Database,
    private readonly options: SyncControllerOptions,
  ) {
    this.baseUrl = toBaseUrl(options.url)
    this.wsBaseUrl = toWsUrl(this.baseUrl)
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE
    this.pushIntervalMs = options.pushIntervalMs ?? DEFAULT_PUSH_INTERVAL_MS
    this.ackIntervalMs = options.ackIntervalMs ?? DEFAULT_ACK_INTERVAL_MS
    this.maxPushRetryDelayMs = options.maxPushRetryDelayMs ?? DEFAULT_MAX_PUSH_RETRY_DELAY_MS
  }

  async start(): Promise<void> {
    if (this.state === 'running' || this.state === 'starting') return
    this.state = 'starting'
    try {
      this.port ??= this.db.deviceSync()
      this.deviceId = (await this.port.identity()).nodeId
      this.pushCursor = await this.port.getPushCursor()
      this.port.protectUnpushedChanges(this.pushCursor)
      const pullState = await this.port.getPullState()
      this.pullSeq = pullState?.seq ?? null
      this.pullEpoch = pullState?.epoch
      this.lastAckedSeq = null
      if (await this.port.snapshotLoadPending()) {
        this.resyncRequired = true
      }
      await this.openPullStream()
      this.state = 'running'
    } catch (err) {
      this.teardownStream()
      this.state = 'stopped'
      throw err
    }
    this.pushTimer = setInterval(() => {
      void this.drainOutbox()
    }, this.pushIntervalMs)
    unrefTimer(this.pushTimer)
    void this.drainOutbox()
    if (this.resyncRequired) {
      this.scheduleAutoResync()
    }
  }

  pause(): void {
    if (this.state !== 'running') return
    this.teardownStream()
    this.state = 'paused'
    void this.persistPullState()
  }

  async resume(): Promise<void> {
    if (this.state !== 'paused') return
    this.state = 'stopped'
    await this.start()
  }

  async stop(): Promise<void> {
    if (this.state === 'stopped') return
    this.teardownStream()
    this.state = 'stopped'
    await this.persistPullState()
  }

  async status(): Promise<SyncStatus> {
    const pendingPushCount = this.port ? await this.port.countOutboxPending(this.pushCursor) : 0
    return {
      state: this.state,
      deviceId: this.deviceId,
      pendingPushCount,
      lastPushedSeq: this.pushCursor,
      lastPulledSeq: this.pullSeq,
      pushCaughtUp: this.pushCaughtUp,
      resyncRequired: this.resyncRequired,
      lastError: this.lastError,
    }
  }

  triggerPush(): void {
    void this.drainOutbox()
  }

  private async openPullStream(): Promise<void> {
    const deviceId = this.deviceId
    if (deviceId === null) return
    const encodedId = encodeURIComponent(this.options.databaseId)
    const transport = new WebSocketTransport(`${this.wsBaseUrl}/db/${encodedId}`, {
      requestTimeout: this.options.requestTimeout,
    })
    this.transport = transport
    for (const table of this.options.tables) {
      const subscription = await transport.subscribe(table, undefined, event => this.handlePullEvent(event), {
        deviceId,
        sinceSeq: this.pullSeq ?? undefined,
        epoch: this.pullEpoch,
        onReset: () => this.handlePullReset(),
        onSubscribed: info => this.handleSubscribed(info),
      })
      this.subscriptions.push(subscription)
    }
  }

  private handleSubscribed(info: { seq: bigint | undefined; epoch: string | undefined; resync: boolean }): void {
    if (info.epoch !== undefined) {
      this.pullEpoch = info.epoch
    }
    if (info.resync) {
      this.resyncRequired = true
      if (info.seq !== undefined) {
        this.pullSeq = info.seq
      }
      this.scheduleAutoResync()
    } else if (this.pullSeq === null && info.seq !== undefined) {
      this.pullSeq = info.seq
    }
  }

  private handlePullEvent(event: ChangeEvent): void {
    if (this.pullSeq === null || event.seq > this.pullSeq) {
      this.pullSeq = event.seq
    }
    try {
      this.options.onChange?.(event)
    } finally {
      this.scheduleAckFlush()
    }
  }

  private handlePullReset(): void {
    this.resyncRequired = true
    try {
      this.options.onResyncRequired?.()
    } catch {}
    this.scheduleAutoResync()
  }

  private scheduleAutoResync(): void {
    if (this.options.autoResync === false) return
    if (this.resyncTimer !== null || this.state === 'snapshotting') return
    const baseDelay = this.options.snapshotRetryDelayMs ?? DEFAULT_SNAPSHOT_RETRY_DELAY_MS
    const maxDelay = this.options.maxSnapshotRetryDelayMs ?? DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS
    const delay =
      this.consecutiveResyncFailures === 0
        ? 0
        : Math.min(baseDelay * 2 ** (this.consecutiveResyncFailures - 1), maxDelay)
    this.resyncTimer = setTimeout(() => {
      this.resyncTimer = null
      void this.attemptAutoResync()
    }, delay)
    unrefTimer(this.resyncTimer)
  }

  private async attemptAutoResync(): Promise<void> {
    if (this.state !== 'running') return
    if (!this.resyncRequired) return
    try {
      await this.downloadSnapshot({
        pageSize: this.options.snapshotPageSize,
        onProgress: this.options.onSnapshotProgress,
      })
    } catch {
      this.scheduleAutoResync()
    }
  }

  private scheduleAckFlush(): void {
    if (this.ackTimer !== null) return
    this.ackTimer = setTimeout(() => {
      this.ackTimer = null
      void this.flushAck()
    }, this.ackIntervalMs)
    unrefTimer(this.ackTimer)
  }

  private async flushAck(): Promise<void> {
    if (this.state !== 'running') return
    const deviceId = this.deviceId
    const transport = this.transport
    const seq = this.pullSeq
    if (deviceId === null || transport === null || seq === null) return
    if (this.lastAckedSeq !== null && seq <= this.lastAckedSeq) return
    try {
      await this.persistPullState()
      await transport.ack(deviceId, seq)
      this.lastAckedSeq = seq
    } catch (err) {
      this.recordError(err)
      this.scheduleAckFlush()
    }
  }

  private async persistPullState(): Promise<void> {
    if (this.port === null || this.pullSeq === null) return
    try {
      await this.port.setPullState(this.pullSeq, this.pullEpoch)
    } catch (err) {
      this.recordError(err)
    }
  }

  async downloadSnapshot(options?: SnapshotOptions): Promise<void> {
    if (this.state === 'snapshotting') {
      throw new Error('A snapshot download is already in progress')
    }
    if (this.state !== 'running' && this.state !== 'paused') {
      throw new Error('Snapshot download requires a started sync controller')
    }
    const port = this.port
    if (port === null) {
      throw new Error('Snapshot download requires a started sync controller')
    }

    this.teardownStream()
    this.state = 'snapshotting'
    try {
      while (await this.pushNextBatch(port)) {}
      await downloadDatabaseSnapshot(port, {
        url: this.baseUrl,
        databaseId: this.options.databaseId,
        headers: this.options.headers,
        pageSize: options?.pageSize,
        requestTimeoutMs: this.options.requestTimeout,
        onProgress: options?.onProgress,
      })
      this.resyncRequired = false
      this.consecutiveResyncFailures = 0
      this.lastError = null
    } catch (err) {
      this.recordError(err)
      this.resyncRequired = true
      this.consecutiveResyncFailures += 1
      this.state = 'stopped'
      try {
        await this.start()
      } catch {
        this.state = 'paused'
      }
      throw err
    }
    this.state = 'stopped'
    await this.start()
  }

  private async pushNextBatch(port: DeviceSyncPort): Promise<boolean> {
    const batch = await port.readOutboxBatch(this.pushCursor, this.batchSize)
    if (batch === null) {
      this.pushCaughtUp = true
      return false
    }
    this.pushCaughtUp = false
    await pushSyncBatch(this.baseUrl, this.options.databaseId, batch, this.options.headers)
    this.pushCursor = batch.toSeq
    await port.setPushCursor(batch.toSeq)
    port.protectUnpushedChanges(batch.toSeq)
    return true
  }

  private async drainOutbox(): Promise<void> {
    if (this.pushing || this.state !== 'running' || this.port === null) return
    if (Date.now() < this.nextPushAttemptAt) return
    this.pushing = true
    try {
      while (this.state === 'running') {
        const pushed = await this.pushNextBatch(this.port)
        if (!pushed) break
        this.consecutivePushFailures = 0
        this.nextPushAttemptAt = 0
        this.lastError = null
      }
    } catch (err) {
      this.recordError(err)
      this.consecutivePushFailures += 1
      const delay = Math.min(this.pushIntervalMs * 2 ** this.consecutivePushFailures, this.maxPushRetryDelayMs)
      this.nextPushAttemptAt = Date.now() + delay
    } finally {
      this.pushing = false
    }
  }

  private recordError(err: unknown): void {
    const code = err instanceof Error && 'code' in err ? String((err as { code: unknown }).code) : 'UNKNOWN_ERROR'
    this.lastError = { code, message: err instanceof Error ? err.message : String(err) }
  }

  private teardownStream(): void {
    if (this.pushTimer !== null) {
      clearInterval(this.pushTimer)
      this.pushTimer = null
    }
    if (this.ackTimer !== null) {
      clearTimeout(this.ackTimer)
      this.ackTimer = null
    }
    if (this.resyncTimer !== null) {
      clearTimeout(this.resyncTimer)
      this.resyncTimer = null
    }
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe()
    }
    this.subscriptions = []
    if (this.transport) {
      this.transport.close()
      this.transport = null
    }
  }
}
