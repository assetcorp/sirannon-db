import type { DeviceSyncPort } from '../core/database-sync.js'
import type { ConflictResolver } from '../core/sync/types.js'
import type { ChangeEvent } from '../core/types.js'
import { unrefTimer } from './http-json.js'
import { WebSocketTransport } from './transport/ws.js'
import type { RemoteSubscription } from './types.js'

export const DEFAULT_IMMEDIATE_ACK_AFTER_CHANGES = 500

const STAGE_BATCH_EVENTS = 500

export interface PullStreamConfig {
  wsBaseUrl: string
  databaseId: string
  tables: readonly string[]
  headers?: Record<string, string>
  ackIntervalMs: number
  requestTimeout?: number
  immediateAckAfterChanges?: number
  resolver?: ConflictResolver | ((table: string) => ConflictResolver)
}

export interface PullStreamHooks {
  isRunning(): boolean
  port(): DeviceSyncPort | null
  onChange?: (event: ChangeEvent) => void
  onResyncRequired(): void
  onApplyFailure(err: unknown): void
  onApplySuccess(): void
  recordError(err: unknown): void
}

/**
 * Pulls the device stream and applies it through the on-disk staging table:
 * every received change is staged durably, a transaction is applied to the
 * real tables only when its `txEnd` change is staged, and acknowledgements
 * carry the staged watermark, because a staged change survives a crash and
 * is applied on restart. The subscription resumes from the staged
 * watermark, so a transaction cut off mid-stream is finished, not retried
 * from its start.
 */
export class PullStream {
  private transport: WebSocketTransport | null = null
  private subscriptions: RemoteSubscription[] = []
  private ackTimer: ReturnType<typeof setTimeout> | null = null
  private deviceId: string | null = null
  private lastAckedSeq: bigint | null = null
  private ackBaseSeq: bigint | null = null
  private inbox: ChangeEvent[] = []
  private pumping = false
  private failed = false
  private serverWindow: number | null = null
  private stagedSeq: bigint | null = null
  pullSeq: bigint | null = null
  pullEpoch: string | undefined
  stagedStream = false

  constructor(
    private readonly config: PullStreamConfig,
    private readonly hooks: PullStreamHooks,
  ) {}

  async open(deviceId: string, schemaVersion: number): Promise<void> {
    this.deviceId = deviceId
    this.lastAckedSeq = null
    this.inbox = []
    this.failed = false

    let recoveryApplyError: unknown | null = null
    const port = this.hooks.port()
    if (port !== null) {
      const recovered = await port.recoverStagedPull(this.config.resolver, this.hooks.onChange)
      this.stagedSeq = recovered.resumeSeq
      if (recovered.appliedSeq !== null && (this.pullSeq === null || recovered.appliedSeq > this.pullSeq)) {
        this.pullSeq = recovered.appliedSeq
      }
      recoveryApplyError = recovered.applyError
      if (recoveryApplyError !== null) {
        await this.adoptRecordedCursor(port)
      }
    }
    this.ackBaseSeq = this.resumeWatermark()

    const encodedId = encodeURIComponent(this.config.databaseId)
    const transport = new WebSocketTransport(`${this.config.wsBaseUrl}/db/${encodedId}`, {
      headers: this.config.headers,
      requestTimeout: this.config.requestTimeout,
    })
    this.transport = transport
    const [firstTable] = this.config.tables
    if (firstTable !== undefined) {
      const subscription = await transport.subscribe(firstTable, undefined, event => this.handlePullEvent(event), {
        deviceId,
        schemaVersion,
        tables: this.config.tables,
        sinceSeq: this.resumeWatermark() ?? undefined,
        getResumeSeq: () => this.resumeWatermark() ?? undefined,
        epoch: this.pullEpoch,
        stagedStream: this.stagedStream,
        onReset: () => this.hooks.onResyncRequired(),
        onSubscribed: info => this.handleSubscribed(info),
      })
      this.subscriptions.push(subscription)
    }

    if (recoveryApplyError !== null) {
      this.hooks.onApplyFailure(recoveryApplyError)
    }
  }

  teardown(): void {
    if (this.ackTimer !== null) {
      clearTimeout(this.ackTimer)
      this.ackTimer = null
    }
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe()
    }
    this.subscriptions = []
    if (this.transport) {
      this.transport.close()
      this.transport = null
    }
    this.inbox = []
  }

  async persist(): Promise<void> {
    const port = this.hooks.port()
    if (port === null || this.pullSeq === null) return
    try {
      await port.setPullState(this.pullSeq, this.pullEpoch)
    } catch (err) {
      this.hooks.recordError(err)
    }
  }

  private async adoptRecordedCursor(port: DeviceSyncPort): Promise<void> {
    try {
      const recorded = await port.getPullState()
      if (recorded !== null && (this.pullSeq === null || recorded.seq > this.pullSeq)) {
        this.pullSeq = recorded.seq
      }
    } catch (err) {
      this.hooks.recordError(err)
    }
  }

  private resumeWatermark(): bigint | null {
    if (this.stagedSeq !== null && (this.pullSeq === null || this.stagedSeq > this.pullSeq)) {
      return this.stagedSeq
    }
    return this.pullSeq
  }

  private handleSubscribed(info: {
    seq: bigint | undefined
    epoch: string | undefined
    resync: boolean
    maxUnacknowledgedChanges: number | undefined
  }): void {
    if (info.maxUnacknowledgedChanges !== undefined && info.maxUnacknowledgedChanges > 0) {
      this.serverWindow = info.maxUnacknowledgedChanges
    }
    if (info.resync) {
      this.hooks.onResyncRequired()
      return
    }
    if (info.epoch !== undefined) {
      this.pullEpoch = info.epoch
    }
    if (this.pullSeq === null && this.stagedSeq === null && info.seq !== undefined) {
      this.pullSeq = info.seq
    }
    if (this.ackBaseSeq === null) {
      this.ackBaseSeq = this.resumeWatermark()
    }
  }

  private handlePullEvent(event: ChangeEvent): void {
    if (this.failed) return
    this.inbox.push(event)
    void this.pump()
  }

  private async pump(): Promise<void> {
    if (this.pumping) return
    this.pumping = true
    try {
      while (this.inbox.length > 0 && !this.failed) {
        const port = this.hooks.port()
        if (port === null) {
          this.inbox = []
          return
        }
        const batch = this.inbox.splice(0, STAGE_BATCH_EVENTS)
        const closesTransaction = batch.some(event => event.txEnd === true)
        try {
          const staged = await port.stagePulledChanges(batch)
          if (staged !== null && (this.stagedSeq === null || staged > this.stagedSeq)) {
            this.stagedSeq = staged
          }
          if (closesTransaction) {
            const appliedThrough = await port.applyStagedPull(this.config.resolver, this.hooks.onChange)
            if (appliedThrough !== null) {
              if (this.pullSeq === null || appliedThrough > this.pullSeq) {
                this.pullSeq = appliedThrough
              }
              this.hooks.onApplySuccess()
            }
          }
        } catch (err) {
          this.failed = true
          this.inbox = []
          await this.adoptRecordedCursor(port)
          this.hooks.onApplyFailure(err)
          return
        }
        this.scheduleAckFlush()
      }
    } finally {
      this.pumping = false
    }
  }

  private scheduleAckFlush(): void {
    const configured =
      this.config.immediateAckAfterChanges ??
      (this.serverWindow === null
        ? DEFAULT_IMMEDIATE_ACK_AFTER_CHANGES
        : Math.max(1, Math.floor(this.serverWindow / 2)))
    const threshold = BigInt(configured)
    const base = this.lastAckedSeq ?? this.ackBaseSeq
    const watermark = this.resumeWatermark()
    const outstanding = watermark !== null && base !== null ? watermark - base : 0n
    if (outstanding > threshold) {
      if (this.ackTimer !== null) {
        clearTimeout(this.ackTimer)
        this.ackTimer = null
      }
      void this.flushAck()
      return
    }

    if (this.ackTimer !== null) return
    this.ackTimer = setTimeout(() => {
      this.ackTimer = null
      void this.flushAck()
    }, this.config.ackIntervalMs)
    unrefTimer(this.ackTimer)
  }

  private async flushAck(): Promise<void> {
    if (!this.hooks.isRunning()) return
    const deviceId = this.deviceId
    const transport = this.transport
    const seq = this.resumeWatermark()
    if (deviceId === null || transport === null || seq === null) return
    if (this.lastAckedSeq !== null && seq <= this.lastAckedSeq) return
    try {
      await this.persist()
      await transport.ack(deviceId, seq)
      this.lastAckedSeq = seq
    } catch (err) {
      this.hooks.recordError(err)
      this.scheduleAckFlush()
    }
  }
}
