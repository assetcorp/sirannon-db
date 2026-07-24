import type { DeviceSyncPort } from '../core/database-sync.js'
import type { ChangeEvent } from '../core/types.js'
import { unrefTimer } from './http-json.js'
import { WebSocketTransport } from './transport/ws.js'
import type { RemoteSubscription } from './types.js'

export interface PullStreamConfig {
  wsBaseUrl: string
  databaseId: string
  tables: readonly string[]
  ackIntervalMs: number
  requestTimeout?: number
}

export interface PullStreamHooks {
  isRunning(): boolean
  port(): DeviceSyncPort | null
  onChange?: (event: ChangeEvent) => void
  onResyncRequired(): void
  recordError(err: unknown): void
}

export class PullStream {
  private transport: WebSocketTransport | null = null
  private subscriptions: RemoteSubscription[] = []
  private ackTimer: ReturnType<typeof setTimeout> | null = null
  private deviceId: string | null = null
  private lastAckedSeq: bigint | null = null
  pullSeq: bigint | null = null
  pullEpoch: string | undefined

  constructor(
    private readonly config: PullStreamConfig,
    private readonly hooks: PullStreamHooks,
  ) {}

  async open(deviceId: string, schemaVersion: number): Promise<void> {
    this.deviceId = deviceId
    this.lastAckedSeq = null
    const encodedId = encodeURIComponent(this.config.databaseId)
    const transport = new WebSocketTransport(`${this.config.wsBaseUrl}/db/${encodedId}`, {
      requestTimeout: this.config.requestTimeout,
    })
    this.transport = transport
    for (const table of this.config.tables) {
      const subscription = await transport.subscribe(table, undefined, event => this.handlePullEvent(event), {
        deviceId,
        schemaVersion,
        sinceSeq: this.pullSeq ?? undefined,
        epoch: this.pullEpoch,
        onReset: () => this.hooks.onResyncRequired(),
        onSubscribed: info => this.handleSubscribed(info),
      })
      this.subscriptions.push(subscription)
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

  private handleSubscribed(info: { seq: bigint | undefined; epoch: string | undefined; resync: boolean }): void {
    if (info.epoch !== undefined) {
      this.pullEpoch = info.epoch
    }
    if (info.resync) {
      if (info.seq !== undefined) {
        this.pullSeq = info.seq
      }
      this.hooks.onResyncRequired()
    } else if (this.pullSeq === null && info.seq !== undefined) {
      this.pullSeq = info.seq
    }
  }

  private handlePullEvent(event: ChangeEvent): void {
    if (this.pullSeq === null || event.seq > this.pullSeq) {
      this.pullSeq = event.seq
    }
    try {
      this.hooks.onChange?.(event)
    } finally {
      this.scheduleAckFlush()
    }
  }

  private scheduleAckFlush(): void {
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
    const seq = this.pullSeq
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
