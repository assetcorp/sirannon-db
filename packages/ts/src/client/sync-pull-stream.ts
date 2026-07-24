import type { DeviceSyncPort } from '../core/database-sync.js'
import type { ConflictResolver, ReplicationChange } from '../core/sync/types.js'
import type { ChangeEvent } from '../core/types.js'
import { unrefTimer } from './http-json.js'
import { WebSocketTransport } from './transport/ws.js'
import type { RemoteSubscription } from './types.js'

export const DEFAULT_IMMEDIATE_ACK_AFTER_CHANGES = 500

export interface PullStreamConfig {
  wsBaseUrl: string
  databaseId: string
  tables: readonly string[]
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

export class PullStream {
  private transport: WebSocketTransport | null = null
  private subscriptions: RemoteSubscription[] = []
  private ackTimer: ReturnType<typeof setTimeout> | null = null
  private deviceId: string | null = null
  private lastAckedSeq: bigint | null = null
  private ackBaseSeq: bigint | null = null
  private group: ChangeEvent[] = []
  private queue: ChangeEvent[][] = []
  private draining = false
  private failed = false
  private serverWindow: number | null = null
  pullSeq: bigint | null = null
  pullEpoch: string | undefined

  constructor(
    private readonly config: PullStreamConfig,
    private readonly hooks: PullStreamHooks,
  ) {}

  async open(deviceId: string, schemaVersion: number): Promise<void> {
    this.deviceId = deviceId
    this.lastAckedSeq = null
    this.ackBaseSeq = this.pullSeq
    this.group = []
    this.queue = []
    this.failed = false
    const encodedId = encodeURIComponent(this.config.databaseId)
    const transport = new WebSocketTransport(`${this.config.wsBaseUrl}/db/${encodedId}`, {
      requestTimeout: this.config.requestTimeout,
    })
    this.transport = transport
    const [firstTable] = this.config.tables
    if (firstTable === undefined) return

    const subscription = await transport.subscribe(firstTable, undefined, event => this.handlePullEvent(event), {
      deviceId,
      schemaVersion,
      tables: this.config.tables,
      sinceSeq: this.pullSeq ?? undefined,
      getResumeSeq: () => this.pullSeq ?? undefined,
      epoch: this.pullEpoch,
      onReset: () => this.hooks.onResyncRequired(),
      onSubscribed: info => this.handleSubscribed(info),
    })
    this.subscriptions.push(subscription)
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
    this.group = []
    this.queue = []
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

  private handleSubscribed(info: {
    seq: bigint | undefined
    epoch: string | undefined
    resync: boolean
    maxUnacknowledgedChanges: number | undefined
  }): void {
    this.group = []
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
    if (this.pullSeq === null && info.seq !== undefined) {
      this.pullSeq = info.seq
    }
    if (this.ackBaseSeq === null) {
      this.ackBaseSeq = this.pullSeq
    }
  }

  private handlePullEvent(event: ChangeEvent): void {
    if (this.failed) return
    this.group.push(event)
    if (event.txEnd !== true) return

    this.queue.push(this.group)
    this.group = []
    void this.drain()
  }

  private async drain(): Promise<void> {
    if (this.draining) return
    this.draining = true
    try {
      while (this.queue.length > 0 && !this.failed) {
        const group = this.queue.shift()
        if (group === undefined) break
        await this.applyGroup(group)
      }
    } finally {
      this.draining = false
    }
  }

  private async applyGroup(group: readonly ChangeEvent[]): Promise<void> {
    const port = this.hooks.port()
    if (port === null || group.length === 0) return

    const last = group[group.length - 1]
    try {
      await port.applyPulledTransaction(group.map(toReplicationChange), last.seq, this.config.resolver)
    } catch (err) {
      this.failed = true
      this.queue = []
      this.group = []
      this.hooks.onApplyFailure(err)
      return
    }

    if (this.pullSeq === null || last.seq > this.pullSeq) {
      this.pullSeq = last.seq
    }
    this.hooks.onApplySuccess()
    for (const event of group) {
      this.hooks.onChange?.(event)
    }
    this.scheduleAckFlush()
  }

  private scheduleAckFlush(): void {
    const configured =
      this.config.immediateAckAfterChanges ??
      (this.serverWindow === null
        ? DEFAULT_IMMEDIATE_ACK_AFTER_CHANGES
        : Math.max(1, Math.floor(this.serverWindow / 2)))
    const threshold = BigInt(configured)
    const base = this.lastAckedSeq ?? this.ackBaseSeq
    const outstanding = this.pullSeq !== null && base !== null ? this.pullSeq - base : 0n
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

function toReplicationChange(event: ChangeEvent): ReplicationChange {
  return {
    table: event.table,
    operation: event.type,
    rowId: event.rowId ?? '',
    primaryKey: {},
    hlc: event.hlc ?? '',
    txId: event.txId ?? '',
    nodeId: event.origin ?? '',
    newData: event.type === 'delete' ? null : event.row,
    oldData: event.oldRow ?? null,
  }
}
