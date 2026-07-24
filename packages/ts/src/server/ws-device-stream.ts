import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'

export const DEFAULT_MAX_UNACKNOWLEDGED_CHANGES = 1_000

export interface DeviceStreamDeps {
  deviceId: string
  maxUnacknowledgedChanges: number
  send(event: ChangeEvent): WSSendOutcome
  onOverload(): void
}

export class DeviceChangeStream {
  private pending: ChangeEvent[] = []
  private held: ChangeEvent[][] = []
  private heldCount = 0
  private ackedSeq = 0n
  private highestSentSeq = 0n
  private halted = false
  private priming = false

  constructor(private readonly deps: DeviceStreamDeps) {}

  get deviceId(): string {
    return this.deps.deviceId
  }

  get stopped(): boolean {
    return this.halted
  }

  receive(event: ChangeEvent): void {
    if (this.halted) return

    const openTxId = this.pending[0]?.txId
    if (this.pending.length > 0 && (openTxId === undefined || openTxId !== event.txId)) {
      this.closeGroup()
    }

    this.pending.push(event)

    if (event.txId === undefined) {
      this.closeGroup()
    }
  }

  beginPriming(): void {
    this.priming = true
  }

  endPriming(): void {
    this.priming = false
    this.closeGroup()
  }

  onBatchEnd(atTxBoundary: boolean): void {
    if (this.halted || this.priming || !atTxBoundary) return
    this.closeGroup()
  }

  onAck(seq: bigint): void {
    if (this.halted) return
    if (seq > this.ackedSeq) {
      this.ackedSeq = seq
    }
    this.drainHeld()
  }

  stop(): void {
    this.halted = true
    this.pending = []
    this.held = []
    this.heldCount = 0
  }

  private closeGroup(): void {
    if (this.pending.length === 0) return
    const group = this.pending
    this.pending = []
    const last = group[group.length - 1]
    group[group.length - 1] = { ...last, txEnd: true }

    if (this.held.length > 0 || this.windowClosed()) {
      this.hold(group)
      return
    }
    this.sendGroup(group)
  }

  private windowClosed(): boolean {
    return this.highestSentSeq - this.ackedSeq > BigInt(this.deps.maxUnacknowledgedChanges)
  }

  private hold(group: ChangeEvent[]): void {
    this.held.push(group)
    this.heldCount += group.length
    if (this.heldCount > this.deps.maxUnacknowledgedChanges) {
      this.stop()
      this.deps.onOverload()
    }
  }

  private drainHeld(): void {
    while (this.held.length > 0 && !this.windowClosed()) {
      const group = this.held.shift()
      if (group === undefined) return
      this.heldCount -= group.length
      if (!this.sendGroup(group)) return
    }
  }

  private sendGroup(group: readonly ChangeEvent[]): boolean {
    for (const event of group) {
      const outcome = this.deps.send(event)
      if (outcome === 'dropped') {
        this.stop()
        return false
      }
      if (event.seq > this.highestSentSeq) {
        this.highestSentSeq = event.seq
      }
    }
    return true
  }
}
