import { invokeCallerCallback } from '../caller-callbacks.js'
import type { ChangeEvent } from '../types.js'
import { type HeldRow, LiveResult, type RowChange } from './live-result.js'
import type { LivePlan } from './query-plan.js'
import { encodeRowKey, rowidKey } from './row-keys.js'
import type { ProbeCandidate, RowProbe } from './row-probe.js'
import type { LiveQuery, LiveQueryState, LiveUpdate } from './types.js'

export interface PositionedRead<T> {
  rows: HeldRow<T>[]
  seq: bigint
}

export interface LiveQuerySource<T> {
  plan: LivePlan
  probe: RowProbe
  read(): Promise<PositionedRead<T>>
  start(handlers: { onEvent(event: ChangeEvent): void; onLost(): void }, sinceSeq: bigint): Promise<() => void>
  release(): Promise<void>
  rereadJitterMs: number
  maxTransactionChanges: number
  maxTransactionBytes: number
  wait(ms: number): Promise<void>
  onError?: (error: Error) => void
}

export class MaintainedLiveQuery<T> implements LiveQuery<T> {
  private state: LiveQueryState<T> = { status: 'pending' }
  private readonly listeners = new Set<(update: LiveUpdate<T>) => void>()
  private readonly result: LiveResult<T>
  private readonly batches: ChangeEvent[][] = []
  private pending: ChangeEvent[] = []
  private pendingBytes = 0
  private pendingOverflowed = false
  private stopDelivery: (() => void) | null = null
  private appliedThroughSeq = -1n
  private reading = false
  private readAgain = false
  private draining = false
  private closed = false

  private constructor(private readonly source: LiveQuerySource<T>) {
    this.result = new LiveResult<T>(source.plan)
  }

  static async open<T>(source: LiveQuerySource<T>): Promise<MaintainedLiveQuery<T>> {
    const query = new MaintainedLiveQuery<T>(source)
    try {
      await query.refresh(false)
    } catch (err) {
      await query.close()
      throw err
    }
    if (query.state.status === 'error') {
      const failure = query.state.error
      await query.close()
      throw failure
    }
    return query
  }

  getState(): LiveQueryState<T> {
    return this.state
  }

  subscribe(listener: (update: LiveUpdate<T>) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.stopDelivery?.()
    this.stopDelivery = null
    this.listeners.clear()
    this.batches.length = 0
    this.pending = []
    await this.source.release()
  }

  private receive(event: ChangeEvent): void {
    if (this.closed) return
    if (event.seq <= this.appliedThroughSeq) return

    if (!this.pendingOverflowed) {
      this.pending.push(event)
      this.pendingBytes += approximateBytes(event)
      if (
        this.pending.length > this.source.maxTransactionChanges ||
        this.pendingBytes > this.source.maxTransactionBytes
      ) {
        this.pendingOverflowed = true
        this.pending = []
      }
    }

    if (event.txEnd !== true) return

    const overflowed = this.pendingOverflowed
    const batch = this.pending
    this.pending = []
    this.pendingBytes = 0
    this.pendingOverflowed = false

    this.batches.push(overflowed ? [] : batch)
    void this.drain()
  }

  private deliveryLost(): void {
    if (this.closed) return
    this.stopDelivery = null
    this.batches.length = 0
    this.pending = []
    this.pendingBytes = 0
    this.pendingOverflowed = false
    void this.reread()
  }

  private async drain(): Promise<void> {
    if (this.draining || this.reading || this.closed) return
    this.draining = true
    try {
      while (this.batches.length > 0 && !this.closed && !this.reading) {
        const batch = this.batches.shift()
        if (batch === undefined) break
        await this.applyBatch(batch)
      }
    } finally {
      this.draining = false
    }
  }

  private async applyBatch(batch: ChangeEvent[]): Promise<void> {
    if (batch.length === 0) {
      await this.reread()
      return
    }

    const fresh = batch.filter(event => event.seq > this.appliedThroughSeq)
    if (fresh.length === 0) return

    let changes: RowChange[]
    try {
      changes = await this.evaluate(fresh)
    } catch (err) {
      this.fail(err)
      return
    }
    if (this.closed) return

    const highest = fresh[fresh.length - 1].seq
    if (changes.length === 0) {
      this.appliedThroughSeq = highest
      return
    }

    const ops = changes.length > this.result.size ? null : this.result.apply(changes)
    if (ops === null) {
      await this.reread()
      return
    }

    this.appliedThroughSeq = highest
    this.publish({ status: 'ready', rows: this.result.snapshot(), revalidating: false }, { kind: 'ops', ops })
  }

  private async evaluate(events: readonly ChangeEvent[]): Promise<RowChange[]> {
    const candidates: ProbeCandidate[] = []
    for (let i = 0; i < events.length; i++) {
      const event = events[i]
      if (event.type !== 'insert' && event.oldRow !== undefined) {
        candidates.push({ slot: i * 2, payload: event.oldRow })
      }
      if (event.type !== 'delete') {
        candidates.push({ slot: i * 2 + 1, payload: event.row })
      }
    }

    const matches = await this.source.probe.evaluate(candidates)
    const changes: RowChange[] = []

    for (let i = 0; i < events.length; i++) {
      const event = events[i]
      const before = matches.get(i * 2) ?? null
      const after = matches.get(i * 2 + 1) ?? null
      if (before === null && after === null) continue

      const beforeKey = event.oldRow === undefined ? null : this.keyOf(event, event.oldRow)
      const afterKey = event.type === 'delete' ? null : this.keyOf(event, event.row)

      if (before !== null && after !== null && beforeKey !== null && afterKey !== null && beforeKey !== afterKey) {
        changes.push({ key: beforeKey, before, after: null })
        changes.push({ key: afterKey, before: null, after })
        continue
      }

      const key = after !== null ? (afterKey ?? beforeKey) : beforeKey
      if (key === null) continue
      changes.push({ key, before, after })
    }

    return changes
  }

  private keyOf(event: ChangeEvent, payload: Record<string, unknown>): string | null {
    if (this.source.plan.usesRowid) return event.rowId === undefined ? null : rowidKey(event.rowId)
    return encodeRowKey(this.source.plan.keyColumns.map(column => payload[column]))
  }

  private async reread(): Promise<void> {
    if (this.reading) {
      this.readAgain = true
      return
    }
    if (this.state.status === 'ready' && !this.state.revalidating) {
      this.publish({ status: 'ready', rows: this.state.rows, revalidating: true }, { kind: 'revalidating' })
    }
    await this.refresh(true)
  }

  private async refresh(spread: boolean): Promise<void> {
    this.reading = true
    try {
      do {
        this.readAgain = false
        if (spread && this.source.rereadJitterMs > 0) {
          await this.source.wait(Math.floor(Math.random() * this.source.rereadJitterMs))
        }
        if (this.closed) return

        const refreshed = await this.source.read()
        if (this.closed) return
        this.result.reset(refreshed.rows)
        this.appliedThroughSeq = refreshed.seq

        if (this.stopDelivery === null) {
          const stop = await this.source.start(
            { onEvent: event => this.receive(event), onLost: () => this.deliveryLost() },
            refreshed.seq,
          )
          if (this.closed) {
            stop()
            return
          }
          this.stopDelivery = stop
        }
      } while (this.readAgain && !this.closed)

      this.publish({ status: 'ready', rows: this.result.snapshot(), revalidating: false }, { kind: 'rows' })
    } catch (err) {
      this.fail(err)
    } finally {
      this.reading = false
    }
    if (!this.closed && this.batches.length > 0) void this.drain()
  }

  private fail(err: unknown): void {
    if (this.closed) return
    this.publish({ status: 'error', error: err instanceof Error ? err : new Error(String(err)) }, { kind: 'error' })
  }

  private publish(next: LiveQueryState<T>, update: LiveUpdate<T>): void {
    this.state = next
    for (const listener of [...this.listeners]) {
      invokeCallerCallback(() => listener(update), this.source.onError)
    }
  }
}

function approximateBytes(event: ChangeEvent): number {
  return approximateRowBytes(event.row) + (event.oldRow === undefined ? 0 : approximateRowBytes(event.oldRow))
}

function approximateRowBytes(row: Record<string, unknown>): number {
  let bytes = 0
  for (const key of Object.keys(row)) {
    const value = row[key]
    bytes += key.length + 8
    if (typeof value === 'string') bytes += value.length
    else if (value instanceof Uint8Array) bytes += value.byteLength
  }
  return bytes
}
