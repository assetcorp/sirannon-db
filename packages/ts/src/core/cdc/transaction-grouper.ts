import type { ChangeEvent } from '../types.js'

export class TransactionGrouper {
  private pending: ChangeEvent | undefined
  private stopped = false

  constructor(private readonly deliver: (event: ChangeEvent) => boolean) {}

  receive(event: ChangeEvent): boolean {
    if (this.stopped) return false

    const held = this.pending
    this.pending = undefined
    if (held !== undefined && !this.emit(held, held.txId !== event.txId)) {
      return false
    }

    if (event.txId === undefined) {
      return this.emit(event, true)
    }
    this.pending = event
    return true
  }

  flush(atTxBoundary: boolean): boolean {
    if (this.stopped) return false
    if (!atTxBoundary) return true
    const held = this.pending
    this.pending = undefined
    if (held === undefined) return true
    return this.emit(held, true)
  }

  private emit(event: ChangeEvent, txEnd: boolean): boolean {
    const outgoing = txEnd && event.txEnd !== true ? { ...event, txEnd: true } : event
    if (this.deliver(outgoing)) return true
    this.stopped = true
    this.pending = undefined
    return false
  }
}
