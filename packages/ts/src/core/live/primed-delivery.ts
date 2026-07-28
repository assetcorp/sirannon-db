import { PrimedSubscription } from '../cdc/primed-subscription.js'
import { TransactionGrouper } from '../cdc/transaction-grouper.js'
import type { DatabaseCdcController } from '../database-cdc.js'
import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import type { ChangeEvent, Subscription } from '../types.js'

export interface DeliveryHandlers {
  onEvent(event: ChangeEvent): void
  onLost(): void
}

export class PrimedLiveDelivery {
  constructor(
    private readonly cdc: DatabaseCdcController,
    private readonly conn: SQLiteConnection,
    private readonly run: <R>(operation: () => Promise<R>) => Promise<R>,
    private readonly table: string,
  ) {}

  async start(handlers: DeliveryHandlers, sinceSeq: bigint): Promise<() => void> {
    const tracker = this.cdc.changeTracker
    const manager = this.cdc.subscriptions
    if (tracker === null || manager === null) {
      throw new CDCError(`Cannot deliver changes for '${this.table}': the table is not watched`)
    }

    const grouper = new TransactionGrouper(event => {
      handlers.onEvent(event)
      return true
    })

    let stopped = false
    let removeBatchEnd: () => void = () => {}
    let subscription: Subscription | null = null
    const stop = (): void => {
      if (stopped) return
      stopped = true
      removeBatchEnd()
      subscription?.unsubscribe()
    }

    const primed = new PrimedSubscription(
      sinceSeq,
      event => {
        grouper.receive(event)
        return true
      },
      () => {
        stop()
        handlers.onLost()
      },
    )

    const boundary = tracker.cursor
    const boundaryEndsTransaction = tracker.pollEndedAtTxBoundary
    subscription = manager.subscribe(this.table, undefined, event => primed.onLiveEvent(event))

    try {
      await this.run(() => primed.replayTables(tracker, this.conn, [this.table], undefined, boundary))
    } catch (err) {
      stop()
      throw err
    }

    if (stopped) return stop

    grouper.flush(boundaryEndsTransaction)
    primed.goLive()
    removeBatchEnd = manager.addBatchEndListener(atTxBoundary => {
      grouper.flush(atTxBoundary)
    })

    return stop
  }
}
