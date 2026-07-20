import { CdcAwareTransaction, type CdcTransactionState } from './cdc/cdc-aware-transaction.js'
import { ChangeTracker } from './cdc/change-tracker.js'
import { SubscriptionBuilderImpl, SubscriptionManager, startPolling } from './cdc/subscription.js'
import type { SQLiteConnection } from './driver/types.js'
import { Transaction } from './transaction.js'
import type { SubscriptionBuilder } from './types.js'

type RunExclusive = <T>(op: () => Promise<T>) => Promise<T>

export class DatabaseCdcController {
  private tracker: ChangeTracker | null = null
  private subscriptions: SubscriptionManager | null = null
  private stopPolling: (() => void) | null = null

  constructor(
    private readonly runExclusive: RunExclusive,
    private readonly acquireWriter: () => SQLiteConnection,
    private readonly pollInterval: number,
    private readonly retention: number,
  ) {}

  get changeTracker(): ChangeTracker | null {
    return this.tracker
  }

  async watch(table: string): Promise<void> {
    const { tracker } = this.ensure()
    await this.runExclusive(() => tracker.watch(this.acquireWriter(), table))
    this.ensurePolling()
  }

  async unwatch(table: string): Promise<void> {
    const tracker = this.tracker
    if (!tracker) return

    await this.runExclusive(() => tracker.unwatch(this.acquireWriter(), table))

    if (tracker.watchedTables.size === 0) {
      this.stop()
    }
  }

  on(table: string): SubscriptionBuilder {
    const { subscriptions } = this.ensure()
    return new SubscriptionBuilderImpl(table, subscriptions)
  }

  async runTransaction<T>(writer: SQLiteConnection, fn: (tx: Transaction) => Promise<T>): Promise<T> {
    const tracker = this.tracker
    if (!tracker) {
      return Transaction.run(writer, fn)
    }

    const state: CdcTransactionState = { sawDdl: false, droppedTables: [] }
    const result = await writer.transaction(txConn => fn(new CdcAwareTransaction(txConn, tracker, state)))

    if (state.sawDdl && state.droppedTables.length > 0) {
      await tracker.pruneDroppedTables(writer, state.droppedTables)
    }

    return result
  }

  stop(): void {
    if (this.stopPolling) {
      this.stopPolling()
      this.stopPolling = null
    }
  }

  private ensure(): { tracker: ChangeTracker; subscriptions: SubscriptionManager } {
    const tracker = this.tracker ?? new ChangeTracker({ retention: this.retention })
    const subscriptions = this.subscriptions ?? new SubscriptionManager()
    this.tracker = tracker
    this.subscriptions = subscriptions
    return { tracker, subscriptions }
  }

  private ensurePolling(): void {
    if (this.stopPolling) return
    if (!this.tracker || !this.subscriptions) return

    this.stopPolling = startPolling(
      this.acquireWriter(),
      this.tracker,
      this.subscriptions,
      this.pollInterval,
      undefined,
      this.runExclusive,
    )
  }
}
