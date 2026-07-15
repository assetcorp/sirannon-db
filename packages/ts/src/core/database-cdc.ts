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
    this.ensure()
    const tracker = this.tracker
    await this.runExclusive(() => tracker?.watch(this.acquireWriter(), table) ?? Promise.resolve())
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
    this.ensure()
    const manager = this.subscriptions
    if (!manager) throw new Error('subscriptionManager not initialized')
    return new SubscriptionBuilderImpl(table, manager)
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

  private ensure(): void {
    if (!this.tracker) {
      this.tracker = new ChangeTracker({ retention: this.retention })
    }
    if (!this.subscriptions) {
      this.subscriptions = new SubscriptionManager()
    }
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
