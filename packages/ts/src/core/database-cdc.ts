import { CdcAwareTransaction, type CdcTransactionState } from './cdc/cdc-aware-transaction.js'
import { ChangeTracker } from './cdc/change-tracker.js'
import { ensureCdcEpoch } from './cdc/epoch.js'
import { readAtPosition } from './cdc/read-position.js'
import { SubscriptionBuilderImpl, SubscriptionManager, startPolling } from './cdc/subscription.js'
import type { SQLiteConnection } from './driver/types.js'
import type { StampStatement } from './sync/stamper.js'
import { SyncStamper } from './sync/stamper.js'
import { Transaction } from './transaction.js'
import type { SubscriptionBuilder } from './types.js'

type RunExclusive = <T>(op: () => Promise<T>) => Promise<T>

export class DatabaseCdcController {
  private tracker: ChangeTracker | null = null
  private subscriptionManager: SubscriptionManager | null = null
  private liveConnRequest: Promise<SQLiteConnection> | null = null
  private stopPolling: (() => void) | null = null
  private stamper: SyncStamper | null = null
  private epochRequest: Promise<string> | null = null

  constructor(
    private readonly runExclusive: RunExclusive,
    private readonly acquireWriter: () => SQLiteConnection,
    private readonly pollInterval: number,
    private readonly retention: number,
    private readonly openSnapshotConnection: (() => Promise<SQLiteConnection>) | null,
  ) {}

  ensureEpoch(): Promise<string> {
    this.epochRequest ??= this.runExclusive(() => ensureCdcEpoch(this.acquireWriter())).catch((err: unknown) => {
      this.epochRequest = null
      throw err
    })
    return this.epochRequest
  }

  async readAtPositionWith<T>(
    read: (conn: SQLiteConnection) => Promise<T>,
  ): Promise<{ value: T; position: string; seq: bigint }> {
    const epoch = await this.ensureEpoch()
    const open = this.openSnapshotConnection

    if (open === null) {
      return this.runExclusive(() => readAtPosition(this.acquireWriter(), epoch, read))
    }

    const conn = await open()
    try {
      return await readAtPosition(conn, epoch, read)
    } finally {
      await conn.close().catch(() => {})
    }
  }

  async liveConnection(): Promise<{ conn: SQLiteConnection; run: RunExclusive }> {
    const open = this.openSnapshotConnection
    if (open === null) {
      return { conn: this.acquireWriter(), run: this.runExclusive }
    }

    this.liveConnRequest ??= open().catch((err: unknown) => {
      this.liveConnRequest = null
      throw err
    })
    return { conn: await this.liveConnRequest, run: op => op() }
  }

  async closeLiveConnection(): Promise<void> {
    const request = this.liveConnRequest
    if (request === null) return
    this.liveConnRequest = null
    await request.then(conn => conn.close()).catch(() => {})
  }

  get subscriptions(): SubscriptionManager | null {
    return this.subscriptionManager
  }

  get changeTracker(): ChangeTracker | null {
    return this.tracker
  }

  async watch(table: string): Promise<void> {
    const { tracker } = this.ensure()
    await this.runExclusive(async () => {
      await tracker.watch(this.acquireWriter(), table)
      await this.ensureStamper()
    })
    this.ensurePolling()
  }

  async ensureStamper(): Promise<SyncStamper> {
    this.stamper ??= await SyncStamper.init(this.acquireWriter())
    return this.stamper
  }

  async ensureStamping(): Promise<void> {
    if (this.stamper) return
    await this.runExclusive(() => this.ensureStamper())
  }

  stampStatements(options?: { persistClock?: boolean }): readonly StampStatement[] | null {
    return this.stamper ? this.stamper.stampStatements(options) : null
  }

  async applyStamps(txConn: SQLiteConnection): Promise<void> {
    if (this.stamper) {
      await this.stamper.applyStamps(txConn)
    }
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
    const result = await writer.transaction(async txConn => {
      const value = await fn(new CdcAwareTransaction(txConn, tracker, state))
      await this.applyStamps(txConn)
      return value
    })

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
    const subscriptions = this.subscriptionManager ?? new SubscriptionManager()
    this.tracker = tracker
    this.subscriptionManager = subscriptions
    return { tracker, subscriptions }
  }

  private ensurePolling(): void {
    if (this.stopPolling) return
    if (!this.tracker || !this.subscriptionManager) return

    this.stopPolling = startPolling(
      this.acquireWriter(),
      this.tracker,
      this.subscriptionManager,
      this.pollInterval,
      undefined,
      this.runExclusive,
    )
  }
}
