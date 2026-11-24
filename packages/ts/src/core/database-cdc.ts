import { toError } from './caller-callbacks.js'
import { CdcAwareTransaction, type CdcTransactionState } from './cdc/cdc-aware-transaction.js'
import { type ChangeFeed, startChangeFeed } from './cdc/change-feed.js'
import { pruneChangeLog } from './cdc/change-log-prune.js'
import type { DatabaseChangeSignal } from './cdc/change-signal.js'
import { ChangeTracker } from './cdc/change-tracker.js'
import type { DeviceRetentionPolicy } from './cdc/device-retention.js'
import { ensureCdcEpoch } from './cdc/epoch.js'
import { readAtPosition } from './cdc/read-position.js'
import { type ChangeDispatchObserver, SubscriptionBuilderImpl, SubscriptionManager } from './cdc/subscription.js'
import type { SQLiteConnection } from './driver/types.js'
import type { StampStatement } from './sync/stamper.js'
import { SyncStamper } from './sync/stamper.js'
import { type StatementMeter, Transaction } from './transaction.js'
import type { SubscriptionBuilder } from './types.js'
import { runInSavepoint, runWriterTransaction, type WriterTransactionRunner } from './writer-transaction.js'

type RunExclusive = <T>(op: () => Promise<T>) => Promise<T>

const MAX_CONSECUTIVE_POLL_FAILURES = 10

/**
 * The locks, connections, and settings that a {@link DatabaseCdcController} works through.
 *
 * @internal
 */
export interface DatabaseCdcDeps {
  /** Holds the writer for an operation that can commit a write, and fires the change signal once it ends. */
  runExclusive: RunExclusive
  /** Holds the writer for a read of the change log or a prune, without firing the change signal. */
  runWithoutChangeSignal: RunExclusive
  /**
   * Runs a read of the change log on a reader connection, or on the writer under the lock while the database has no
   * reader or a key change has closed its readers, so that a read never contends with a write for the writer.
   */
  readChangeLog: <T>(read: (conn: SQLiteConnection) => Promise<T>) => Promise<T>
  acquireWriter: () => SQLiteConnection
  signal: DatabaseChangeSignal
  pollInterval: number
  retention: number
  openSnapshotConnection: (() => Promise<SQLiteConnection>) | null
  deviceRetention: DeviceRetentionPolicy
  onDispatched?: ChangeDispatchObserver
}

export class DatabaseCdcController {
  private tracker: ChangeTracker | null = null
  private subscriptionManager: SubscriptionManager | null = null
  private liveConnRequest: Promise<SQLiteConnection> | null = null
  private feed: ChangeFeed | null = null
  private stamper: SyncStamper | null = null
  private epochRequest: Promise<string> | null = null
  private readonly liveConnectionListeners = new Set<() => void>()
  private readonly runExclusive: RunExclusive
  private readonly acquireWriter: () => SQLiteConnection
  private readonly openSnapshotConnection: (() => Promise<SQLiteConnection>) | null

  constructor(private readonly deps: DatabaseCdcDeps) {
    this.runExclusive = deps.runExclusive
    this.acquireWriter = deps.acquireWriter
    this.openSnapshotConnection = deps.openSnapshotConnection
  }

  /** The signal that fires when this database may hold changes that a change poller has not read yet. */
  get changeSignal(): DatabaseChangeSignal {
    return this.deps.signal
  }

  /** The shortest time between two reads of the change log, in milliseconds. */
  get pollInterval(): number {
    return this.deps.pollInterval
  }

  /** Runs a read of the change log on a connection that leaves the writer free, as {@link DatabaseCdcDeps.readChangeLog} describes. */
  readChangeLog<T>(read: (conn: SQLiteConnection) => Promise<T>): Promise<T> {
    return this.deps.readChangeLog(read)
  }

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
      return { conn: await this.runExclusive(async () => this.acquireWriter()), run: this.runExclusive }
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

  /**
   * Registers a function that the controller calls when a key change closes the live connection to replace the file,
   * so that each live query plans again on the connection that opens afterwards.
   *
   * @param listener - The function to call.
   * @returns A function that removes the registration.
   */
  onLiveConnectionReplaced(listener: () => void): () => void {
    this.liveConnectionListeners.add(listener)
    return () => {
      this.liveConnectionListeners.delete(listener)
    }
  }

  /**
   * Closes the live connection before a key change replaces the file, after telling every live query to plan again
   * on the connection that the next call opens once the replacement ends. Where live queries read through the
   * pool's writer, {@link DatabaseCdcController.resumeOnReopenedPool} tells them instead, once that writer exists.
   */
  async releaseLiveConnection(): Promise<void> {
    if (this.openSnapshotConnection === null) return
    const request = this.liveConnRequest
    this.liveConnRequest = null
    this.replanLiveQueries()
    if (request !== null) await request.then(conn => conn.close()).catch(() => {})
  }

  /**
   * Starts polling again where polling was on, and has every live query that reads through the pool's writer plan
   * again, after the pool opened its connections to the file that a key change left in place. The key change calls
   * this while it holds the writer, so no live read reaches the closed writer.
   */
  resumeOnReopenedPool(): void {
    if (this.openSnapshotConnection === null) this.replanLiveQueries()
    if (this.feed === null) return
    this.stop()
    this.ensurePolling()
  }

  private replanLiveQueries(): void {
    for (const listener of [...this.liveConnectionListeners]) listener()
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

  stampStatements(options?: { persistClock?: boolean }): readonly StampStatement[] | null {
    return this.stamper ? this.stamper.stampStatements(options) : null
  }

  stampOpening(): StampStatement | null {
    return this.stamper ? this.stamper.openingStatement() : null
  }

  get stamps(): boolean {
    return this.stamper !== null
  }

  get activeStamper(): SyncStamper | null {
    return this.stamper
  }

  openStamps(txConn: SQLiteConnection, runInTransaction: WriterTransactionRunner): Promise<void> | null {
    if (!this.stamper || runInTransaction === runInSavepoint) return null
    return this.stamper.openStamps(txConn)
  }

  applyStamps(txConn: SQLiteConnection, runInTransaction: WriterTransactionRunner): Promise<void> | null {
    if (!this.stamper || runInTransaction === runInSavepoint) return null
    return this.stamper.applyStamps(txConn)
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

  async runTransaction<T>(
    writer: SQLiteConnection,
    fn: (tx: Transaction) => Promise<T>,
    meter: StatementMeter | null = null,
    runInTransaction: WriterTransactionRunner = runWriterTransaction,
  ): Promise<T> {
    const tracker = this.tracker
    if (!tracker && !this.stamper) {
      return Transaction.run(writer, fn, meter, runInTransaction)
    }
    if (!tracker) {
      return runInTransaction(writer, async txConn => {
        await this.openStamps(txConn, runInTransaction)
        const tx = new Transaction(txConn).withMeter(meter)
        let value: T
        try {
          value = await fn(tx)
        } finally {
          tx.end()
        }
        await this.applyStamps(txConn, runInTransaction)
        return value
      })
    }

    const state: CdcTransactionState = { sawDdl: false, droppedTables: [] }
    const result = await runInTransaction(writer, async txConn => {
      await this.openStamps(txConn, runInTransaction)
      const tx = new CdcAwareTransaction(txConn, tracker, state).withMeter(meter)
      let value: T
      try {
        value = await fn(tx)
      } finally {
        tx.end()
      }
      await this.applyStamps(txConn, runInTransaction)
      return value
    })

    if (state.sawDdl && state.droppedTables.length > 0) {
      await tracker.pruneDroppedTables(writer, state.droppedTables)
    }

    return result
  }

  stop(): void {
    if (this.feed) {
      this.feed.stop()
      this.feed = null
    }
  }

  private ensure(): { tracker: ChangeTracker; subscriptions: SubscriptionManager } {
    const tracker = this.tracker ?? new ChangeTracker({ retention: this.deps.retention })
    const subscriptions = this.subscriptionManager ?? new SubscriptionManager(this.deps.onDispatched)
    this.tracker = tracker
    this.subscriptionManager = subscriptions
    return { tracker, subscriptions }
  }

  private ensurePolling(): void {
    if (this.feed) return
    const tracker = this.tracker
    const manager = this.subscriptionManager
    if (!tracker || !manager) return

    const { runWithoutChangeSignal, readChangeLog, deviceRetention, retention } = this.deps
    this.feed = startChangeFeed({
      tracker,
      manager,
      signal: this.deps.signal,
      intervalMs: this.deps.pollInterval,
      readChanges: () => readChangeLog(conn => tracker.poll(conn)),
      prune: () =>
        runWithoutChangeSignal(() => pruneChangeLog(this.acquireWriter(), tracker, deviceRetention, retention)),
      onFailure: (error, consecutiveFailures) => {
        if (consecutiveFailures < MAX_CONSECUTIVE_POLL_FAILURES) return true
        manager.reportError(toError(error))
        return false
      },
    })
  }
}
