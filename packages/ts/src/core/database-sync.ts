import type { DatabaseCdcController } from './database-cdc.js'
import type { SQLiteConnection } from './driver/types.js'
import { CHANGES_TABLE } from './internal-tables.js'
import { mirrorSchemaVersion } from './migrations/schema-version.js'
import { BatchApplier } from './sync/batch-applier.js'
import { BatchReader } from './sync/batch-reader.js'
import { LWWResolver } from './sync/conflict/lww.js'
import { REMOTE_ORIGIN_NODE_ID } from './sync/origins.js'
import { PkResolver } from './sync/pk.js'
import {
  abortSnapshotLoad,
  applySnapshotSchema,
  beginSnapshotLoad,
  endSnapshotLoad,
  loadSnapshotPage,
  snapshotLoadPending,
} from './sync/snapshot-apply.js'
import {
  applyStagedTransactions,
  recoverStagedPull,
  type StagedRecovery,
  stagePulledChanges,
} from './sync/staged-pull.js'
import { recordLocalColumnVersions } from './sync/stamp-ops.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './sync/types.js'
import { SEQ_STRING_RE } from './sync/validators.js'
import {
  type AppliedMigrationRow,
  ensureBatchApplyTables,
  ensureChangesTable,
  ensureMetaTable,
  highestMigrationVersion,
  replaceMigrationHistory,
  selectCountOutboundChanges,
  selectMaxAppliedSourceSeq,
  selectMaxChangeSeq,
  selectMetaValue,
  upsertMetaValue,
} from './system-catalog/index.js'
import type { ChangeEvent } from './types.js'

type RunExclusive = <T>(op: () => Promise<T>) => Promise<T>

const PUSHED_SEQ_META_KEY = 'device_sync_pushed_seq'
const PULL_SEQ_META_KEY = 'device_sync_pull_seq'
const PULL_EPOCH_META_KEY = 'device_sync_pull_epoch'
const RESYNC_REQUIRED_META_KEY = 'device_sync_resync_required'
const COLUMN_VERSIONS_SEQ_META_KEY = 'device_sync_column_versions_seq'

/**
 * How far a device has pulled from its server, and which sequence space that
 * position belongs to.
 *
 * @public
 */
export interface DeviceSyncPullState {
  /** Highest server change-log position this device has applied. */
  seq: bigint
  /** Sequence space that position came from. */
  epoch: string | undefined
}

/**
 * What a device's local database offers the sync loop: it stages and applies
 * what the server sends, reads what the device still owes the server, and
 * keeps both cursors. Take one from {@link Database.deviceSync}.
 *
 * @public
 */
export interface DeviceSyncPort {
  /** Returns the identifier this device stamps its own changes with. */
  identity(): Promise<{ nodeId: string }>
  /** Applies one pulled transaction and advances the pull cursor in the same write. */
  applyPulledTransaction(
    changes: readonly ReplicationChange[],
    pullSeq: bigint,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult>
  /** Stores pulled changes durably without applying them, so an interrupted device resumes from disk. */
  stagePulledChanges(events: readonly ChangeEvent[]): Promise<bigint | null>
  /** Applies whatever is staged and returns the position reached. */
  applyStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<bigint | null>
  /** Applies what a previous run left staged, and reports the failure that stopped it. */
  recoverStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<StagedRecovery>
  /** Reads the next run of local changes the device still owes the server. */
  readOutboxBatch(afterSeq: bigint, limit: number): Promise<ReplicationBatch | null>
  /** Counts the local changes the device still owes the server. */
  countOutboxPending(afterSeq: bigint): Promise<number>
  /** Reads how far the device has pushed. */
  getPushCursor(): Promise<bigint>
  /** Records how far the device has pushed. */
  setPushCursor(seq: bigint): Promise<void>
  /** Reads how far the device has pulled, or null before it has pulled anything. */
  getPullState(): Promise<DeviceSyncPullState | null>
  /** Reports whether the server has told this device to download a fresh snapshot. */
  getResyncRequired(): Promise<boolean>
  /** Records whether the device must download a fresh snapshot. */
  setResyncRequired(required: boolean): Promise<void>
  /** Records how far the device has pulled, and the sequence space it came from. */
  setPullState(seq: bigint, epoch?: string): Promise<void>
  /** Keeps changes above a position out of pruning until the server has accepted them. */
  protectUnpushedChanges(pushedSeq: bigint): void
  /** Reports whether a snapshot download was interrupted and still has to finish. */
  snapshotLoadPending(): Promise<boolean>
  /** Clears the named tables and opens the database for a snapshot download. */
  beginSnapshotLoad(tables: readonly string[]): Promise<void>
  /** Applies the schema statements a snapshot carries. */
  applySnapshotSchema(schema: readonly string[]): Promise<void>
  /** Writes one page of snapshot rows into a table. */
  loadSnapshotPage(table: string, rows: readonly Record<string, unknown>[]): Promise<void>
  /** Replaces the local migration history with the server's. */
  replaceMigrationHistory(rows: readonly AppliedMigrationRow[]): Promise<void>
  /** Closes a completed snapshot download and reopens the database for normal use. */
  endSnapshotLoad(tables: readonly string[]): Promise<void>
  /** Abandons a snapshot download, leaving the database marked as needing another. */
  abortSnapshotLoad(): Promise<void>
}

export class DatabaseSyncController {
  private pkResolver: PkResolver | null = null
  private tablesReady = false
  private metaReady = false
  private outboxReader: BatchReader | null = null
  private localPruneBoundary: bigint | null = null
  private snapshotGate = false
  private readonly defaultResolver = new LWWResolver()

  constructor(
    private readonly runExclusive: RunExclusive,
    private readonly acquireWriter: () => SQLiteConnection,
    private readonly cdc: DatabaseCdcController,
  ) {}

  applyChanges(
    batch: ReplicationBatch,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    return this.runExclusive(async () => {
      const applier = await this.ensureApplier()
      const result = await applier.applyBatch(batch, resolver ?? this.defaultResolver)
      if (result.droppedTables.length > 0) {
        await this.cdc.changeTracker?.pruneDroppedTables(this.acquireWriter(), result.droppedTables)
      }
      return result
    })
  }

  devicePort(): DeviceSyncPort {
    return {
      identity: () => this.runExclusive(async () => ({ nodeId: (await this.cdc.ensureStamper()).nodeId })),
      applyPulledTransaction: (changes, pullSeq, resolver) => this.applyPulledTransaction(changes, pullSeq, resolver),
      stagePulledChanges: events => this.stagePulled(events),
      applyStagedPull: (resolver, onChange) => this.applyStagedPull(resolver, onChange),
      recoverStagedPull: (resolver, onChange) => this.recoverStagedPullState(resolver, onChange),
      readOutboxBatch: (afterSeq, limit) => this.readOutboxBatch(afterSeq, limit),
      countOutboxPending: afterSeq => this.countOutboxPending(afterSeq),
      getPushCursor: () => this.getMetaSeq(PUSHED_SEQ_META_KEY).then(seq => seq ?? 0n),
      setPushCursor: seq => this.setMetaSeq(PUSHED_SEQ_META_KEY, seq),
      getPullState: () => this.getPullState(),
      getResyncRequired: () => this.getResyncRequired(),
      setResyncRequired: required => this.setResyncRequired(required),
      setPullState: (seq, epoch) => this.setPullState(seq, epoch),
      protectUnpushedChanges: pushedSeq => {
        this.localPruneBoundary = pushedSeq
        this.applyLocalPruneBoundary()
      },
      snapshotLoadPending: () => this.runExclusive(() => snapshotLoadPending(this.acquireWriter())),
      beginSnapshotLoad: tables => this.beginSnapshotLoad(tables),
      applySnapshotSchema: schema => this.runExclusive(() => applySnapshotSchema(this.acquireWriter(), schema)),
      loadSnapshotPage: (table, rows) => this.runExclusive(() => loadSnapshotPage(this.acquireWriter(), table, rows)),
      replaceMigrationHistory: rows => this.replaceMigrationHistory(rows),
      endSnapshotLoad: tables => this.endSnapshotLoad(tables),
      abortSnapshotLoad: () => this.runExclusive(() => abortSnapshotLoad(this.acquireWriter())),
    }
  }

  get snapshotLoadBlocked(): boolean {
    return this.snapshotGate
  }

  seedSnapshotGate(): void {
    this.snapshotGate = true
  }

  private applyPulledTransaction(
    changes: readonly ReplicationChange[],
    pullSeq: bigint,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    return this.runExclusive(async () => {
      if (changes.length === 0) {
        return { applied: 0, skipped: 0, conflicts: 0, droppedTables: [] }
      }

      await this.ensureMeta()
      const applier = await this.ensureApplier()

      let maxHlc = ''
      let sourceNodeId = REMOTE_ORIGIN_NODE_ID
      for (const change of changes) {
        if (change.hlc > maxHlc) maxHlc = change.hlc
        if (sourceNodeId === REMOTE_ORIGIN_NODE_ID && change.nodeId !== '') sourceNodeId = change.nodeId
      }

      const result = await applier.applyGroup({
        sourceNodeId,
        txId: changes[0].txId,
        changes,
        resolver: resolver ?? this.defaultResolver,
        withinTx: tx => upsertMetaValue(tx, PULL_SEQ_META_KEY, pullSeq.toString()),
      })

      if (result.droppedTables.length > 0) {
        await this.cdc.changeTracker?.pruneDroppedTables(this.acquireWriter(), result.droppedTables)
      }
      if (maxHlc !== '') {
        await applier.mergeHlc(maxHlc)
      }
      return result
    })
  }

  private stagePulled(events: readonly ChangeEvent[]): Promise<bigint | null> {
    return this.runExclusive(async () => {
      await this.ensureMeta()
      return stagePulledChanges(this.acquireWriter(), events)
    })
  }

  private applyStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<bigint | null> {
    return this.runExclusive(async () => {
      await this.ensureMeta()
      const applier = await this.ensureApplier()
      return applyStagedTransactions(this.acquireWriter(), applier, this.stagedApplyOptions(resolver, onChange))
    })
  }

  private recoverStagedPullState(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<StagedRecovery> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      const applier = await this.ensureApplier()
      const recorded = await selectMetaValue(writer, PULL_SEQ_META_KEY)
      const appliedFloor = recorded !== null && SEQ_STRING_RE.test(recorded) ? BigInt(recorded) : null
      return recoverStagedPull(writer, applier, appliedFloor, this.stagedApplyOptions(resolver, onChange))
    })
  }

  private stagedApplyOptions(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): {
    resolver: ConflictResolver | ((table: string) => ConflictResolver)
    withinTx: (tx: SQLiteConnection, appliedThroughSeq: bigint) => Promise<void>
    onChange?: (event: ChangeEvent) => void
  } {
    return {
      resolver: resolver ?? this.defaultResolver,
      withinTx: (tx, appliedThroughSeq) => upsertMetaValue(tx, PULL_SEQ_META_KEY, appliedThroughSeq.toString()),
      onChange,
    }
  }

  private replaceMigrationHistory(rows: readonly AppliedMigrationRow[]): Promise<void> {
    return this.runExclusive(async () => {
      const writer = this.acquireWriter()
      await replaceMigrationHistory(writer, rows)
      await mirrorSchemaVersion(writer, highestMigrationVersion(rows))
    })
  }

  private async beginSnapshotLoad(tables: readonly string[]): Promise<void> {
    this.snapshotGate = true
    try {
      await this.runExclusive(() => beginSnapshotLoad(this.acquireWriter(), tables, this.cdc.changeTracker))
    } catch (err) {
      this.snapshotGate = await this.runExclusive(() => snapshotLoadPending(this.acquireWriter()))
      throw err
    }
  }

  private async endSnapshotLoad(tables: readonly string[]): Promise<void> {
    await this.runExclusive(() => endSnapshotLoad(this.acquireWriter(), tables, this.cdc.changeTracker))
    this.snapshotGate = false
  }

  private applyLocalPruneBoundary(): void {
    if (this.localPruneBoundary !== null) {
      this.cdc.changeTracker?.setPruneBoundary(this.localPruneBoundary)
    }
  }

  private readOutboxBatch(afterSeq: bigint, limit: number): Promise<ReplicationBatch | null> {
    return this.runExclusive(async () => {
      this.applyLocalPruneBoundary()
      const reader = await this.ensureOutboxReader()
      return reader.readBatch(afterSeq, limit)
    })
  }

  private countOutboxPending(afterSeq: bigint): Promise<number> {
    return this.runExclusive(async () => {
      this.applyLocalPruneBoundary()
      const writer = this.acquireWriter()
      await this.ensureSyncTables(writer)
      const stamper = await this.cdc.ensureStamper()
      return selectCountOutboundChanges(writer, CHANGES_TABLE, afterSeq, stamper.nodeId)
    })
  }

  private getResyncRequired(): Promise<boolean> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      return (await selectMetaValue(writer, RESYNC_REQUIRED_META_KEY)) === '1'
    })
  }

  private setResyncRequired(required: boolean): Promise<void> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      await upsertMetaValue(writer, RESYNC_REQUIRED_META_KEY, required ? '1' : '0')
    })
  }

  private getPullState(): Promise<DeviceSyncPullState | null> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      const seq = await selectMetaValue(writer, PULL_SEQ_META_KEY)
      if (seq === null || !SEQ_STRING_RE.test(seq)) return null
      const epoch = await selectMetaValue(writer, PULL_EPOCH_META_KEY)
      return { seq: BigInt(seq), epoch: epoch ?? undefined }
    })
  }

  private setPullState(seq: bigint, epoch?: string): Promise<void> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      await upsertMetaValue(writer, PULL_SEQ_META_KEY, seq.toString())
      if (epoch !== undefined) {
        await upsertMetaValue(writer, PULL_EPOCH_META_KEY, epoch)
      }
    })
  }

  private getMetaSeq(key: string): Promise<bigint | null> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      const value = await selectMetaValue(writer, key)
      if (value === null || !SEQ_STRING_RE.test(value)) return null
      return BigInt(value)
    })
  }

  private setMetaSeq(key: string, seq: bigint): Promise<void> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      await upsertMetaValue(writer, key, seq.toString())
    })
  }

  private async ensureMeta(): Promise<SQLiteConnection> {
    const writer = this.acquireWriter()
    if (!this.metaReady) {
      await ensureMetaTable(writer)
      this.metaReady = true
    }
    return writer
  }

  private async ensureSyncTables(writer: SQLiteConnection): Promise<void> {
    if (!this.tablesReady) {
      await ensureChangesTable(writer, CHANGES_TABLE)
      await ensureBatchApplyTables(writer)
      this.tablesReady = true
    }
  }

  private async ensureOutboxReader(): Promise<BatchReader> {
    const writer = this.acquireWriter()
    await this.ensureSyncTables(writer)
    const stamper = await this.cdc.ensureStamper()
    this.pkResolver ??= new PkResolver(writer)
    this.outboxReader ??= new BatchReader(writer, stamper.nodeId, CHANGES_TABLE, this.pkResolver, true)
    return this.outboxReader
  }

  private async ensureApplier(): Promise<BatchApplier> {
    const writer = this.acquireWriter()
    await this.ensureSyncTables(writer)
    const stamper = await this.cdc.ensureStamper()
    this.pkResolver ??= new PkResolver(writer)

    return new BatchApplier(
      writer,
      stamper.nodeId,
      stamper.hlc,
      this.pkResolver,
      fromNodeId => this.lastAppliedSeq(fromNodeId),
      this.cdc.changeTracker ?? undefined,
      CHANGES_TABLE,
      tx => this.catchUpLocalColumnVersions(tx, stamper.nodeId),
    )
  }

  private async catchUpLocalColumnVersions(tx: SQLiteConnection, localNodeId: string): Promise<void> {
    const recorded = await selectMetaValue(tx, COLUMN_VERSIONS_SEQ_META_KEY)
    const afterSeq = recorded !== null && SEQ_STRING_RE.test(recorded) ? BigInt(recorded) : 0n
    const latestSeq = await selectMaxChangeSeq(tx, CHANGES_TABLE)
    if (latestSeq <= afterSeq) return
    await recordLocalColumnVersions(tx, CHANGES_TABLE, localNodeId, afterSeq)
    await upsertMetaValue(tx, COLUMN_VERSIONS_SEQ_META_KEY, latestSeq.toString())
  }

  private lastAppliedSeq(fromNodeId: string): Promise<bigint> {
    return selectMaxAppliedSourceSeq(this.acquireWriter(), fromNodeId)
  }
}
