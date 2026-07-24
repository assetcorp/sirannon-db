import type { DatabaseCdcController } from './database-cdc.js'
import type { SQLiteConnection } from './driver/types.js'
import { APPLIED_CHANGES_TABLE, CHANGES_TABLE } from './internal-tables.js'
import { mirrorSchemaVersion } from './migrations/schema-version.js'
import { BatchApplier } from './sync/batch-applier.js'
import { BatchReader } from './sync/batch-reader.js'
import { LWWResolver } from './sync/conflict/lww.js'
import { PkResolver } from './sync/pk.js'
import {
  abortSnapshotLoad,
  applySnapshotSchema,
  beginSnapshotLoad,
  endSnapshotLoad,
  loadSnapshotPage,
  snapshotLoadPending,
} from './sync/snapshot-apply.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
import { SEQ_STRING_RE } from './sync/validators.js'
import {
  type AppliedMigrationRow,
  ensureBatchApplyTables,
  ensureChangesTable,
  ensureMetaTable,
  getMetaValue,
  highestMigrationVersion,
  replaceMigrationHistory,
  setMetaValue,
} from './system-catalog/index.js'

type RunExclusive = <T>(op: () => Promise<T>) => Promise<T>

const PUSHED_SEQ_META_KEY = 'device_sync_pushed_seq'
const PULL_SEQ_META_KEY = 'device_sync_pull_seq'
const PULL_EPOCH_META_KEY = 'device_sync_pull_epoch'

export interface DeviceSyncPullState {
  seq: bigint
  epoch: string | undefined
}

export interface DeviceSyncPort {
  identity(): Promise<{ nodeId: string }>
  readOutboxBatch(afterSeq: bigint, limit: number): Promise<ReplicationBatch | null>
  countOutboxPending(afterSeq: bigint): Promise<number>
  getPushCursor(): Promise<bigint>
  setPushCursor(seq: bigint): Promise<void>
  getPullState(): Promise<DeviceSyncPullState | null>
  setPullState(seq: bigint, epoch?: string): Promise<void>
  protectUnpushedChanges(pushedSeq: bigint): void
  snapshotLoadPending(): Promise<boolean>
  beginSnapshotLoad(tables: readonly string[]): Promise<void>
  applySnapshotSchema(schema: readonly string[]): Promise<void>
  loadSnapshotPage(table: string, rows: readonly Record<string, unknown>[]): Promise<void>
  replaceMigrationHistory(rows: readonly AppliedMigrationRow[]): Promise<void>
  endSnapshotLoad(tables: readonly string[]): Promise<void>
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
      readOutboxBatch: (afterSeq, limit) => this.readOutboxBatch(afterSeq, limit),
      countOutboxPending: afterSeq => this.countOutboxPending(afterSeq),
      getPushCursor: () => this.getMetaSeq(PUSHED_SEQ_META_KEY).then(seq => seq ?? 0n),
      setPushCursor: seq => this.setMetaSeq(PUSHED_SEQ_META_KEY, seq),
      getPullState: () => this.getPullState(),
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
      const stmt = await writer.prepare(
        `SELECT COUNT(*) AS pending FROM ${CHANGES_TABLE} WHERE seq > ? AND node_id = ? AND operation != 'DDL'`,
      )
      const row = (await stmt.get(afterSeq.toString(), stamper.nodeId)) as { pending: number | bigint }
      return Number(row.pending)
    })
  }

  private getPullState(): Promise<DeviceSyncPullState | null> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      const seq = await getMetaValue(writer, PULL_SEQ_META_KEY)
      if (seq === null || !SEQ_STRING_RE.test(seq)) return null
      const epoch = await getMetaValue(writer, PULL_EPOCH_META_KEY)
      return { seq: BigInt(seq), epoch: epoch ?? undefined }
    })
  }

  private setPullState(seq: bigint, epoch?: string): Promise<void> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      await setMetaValue(writer, PULL_SEQ_META_KEY, seq.toString())
      if (epoch !== undefined) {
        await setMetaValue(writer, PULL_EPOCH_META_KEY, epoch)
      }
    })
  }

  private getMetaSeq(key: string): Promise<bigint | null> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      const value = await getMetaValue(writer, key)
      if (value === null || !SEQ_STRING_RE.test(value)) return null
      return BigInt(value)
    })
  }

  private setMetaSeq(key: string, seq: bigint): Promise<void> {
    return this.runExclusive(async () => {
      const writer = await this.ensureMeta()
      await setMetaValue(writer, key, seq.toString())
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
    )
  }

  private async lastAppliedSeq(fromNodeId: string): Promise<bigint> {
    const stmt = await this.acquireWriter().prepare(
      `SELECT MAX(source_seq) AS max_seq FROM ${APPLIED_CHANGES_TABLE} WHERE source_node_id = ?`,
    )
    const row = (await stmt.get(fromNodeId)) as { max_seq: number | bigint | null } | undefined
    if (!row || row.max_seq === null || row.max_seq === undefined) return 0n
    return BigInt(row.max_seq)
  }
}
