import type { DatabaseCdcController } from './database-cdc.js'
import type { SQLiteConnection } from './driver/types.js'
import { APPLIED_CHANGES_TABLE, CHANGES_TABLE } from './internal-tables.js'
import { BatchApplier } from './sync/batch-applier.js'
import { LWWResolver } from './sync/conflict/lww.js'
import { PkResolver } from './sync/pk.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch } from './sync/types.js'
import { ensureBatchApplyTables, ensureChangesTable } from './system-catalog/index.js'

type RunExclusive = <T>(op: () => Promise<T>) => Promise<T>

export class DatabaseSyncController {
  private pkResolver: PkResolver | null = null
  private tablesReady = false
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

  private async ensureApplier(): Promise<BatchApplier> {
    const writer = this.acquireWriter()
    if (!this.tablesReady) {
      await ensureChangesTable(writer, CHANGES_TABLE)
      await ensureBatchApplyTables(writer)
      this.tablesReady = true
    }
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
