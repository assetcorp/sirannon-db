import type { ChangeTracker } from '../cdc/change-tracker.js'
import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE } from '../internal-tables.js'
import {
  prepareInsertAppliedChange,
  selectAppliedSourceSeqsInRange,
  selectMaxChangeSeq,
  updateChangeStampsAfterSeqSql,
} from '../system-catalog/index.js'
import { computeChecksum } from './checksum.js'
import { BatchValidationError } from './errors.js'
import type { HLC } from './hlc.js'
import { persistHlcClock } from './hlc-store.js'
import type { PkResolver } from './pk.js'
import { RowWriter } from './row-writer.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './types.js'
import { extractDroppedTable, IDENTIFIER_RE, validateDdlSafety } from './validators.js'

export interface ApplyGroupOptions {
  sourceNodeId: string
  txId: string
  changes: readonly ReplicationChange[]
  resolver: ConflictResolver | ((table: string) => ConflictResolver)
  withinTx?: (tx: SQLiteConnection) => Promise<void>
}

export class BatchApplier {
  private readonly rows: RowWriter

  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    pkResolver: PkResolver,
    private readonly getLastAppliedSeq: (fromNodeId: string) => Promise<bigint>,
    private readonly tracker?: ChangeTracker,
    private readonly changesTable: string = CHANGES_TABLE,
    private readonly beforeApply?: (tx: SQLiteConnection) => Promise<void>,
  ) {
    this.rows = new RowWriter(pkResolver, changesTable)
  }

  async applyBatch(
    batch: ReplicationBatch,
    resolver: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    const expectedChecksum = computeChecksum(batch.changes)
    if (batch.checksum !== expectedChecksum) {
      throw new BatchValidationError(`Checksum mismatch: expected ${expectedChecksum}, got ${batch.checksum}`)
    }

    for (const change of batch.changes) {
      if (change.operation !== 'ddl') {
        if (!IDENTIFIER_RE.test(change.table)) {
          throw new BatchValidationError(`Invalid table name: ${change.table}`)
        }
      }
    }

    const lastApplied = await this.getLastAppliedSeq(batch.sourceNodeId)
    if (batch.toSeq <= lastApplied) {
      return { applied: 0, skipped: batch.changes.length, conflicts: 0, droppedTables: [] }
    }

    const needsPartialDedup = batch.fromSeq <= lastApplied
    let appliedSeqSet: Set<string> | null = null
    if (needsPartialDedup) {
      appliedSeqSet = await selectAppliedSourceSeqsInRange(this.conn, batch.sourceNodeId, batch.fromSeq, batch.toSeq)
    }

    let applied = 0
    let skipped = 0
    let conflicts = 0
    const droppedTables: string[] = []

    const changesByTx = new Map<string, ReplicationChange[]>()
    for (const change of batch.changes) {
      const txGroup = changesByTx.get(change.txId)
      if (txGroup) {
        txGroup.push(change)
      } else {
        changesByTx.set(change.txId, [change])
      }
    }

    for (const [txId, txChanges] of changesByTx) {
      const result = await this.applyGroup({
        sourceNodeId: batch.sourceNodeId,
        txId,
        changes: txChanges,
        resolver,
      })

      applied += result.applied
      skipped += result.skipped
      conflicts += result.conflicts
      droppedTables.push(...result.droppedTables)
    }

    const recordStmt = await prepareInsertAppliedChange(this.conn)
    const nowSec = Date.now() / 1000
    for (let seq = batch.fromSeq; seq <= batch.toSeq; seq += 1n) {
      if (appliedSeqSet?.has(seq.toString())) continue
      await recordStmt.run(batch.sourceNodeId, seq.toString(), nowSec)
    }

    await this.mergeHlc(batch.hlcRange.max)

    return { applied, skipped, conflicts, droppedTables }
  }

  async applyGroup(options: ApplyGroupOptions): Promise<ApplyResult> {
    const { sourceNodeId, txId, changes, resolver, withinTx } = options
    const droppedTables: string[] = []

    const result = await this.conn.transaction(async tx => {
      const seqBefore = await this.selectMaxChangeSeq(tx)
      if (this.beforeApply) {
        await this.beforeApply(tx)
      }
      let applied = 0
      let skipped = 0
      let conflicts = 0

      const ddlChanges = changes.filter(c => c.operation === 'ddl')
      const dataChanges = changes.filter(c => c.operation !== 'ddl')

      for (const ddl of ddlChanges) {
        const ddlSql = ddl.ddlStatement
        if (!ddlSql || !validateDdlSafety(ddlSql)) {
          throw new BatchValidationError(`Unsafe or missing DDL statement: ${ddlSql ?? 'none'}`)
        }
        await tx.exec(ddlSql)
        const droppedTable = extractDroppedTable(ddlSql)
        if (droppedTable !== null) {
          droppedTables.push(droppedTable)
        }
        if (this.tracker) {
          await this.tracker.refreshAllTriggersUsingConnection(tx)
        }
        applied += 1
      }

      for (const change of dataChanges) {
        const existingRow = await this.rows.findExistingRow(tx, change)

        if (existingRow === undefined) {
          if (change.operation === 'insert' && change.newData) {
            await this.rows.insertRow(tx, change)
            await this.rows.recordColumnVersions(tx, change, change.newData)
            applied += 1
          } else if (change.operation === 'delete') {
            applied += 1
          } else {
            skipped += 1
          }
          continue
        }

        conflicts += 1
        const localHlc = await this.rows.getLocalHlcForRow(tx, change.table, change.rowId)

        const localChange: ReplicationChange = {
          table: change.table,
          operation: 'update',
          rowId: change.rowId,
          primaryKey: change.primaryKey,
          hlc: localHlc ?? '',
          txId: '',
          nodeId: this.localNodeId,
          newData: existingRow,
          oldData: null,
        }

        const changeResolver = typeof resolver === 'function' ? resolver(change.table) : resolver
        const resolution = await changeResolver.resolve({
          table: change.table,
          rowId: change.rowId,
          localChange,
          remoteChange: change,
          localHlc,
          remoteHlc: change.hlc,
        })

        if (resolution.action === 'accept_remote') {
          await this.rows.applyRemoteChange(tx, change)
          await this.rows.recordColumnVersions(tx, change, change.newData)
          applied += 1
        } else if (resolution.action === 'merge' && resolution.mergedData) {
          await this.rows.applyMergedData(tx, change, resolution.mergedData)
          await this.rows.recordColumnVersions(tx, change, resolution.mergedData)
          applied += 1
        } else {
          skipped += 1
        }
      }

      await this.stampAppliedEcho(tx, seqBefore, sourceNodeId, txId, changes)

      if (withinTx) {
        await withinTx(tx)
      }

      return { applied, skipped, conflicts }
    })

    return { ...result, droppedTables }
  }

  async mergeHlc(remoteHlc: string): Promise<void> {
    try {
      const merged = this.hlc.receive(remoteHlc)
      await persistHlcClock(this.conn, merged)
    } catch {
      return
    }
  }

  private async selectMaxChangeSeq(tx: SQLiteConnection): Promise<string> {
    return (await selectMaxChangeSeq(tx, this.changesTable)).toString()
  }

  private async stampAppliedEcho(
    tx: SQLiteConnection,
    seqBefore: string,
    sourceNodeId: string,
    txId: string,
    txChanges: readonly ReplicationChange[],
  ): Promise<void> {
    let maxHlc = ''
    for (const change of txChanges) {
      if (change.hlc > maxHlc) {
        maxHlc = change.hlc
      }
    }
    const stmt = await tx.prepare(updateChangeStampsAfterSeqSql(this.changesTable))
    await stmt.run(sourceNodeId, txId, maxHlc, seqBefore)
  }
}
