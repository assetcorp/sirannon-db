import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE, isReservedIdentifier } from '../internal-tables.js'
import { insertDdlChange, selectMaxChangeSeq, updateChangeStampsAfterSeqSql } from '../system-catalog/index.js'
import type { BatchApplierOptions } from './batch-applier-options.js'
import { assertTablesExist, type PlannedGroup, planBatch } from './batch-plan.js'
import { attributeChangeSeqs } from './change-seqs.js'
import { computeChecksum } from './checksum.js'
import { resolveConflict } from './conflict/resolve-conflict.js'
import { BatchValidationError } from './errors.js'
import { HLC } from './hlc.js'
import { persistHlcClock } from './hlc-store.js'
import { REMOTE_ORIGIN_NODE_ID } from './origins.js'
import type { PkResolver } from './pk.js'
import { completeStagedPush, recordAppliedSeqs, StagedPushSource, stagePushedChanges } from './push-staging.js'
import { RowWriter } from './row-writer.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './types.js'
import { extractDroppedTable, IDENTIFIER_RE, validateDdlSafety } from './validators.js'

interface ApplyTally {
  applied: number
  skipped: number
  conflicts: number
  writtenRows: Set<string>
}

export interface ApplyGroupOptions {
  sourceNodeId: string
  txId: string
  changes: readonly ReplicationChange[]
  resolver: ConflictResolver | ((table: string) => ConflictResolver)
  withinTx?: (tx: SQLiteConnection) => Promise<void>
}

export interface StagedGroupEntry {
  seq: bigint
  change: ReplicationChange
}

export interface StagedGroupSource {
  nextBatch(afterSeq: bigint): Promise<readonly StagedGroupEntry[]>
}

export interface ApplyStagedGroupOptions {
  source: StagedGroupSource
  resolver: ConflictResolver | ((table: string) => ConflictResolver)
  withinTx?: (tx: SQLiteConnection) => Promise<void>
}

export interface StagedGroupResult extends ApplyResult {
  maxHlc: string
  lastSeq: bigint | null
}

export class BatchApplier {
  private readonly rows: RowWriter
  private readonly changesTable: string

  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    pkResolver: PkResolver,
    private readonly getLastAppliedSeq: (fromNodeId: string) => Promise<bigint>,
    private readonly options: BatchApplierOptions = {},
  ) {
    this.changesTable = options.changesTable ?? CHANGES_TABLE
    this.rows = new RowWriter(pkResolver, this.changesTable)
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
        if (!IDENTIFIER_RE.test(change.table) || isReservedIdentifier(change.table)) {
          throw new BatchValidationError(`Invalid table name: ${change.table}`)
        }
      }
    }

    const seqs = attributeChangeSeqs(batch)
    await assertTablesExist(this.conn, batch.changes)

    const lastApplied = await this.getLastAppliedSeq(batch.sourceNodeId)
    if (batch.toSeq <= lastApplied) {
      return { applied: 0, skipped: batch.changes.length, conflicts: 0, droppedTables: [] }
    }

    const plan = await planBatch(this.conn, batch, seqs, lastApplied)
    const total: ApplyResult = { applied: 0, skipped: plan.skipped, conflicts: 0, droppedTables: [] }
    for (const group of plan.groups) {
      const result = await this.applyPlannedGroup(batch.sourceNodeId, group, resolver)
      total.applied += result.applied
      total.skipped += result.skipped
      total.conflicts += result.conflicts
      total.droppedTables.push(...result.droppedTables)
    }

    await this.mergeHlc(batch.hlcRange.max)

    return total
  }

  private async applyPlannedGroup(
    sourceNodeId: string,
    group: PlannedGroup,
    resolver: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    if (group.kind === 'stage') {
      await stagePushedChanges(this.conn, sourceNodeId, group.entries)
      return { applied: 0, skipped: 0, conflicts: 0, droppedTables: [] }
    }
    const seqs = group.entries.flatMap(entry => entry.seqs)
    if (group.kind === 'complete') {
      return this.applyStagedGroup({
        source: new StagedPushSource(this.conn, sourceNodeId, group.entries),
        resolver,
        withinTx: tx => completeStagedPush(tx, sourceNodeId, seqs),
      })
    }
    return this.applyGroup({
      sourceNodeId,
      txId: group.txId,
      changes: group.entries.map(entry => entry.change),
      resolver,
      withinTx: tx => recordAppliedSeqs(tx, sourceNodeId, seqs),
    })
  }

  async applyGroup(options: ApplyGroupOptions): Promise<ApplyResult> {
    const { sourceNodeId, txId, changes, resolver, withinTx } = options
    const droppedTables: string[] = []

    const result = await this.conn.transaction(async tx => {
      const seqBefore = await this.selectMaxChangeSeq(tx)
      if (this.options.beforeApply) {
        await this.options.beforeApply(tx)
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
        if (this.options.appliedSchemaChanges === 'logged') {
          await insertDdlChange(tx, this.changesTable, {
            ddlStatement: ddlSql,
            nodeId: sourceNodeId,
            txId: ddl.txId,
            hlc: ddl.hlc,
          })
        }
        const droppedTable = extractDroppedTable(ddlSql)
        if (droppedTable !== null) {
          droppedTables.push(droppedTable)
        }
        if (this.options.tracker) {
          await this.options.tracker.refreshAllTriggersUsingConnection(tx)
        }
        applied += 1
      }

      const tally: ApplyTally = { applied, skipped, conflicts, writtenRows: new Set() }
      for (const change of dataChanges) {
        await this.applyDataChange(tx, change, resolver, tally)
      }
      applied = tally.applied
      skipped = tally.skipped
      conflicts = tally.conflicts

      await this.stampAppliedEcho(tx, seqBefore, sourceNodeId, txId, changes)

      if (withinTx) {
        await withinTx(tx)
      }

      return { applied, skipped, conflicts }
    })

    return { ...result, droppedTables }
  }

  /**
   * Applies one staged transaction, reading its changes from `source` one batch
   * at a time so that memory use stays bounded for a transaction of any size.
   * The method writes every batch, the echo stamps, and the `withinTx` work
   * inside one local transaction, so a failure at any step rolls all of it back.
   */
  async applyStagedGroup(options: ApplyStagedGroupOptions): Promise<StagedGroupResult> {
    const { source, resolver, withinTx } = options

    return this.conn.transaction(async tx => {
      const seqBefore = await this.selectMaxChangeSeq(tx)
      if (this.options.beforeApply) {
        await this.options.beforeApply(tx)
      }

      const tally: ApplyTally = { applied: 0, skipped: 0, conflicts: 0, writtenRows: new Set() }
      let cursor = 0n
      let lastSeq: bigint | null = null
      let txId = ''
      let sawFirst = false
      let sourceNodeId = REMOTE_ORIGIN_NODE_ID
      let maxHlc = ''

      while (true) {
        const batch = await source.nextBatch(cursor)
        if (batch.length === 0) break
        for (const entry of batch) {
          const change = entry.change
          if (change.operation === 'ddl') {
            throw new BatchValidationError('A staged device transaction cannot carry a DDL change')
          }
          if (!IDENTIFIER_RE.test(change.table) || isReservedIdentifier(change.table)) {
            throw new BatchValidationError(`Invalid table name: ${change.table}`)
          }
          if (!sawFirst) {
            txId = change.txId
            sawFirst = true
          } else if (change.txId !== txId) {
            throw new BatchValidationError(`Staged rows span more than one transaction: '${txId}' and '${change.txId}'`)
          }
          if (sourceNodeId === REMOTE_ORIGIN_NODE_ID && change.nodeId !== '') {
            sourceNodeId = change.nodeId
          }
          if (change.hlc > maxHlc) {
            maxHlc = change.hlc
          }
          await this.applyDataChange(tx, change, resolver, tally)
          cursor = entry.seq
          lastSeq = entry.seq
        }
      }

      await this.stampAppliedEchoStamps(tx, seqBefore, sourceNodeId, txId, maxHlc)

      if (withinTx) {
        await withinTx(tx)
      }

      const { applied, skipped, conflicts } = tally
      return { applied, skipped, conflicts, droppedTables: [], maxHlc, lastSeq }
    })
  }

  private async applyDataChange(
    tx: SQLiteConnection,
    change: ReplicationChange,
    resolver: ConflictResolver | ((table: string) => ConflictResolver),
    tally: ApplyTally,
  ): Promise<void> {
    if (isReservedIdentifier(change.table)) {
      throw new BatchValidationError(`Invalid table name: ${change.table}`)
    }

    const rowKey = `${change.table}\u0000${change.rowId}`
    const existingRow = await this.rows.findExistingRow(tx, change)

    if (existingRow === undefined) {
      if (await this.applyToAbsentRow(tx, change, tally)) tally.writtenRows.add(rowKey)
      return
    }

    if (tally.writtenRows.has(rowKey)) {
      await this.rows.applyAfterSameTransaction(tx, change, existingRow)
      tally.applied += 1
      return
    }

    tally.conflicts += 1
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

    const resolution = await resolveConflict(resolver, {
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
    } else if (resolution.action === 'merge' && resolution.mergedData) {
      await this.rows.applyMergedData(tx, change, resolution.mergedData)
      await this.rows.recordColumnVersions(tx, change, resolution.mergedData)
    } else {
      tally.skipped += 1
      return
    }
    tally.applied += 1
    tally.writtenRows.add(rowKey)
  }

  private async applyToAbsentRow(tx: SQLiteConnection, change: ReplicationChange, tally: ApplyTally): Promise<boolean> {
    if (change.operation === 'delete') {
      await this.rows.recordTombstone(tx, change)
      tally.applied += 1
      return true
    }
    const deletedAt = await this.rows.getAbsentRowHlc(tx, change.table, change.rowId)
    const wins = deletedAt === null ? change.operation === 'insert' : HLC.compare(change.hlc, deletedAt) >= 0
    if (!wins || !change.newData) {
      tally.skipped += 1
      return false
    }
    await this.rows.insertRow(tx, change)
    await this.rows.recordColumnVersions(tx, change, change.newData)
    tally.applied += 1
    return true
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
    await this.stampAppliedEchoStamps(tx, seqBefore, sourceNodeId, txId, maxHlc)
  }

  private async stampAppliedEchoStamps(
    tx: SQLiteConnection,
    seqBefore: string,
    sourceNodeId: string,
    txId: string,
    maxHlc: string,
  ): Promise<void> {
    const stmt = await tx.prepare(updateChangeStampsAfterSeqSql(this.changesTable))
    await stmt.run(sourceNodeId, txId, maxHlc, seqBefore)
  }
}
