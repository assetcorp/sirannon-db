import type { ChangeTracker } from '../cdc/change-tracker.js'
import type { SQLiteConnection } from '../driver/types.js'
import { APPLIED_CHANGES_TABLE, CHANGES_TABLE, COLUMN_VERSIONS_TABLE } from '../internal-tables.js'
import { computeChecksum } from './checksum.js'
import { BatchValidationError } from './errors.js'
import type { HLC } from './hlc.js'
import { persistHlcClock } from './hlc-store.js'
import type { PkResolver } from './pk.js'
import { findRowByPk } from './row-lookup.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './types.js'
import { extractDroppedTable, IDENTIFIER_RE, validateDdlSafety, validateIdentifier } from './validators.js'

export class BatchApplier {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    private readonly pkResolver: PkResolver,
    private readonly getLastAppliedSeq: (fromNodeId: string) => Promise<bigint>,
    private readonly tracker?: ChangeTracker,
    private readonly changesTable: string = CHANGES_TABLE,
  ) {}

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
      appliedSeqSet = new Set<string>()
      const checkStmt = await this.conn.prepare(
        `SELECT source_seq FROM ${APPLIED_CHANGES_TABLE} WHERE source_node_id = ? AND source_seq >= ? AND source_seq <= ?`,
      )
      const applied = (await checkStmt.all(
        batch.sourceNodeId,
        batch.fromSeq.toString(),
        batch.toSeq.toString(),
      )) as Array<{ source_seq: number }>
      for (const row of applied) {
        appliedSeqSet.add(String(row.source_seq))
      }
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
      const txDroppedTables: string[] = []
      const result = await this.conn.transaction(async tx => {
        const seqBefore = await this.maxChangeSeq(tx)
        let txApplied = 0
        let txSkipped = 0
        let txConflicts = 0

        const ddlChanges = txChanges.filter(c => c.operation === 'ddl')
        const dataChanges = txChanges.filter(c => c.operation !== 'ddl')

        for (const ddl of ddlChanges) {
          const ddlSql = ddl.ddlStatement
          if (!ddlSql || !validateDdlSafety(ddlSql)) {
            throw new BatchValidationError(`Unsafe or missing DDL statement: ${ddlSql ?? 'none'}`)
          }
          await tx.exec(ddlSql)
          const droppedTable = extractDroppedTable(ddlSql)
          if (droppedTable !== null) {
            txDroppedTables.push(droppedTable)
          }
          if (this.tracker) {
            await this.tracker.refreshAllTriggersUsingConnection(tx)
          }
          txApplied += 1
        }

        for (const change of dataChanges) {
          const existingRow = await this.findExistingRow(tx, change)

          if (existingRow === undefined) {
            if (change.operation === 'insert' && change.newData) {
              await this.insertRow(tx, change)
              await this.recordColumnVersions(tx, change, change.newData)
              txApplied += 1
            } else if (change.operation === 'delete') {
              txApplied += 1
            } else {
              txSkipped += 1
            }
          } else {
            txConflicts += 1
            const localHlc = await this.getLocalHlcForRow(tx, change.table, change.rowId)

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
              await this.applyRemoteChange(tx, change)
              await this.recordColumnVersions(tx, change, change.newData)
              txApplied += 1
            } else if (resolution.action === 'merge' && resolution.mergedData) {
              await this.applyMergedData(tx, change, resolution.mergedData)
              await this.recordColumnVersions(tx, change, resolution.mergedData)
              txApplied += 1
            } else {
              txSkipped += 1
            }
          }
        }

        await this.stampAppliedEcho(tx, seqBefore, batch.sourceNodeId, txId, txChanges)

        return { txApplied, txSkipped, txConflicts }
      })

      applied += result.txApplied
      skipped += result.txSkipped
      conflicts += result.txConflicts
      if (txDroppedTables.length > 0) {
        droppedTables.push(...txDroppedTables)
      }
    }

    const recordStmt = await this.conn.prepare(
      `INSERT OR IGNORE INTO ${APPLIED_CHANGES_TABLE} (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)`,
    )
    const nowSec = Date.now() / 1000
    for (let seq = batch.fromSeq; seq <= batch.toSeq; seq += 1n) {
      if (appliedSeqSet?.has(seq.toString())) continue
      await recordStmt.run(batch.sourceNodeId, seq.toString(), nowSec)
    }

    try {
      const merged = this.hlc.receive(batch.hlcRange.max)
      await persistHlcClock(this.conn, merged)
    } catch {
      /* batch is already durable; HLC merge failure should not surface as a batch error */
    }

    return { applied, skipped, conflicts, droppedTables }
  }

  private async maxChangeSeq(tx: SQLiteConnection): Promise<string> {
    const stmt = await tx.prepare(`SELECT COALESCE(MAX(seq), 0) AS seq FROM "${this.changesTable}"`)
    const row = (await stmt.get()) as { seq?: unknown } | undefined
    return String(row?.seq ?? 0)
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
    const stmt = await tx.prepare(
      `UPDATE "${this.changesTable}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`,
    )
    await stmt.run(sourceNodeId, txId, maxHlc, seqBefore)
  }

  private async findExistingRow(
    tx: SQLiteConnection,
    change: ReplicationChange,
  ): Promise<Record<string, unknown> | undefined> {
    if (!IDENTIFIER_RE.test(change.table)) return undefined

    const pkColumns = await this.pkResolver.forTable(change.table)

    const result = await findRowByPk(tx, change.table, pkColumns, change.newData ?? change.oldData ?? {})
    if (result) return result

    if (change.operation === 'update' && change.oldData) {
      return findRowByPk(tx, change.table, pkColumns, change.oldData)
    }

    return undefined
  }

  private async getLocalHlcForRow(tx: SQLiteConnection, table: string, rowId: string): Promise<string | null> {
    const stmt = await tx.prepare(
      `SELECT MAX(hlc) as max_hlc FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`,
    )
    const row = (await stmt.get(table, rowId)) as { max_hlc: string | null } | undefined
    if (row?.max_hlc) return row.max_hlc

    const logStmt = await tx.prepare(
      `SELECT MAX(hlc) as max_hlc FROM "${this.changesTable}" WHERE table_name = ? AND row_id = ? AND hlc != ''`,
    )
    const logRow = (await logStmt.get(table, rowId)) as { max_hlc: string | null } | undefined
    return logRow?.max_hlc ?? null
  }

  private async insertRow(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    if (!change.newData) return

    const columns = Object.keys(change.newData).filter(validateIdentifier)
    if (columns.length === 0) return

    const placeholders = columns.map(() => '?').join(', ')
    const colNames = columns.map(c => `"${c}"`).join(', ')
    const values = columns.map(c => change.newData?.[c])

    const stmt = await tx.prepare(`INSERT INTO "${change.table}" (${colNames}) VALUES (${placeholders})`)
    await stmt.run(...values)
  }

  private async applyRemoteChange(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    if (change.operation === 'delete') {
      await this.deleteRow(tx, change)
      return
    }

    if (!change.newData) return

    const pkColumns = await this.pkResolver.forTable(change.table)
    const sourceData = change.newData
    const wherePkSource = change.oldData ?? sourceData

    const setClauses: string[] = []
    const setValues: unknown[] = []
    const whereConditions: string[] = []
    const whereValues: unknown[] = []

    const pkSet = new Set(pkColumns)

    for (const [col, val] of Object.entries(sourceData)) {
      if (!validateIdentifier(col)) continue
      if (pkSet.has(col)) continue
      setClauses.push(`"${col}" = ?`)
      setValues.push(val)
    }

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) continue
      whereConditions.push(`"${col}" = ?`)
      whereValues.push(wherePkSource[col])
    }

    if (setClauses.length === 0 || whereConditions.length === 0) return

    const stmt = await tx.prepare(
      `UPDATE "${change.table}" SET ${setClauses.join(', ')} WHERE ${whereConditions.join(' AND ')}`,
    )
    await stmt.run(...setValues, ...whereValues)
  }

  private async applyMergedData(
    tx: SQLiteConnection,
    change: ReplicationChange,
    mergedData: Record<string, unknown>,
  ): Promise<void> {
    const pkColumns = await this.pkResolver.forTable(change.table)
    const sourceData = change.newData ?? change.oldData ?? {}

    const setClauses: string[] = []
    const setValues: unknown[] = []
    const whereConditions: string[] = []
    const whereValues: unknown[] = []

    for (const [col, val] of Object.entries(mergedData)) {
      if (!validateIdentifier(col)) continue
      if (!pkColumns.includes(col)) {
        setClauses.push(`"${col}" = ?`)
        setValues.push(val)
      }
    }

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) continue
      whereConditions.push(`"${col}" = ?`)
      whereValues.push(sourceData[col])
    }

    if (setClauses.length === 0 || whereConditions.length === 0) return

    const stmt = await tx.prepare(
      `UPDATE "${change.table}" SET ${setClauses.join(', ')} WHERE ${whereConditions.join(' AND ')}`,
    )
    await stmt.run(...setValues, ...whereValues)
  }

  private async deleteRow(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    const pkColumns = await this.pkResolver.forTable(change.table)
    const sourceData = change.oldData ?? change.newData ?? {}

    const conditions: string[] = []
    const values: unknown[] = []

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) continue
      conditions.push(`"${col}" = ?`)
      values.push(sourceData[col])
    }

    if (conditions.length === 0) return

    const stmt = await tx.prepare(`DELETE FROM "${change.table}" WHERE ${conditions.join(' AND ')}`)
    await stmt.run(...values)
  }

  private async recordColumnVersions(
    tx: SQLiteConnection,
    change: ReplicationChange,
    data: Record<string, unknown> | null,
  ): Promise<void> {
    if (change.operation === 'delete') {
      const delStmt = await tx.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`)
      await delStmt.run(change.table, change.rowId)
      return
    }

    if (!data) return

    const upsertStmt = await tx.prepare(
      `INSERT INTO ${COLUMN_VERSIONS_TABLE} (table_name, row_id, column_name, hlc, node_id)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(table_name, row_id, column_name)
       DO UPDATE SET hlc = excluded.hlc, node_id = excluded.node_id`,
    )

    for (const col of Object.keys(data)) {
      if (!validateIdentifier(col)) continue
      await upsertStmt.run(change.table, change.rowId, col, change.hlc, change.nodeId)
    }
  }
}
