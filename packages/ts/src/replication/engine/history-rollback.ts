import { decodeTaggedValues } from '../../core/cdc/encoding.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE, isReservedIdentifier } from '../../core/internal-tables.js'
import { IDENTIFIER_RE } from '../../core/sync/validators.js'
import {
  deleteAppliedChangesAfterSeq,
  deleteColumnVersionsAfterHlc,
  deleteHistoryChangesAfterHlc,
  deletePeerState,
  type HistoryChangeRow,
  prepareInsertAppliedChange,
  selectHistoryChangesAfterHlc,
  selectHistoryTransaction,
  tablePkColumns,
  upsertPeerAckedSeq,
} from '../../core/system-catalog/index.js'
import { SyncError } from '../errors.js'
import type { SyncHistoryPoint } from '../types.js'
import type { ReplicationEngine } from './engine.js'
import { historyOrigins, selectHistoryBase, upsertAnchoredTerm } from './term-history.js'

const INTEGER_ROW_ID = /^-?\d+$/

interface RowLocator {
  clause: string
  values: unknown[]
}

function decodeImage(raw: string | null): Record<string, unknown> | null {
  if (raw === null || raw === '') return null
  return decodeTaggedValues(JSON.parse(raw)) as Record<string, unknown>
}

function rowIdValue(rowId: string): unknown {
  return INTEGER_ROW_ID.test(rowId) ? BigInt(rowId) : rowId
}

function assertColumns(table: string, image: Record<string, unknown>): string[] {
  const columns = Object.keys(image)
  for (const column of columns) {
    if (!IDENTIFIER_RE.test(column)) {
      throw new SyncError(`Cannot roll back a change to '${table}' with the column name '${column}'`)
    }
  }
  return columns
}

function locateRow(
  table: string,
  pkColumns: readonly string[],
  image: Record<string, unknown> | null,
  rowId: string,
): RowLocator {
  if (pkColumns.length === 0) return { clause: 'rowid = ?', values: [rowIdValue(rowId)] }
  if (!image) throw new SyncError(`Cannot roll back a change to '${table}' that records no row image`)
  const values: unknown[] = []
  for (const column of pkColumns) {
    if (!(column in image)) {
      throw new SyncError(`Cannot roll back a change to '${table}' whose row image lacks the key column '${column}'`)
    }
    values.push(image[column])
  }
  return { clause: pkColumns.map(column => `"${column}" = ?`).join(' AND '), values }
}

async function deleteRow(tx: SQLiteConnection, table: string, at: RowLocator): Promise<void> {
  const stmt = await tx.prepare(`DELETE FROM "${table}" WHERE ${at.clause}`)
  await stmt.run(...at.values)
}

async function restoreRow(
  tx: SQLiteConnection,
  table: string,
  image: Record<string, unknown> | null,
  at: RowLocator,
): Promise<void> {
  if (!image) throw new SyncError(`Cannot roll back an update to '${table}' that records no earlier row image`)
  const columns = assertColumns(table, image)
  if (columns.length === 0) return
  const stmt = await tx.prepare(
    `UPDATE "${table}" SET ${columns.map(column => `"${column}" = ?`).join(', ')} WHERE ${at.clause}`,
  )
  await stmt.run(...columns.map(column => image[column]), ...at.values)
}

async function insertRow(
  tx: SQLiteConnection,
  table: string,
  image: Record<string, unknown> | null,
  rowId: string | null,
): Promise<void> {
  if (!image) throw new SyncError(`Cannot roll back a delete from '${table}' that records no row image`)
  const columns = assertColumns(table, image)
  const names = columns.map(column => `"${column}"`)
  const values = columns.map(column => image[column])
  if (rowId !== null) {
    names.push('rowid')
    values.push(rowIdValue(rowId))
  }
  const stmt = await tx.prepare(
    `INSERT INTO "${table}" (${names.join(', ')}) VALUES (${names.map(() => '?').join(', ')})`,
  )
  await stmt.run(...values)
}

async function undoChange(
  tx: SQLiteConnection,
  row: HistoryChangeRow,
  pkColumnsByTable: Map<string, string[]>,
): Promise<void> {
  const table = row.table_name
  const operation = row.operation.toUpperCase()
  if (operation === 'DDL') {
    throw new SyncError('Cannot roll back a schema change, so the node needs a full copy')
  }
  if (!IDENTIFIER_RE.test(table) || isReservedIdentifier(table)) {
    throw new SyncError(`Cannot roll back a change to the table name '${table}'`)
  }
  let pkColumns = pkColumnsByTable.get(table)
  if (!pkColumns) {
    pkColumns = await tablePkColumns(tx, table)
    pkColumnsByTable.set(table, pkColumns)
  }
  const oldImage = decodeImage(row.old_data)
  const newImage = decodeImage(row.new_data)
  if (operation === 'INSERT') {
    await deleteRow(tx, table, locateRow(table, pkColumns, newImage, row.row_id))
  } else if (operation === 'UPDATE') {
    await restoreRow(tx, table, oldImage, locateRow(table, pkColumns, newImage, row.row_id))
  } else if (operation === 'DELETE') {
    await insertRow(tx, table, oldImage, pkColumns.length === 0 ? row.row_id : null)
  } else {
    throw new SyncError(`Cannot roll back the change operation '${row.operation}'`)
  }
}

/**
 * Discards every change in this node's history stamped after the last transaction that it shares with the current
 * primary, and records that the node has applied the primary's log up to `sourceSeq`, all in one transaction.
 *
 * The node undoes each discarded change from the row images that its change log stores, newest first, and deletes the
 * discarded changes and the column versions that they stamped. The change-log entries that the undo itself writes stay
 * unstamped, so that local subscribers and live queries receive the reverse changes while the undo replicates nowhere
 * and joins no history. It refuses, and changes nothing, when it holds no record of the shared transaction, when a full copy
 * left it holding a later transaction than the shared one, or when a discarded change is a schema change, because the
 * node can then reach the primary's history only through a full copy.
 *
 * @param engine - The engine of the replica that reconciles its history.
 * @param point - The last transaction that the node shares with the primary.
 * @param sourceNodeId - The current primary's node ID.
 * @param sourceSeq - The position in the primary's log after which the primary resends its history.
 * @param term - The primary term that the reconciled history belongs to.
 * @returns True when the node reconciled its history, and false when it needs a full copy.
 */
export function rollBackToHistoryPoint(
  engine: ReplicationEngine,
  point: SyncHistoryPoint,
  sourceNodeId: string,
  sourceSeq: bigint,
  term: bigint,
): Promise<boolean> {
  return engine.history.exclusive(() =>
    engine.writerConn.transaction(async tx => {
      const base = await selectHistoryBase(tx)
      const logged = await selectHistoryTransaction(tx, CHANGES_TABLE, point.txId)
      const cut = logged?.hlc ?? (base !== null && base.txId === point.txId ? base.hlc : null)
      if (cut === null || (base !== null && base.hlc > cut)) return false
      const origins = historyOrigins(engine)
      const divergent = await selectHistoryChangesAfterHlc(tx, CHANGES_TABLE, origins, cut)
      const pkColumnsByTable = new Map<string, string[]>()
      for (const row of divergent) {
        await undoChange(tx, row, pkColumnsByTable)
      }
      await deleteHistoryChangesAfterHlc(tx, CHANGES_TABLE, origins, cut)
      await deleteColumnVersionsAfterHlc(tx, cut)
      await deleteAppliedChangesAfterSeq(tx, sourceNodeId, sourceSeq)
      const recordStmt = await prepareInsertAppliedChange(tx)
      await recordStmt.run(sourceNodeId, sourceSeq.toString(), Date.now() / 1000)
      await deletePeerState(tx, sourceNodeId)
      await upsertPeerAckedSeq(tx, sourceNodeId, sourceSeq, Date.now() / 1000)
      await upsertAnchoredTerm(tx, term)
      return true
    }),
  )
}
