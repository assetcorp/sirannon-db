import type { SQLiteConnection } from '../driver/types.js'
import {
  deleteStagedPushChanges,
  selectAppliedSourceSeqsInRange,
  selectStagedPushSeqsInRange,
  selectStagedPushTail,
  selectTableExists,
} from '../system-catalog/index.js'
import { BatchValidationError } from './errors.js'
import type { PushEntry } from './push-staging.js'
import type { ReplicationBatch, ReplicationChange } from './types.js'

/**
 * Names what the receiving node does with one transaction's changes: `apply` commits them now, `stage` stores them
 * until a later batch continues the transaction, and `complete` commits them with the changes that earlier batches
 * stored.
 *
 * @internal
 */
export type PlannedGroupKind = 'apply' | 'stage' | 'complete'

/**
 * Holds the changes of one transaction in a batch, in batch order, with what the receiving node does with them.
 *
 * @internal
 */
export interface PlannedGroup {
  kind: PlannedGroupKind
  txId: string
  entries: PushEntry[]
}

/**
 * Holds the number of changes in a batch that the receiving node has applied already, and the groups that it
 * applies, in order.
 *
 * @internal
 */
export interface BatchPlan {
  skipped: number
  groups: PlannedGroup[]
}

const NO_SEQS: ReadonlySet<string> = new Set()

/**
 * Checks that the receiving database holds every table that a batch's row changes name. A batch with a schema change
 * passes unchecked, because its schema statement can create the tables that the later changes name.
 *
 * @param conn - The writer connection.
 * @param changes - The batch's changes.
 * @returns A promise that settles once every table has been found.
 * @throws {@link BatchValidationError} naming the first table that the database does not hold.
 * @internal
 */
export async function assertTablesExist(conn: SQLiteConnection, changes: readonly ReplicationChange[]): Promise<void> {
  if (changes.some(change => change.operation === 'ddl')) return
  const tables = new Set(changes.map(change => change.table))
  for (const table of tables) {
    if (!(await selectTableExists(conn, table))) {
      throw new BatchValidationError(`Table '${table}' does not exist on the receiving database`)
    }
  }
}

function groupEntries(
  entries: readonly PushEntry[],
  openTxId: string | null,
  stagedTxId: string | null,
): PlannedGroup[] {
  const byTx = new Map<string, PushEntry[]>()
  for (const entry of entries) {
    const group = byTx.get(entry.change.txId)
    if (group) {
      group.push(entry)
    } else {
      byTx.set(entry.change.txId, [entry])
    }
  }
  return [...byTx].map(([txId, txEntries]) => ({
    kind: txId === openTxId ? 'stage' : txId === stagedTxId ? 'complete' : 'apply',
    txId,
    entries: txEntries,
  }))
}

/**
 * Returns the changes of a batch that the receiving node has yet to apply, grouped by transaction, with each group
 * marked to apply now, to stage until its transaction continues, or to complete a transaction that earlier batches
 * staged. A change is left out when the applied-changes table records one of its sequences or the staging table
 * already holds it. When the batch opens a transaction other than the staged one, this function deletes the staged
 * changes, because the source has given that transaction up.
 *
 * @param conn - The writer connection.
 * @param batch - The validated batch.
 * @param seqs - The sequences of each change, from `attributeChangeSeqs`.
 * @param lastApplied - The highest sequence that the applied-changes table records for the batch's source.
 * @returns How many changes are left out, and the groups to apply in order.
 * @throws {@link BatchValidationError} when a batch whose transaction continues carries a schema change.
 * @internal
 */
export async function planBatch(
  conn: SQLiteConnection,
  batch: ReplicationBatch,
  seqs: readonly bigint[][],
  lastApplied: bigint,
): Promise<BatchPlan> {
  const source = batch.sourceNodeId
  const recorded =
    batch.fromSeq <= lastApplied
      ? await selectAppliedSourceSeqsInRange(conn, source, batch.fromSeq, lastApplied)
      : NO_SEQS
  let tail = batch.changeSeqs === undefined ? null : await selectStagedPushTail(conn, source)
  const staged =
    tail !== null && batch.fromSeq <= tail.maxSeq
      ? await selectStagedPushSeqsInRange(conn, source, batch.fromSeq, tail.maxSeq)
      : NO_SEQS

  const fresh: PushEntry[] = []
  for (const [index, change] of batch.changes.entries()) {
    const changeSeqs = seqs[index]
    if (changeSeqs.some(seq => recorded.has(seq.toString()) || staged.has(seq.toString()))) continue
    fresh.push({ change, seqs: changeSeqs })
  }

  if (tail !== null && fresh.length > 0 && fresh[0].change.txId !== tail.txId) {
    await deleteStagedPushChanges(conn, source)
    tail = null
  }

  const openTxId = batch.txContinues === true ? batch.changes[batch.changes.length - 1].txId : null
  if (openTxId !== null && batch.changes.some(change => change.operation === 'ddl')) {
    throw new BatchValidationError('A batch whose transaction continues cannot carry a DDL change')
  }
  return {
    skipped: batch.changes.length - fresh.length,
    groups: groupEntries(fresh, openTxId, tail?.txId ?? null),
  }
}
