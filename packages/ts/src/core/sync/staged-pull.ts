import { invokeCallerCallback } from '../caller-callbacks.js'
import {
  changedAtToEventTimestamp,
  decodeTaggedValues,
  encodeTaggedValues,
  eventTimestampToChangedAt,
} from '../cdc/encoding.js'
import type { SQLiteConnection } from '../driver/types.js'
import {
  deleteStagedChangesUpToSeq,
  ensureStagedChangesTable,
  type StagedChangeRow,
  selectFirstStagedTransactionEnd,
  selectMaxStagedSeq,
  selectStagedChangesInRange,
  upsertStagedChangeSql,
} from '../system-catalog/index.js'
import type { ChangeEvent } from '../types.js'
import type { BatchApplier, StagedGroupEntry } from './batch-applier.js'
import type { ConflictResolver, ReplicationChange } from './types.js'

const STAGED_APPLY_BATCH_ROWS = 500

export interface ApplyStagedOptions {
  resolver: ConflictResolver | ((table: string) => ConflictResolver)
  withinTx?: (tx: SQLiteConnection, appliedThroughSeq: bigint) => Promise<void>
  onChange?: (event: ChangeEvent) => void
}

/**
 * Writes pulled changes to the staging table in one transaction, so that a
 * device keeps a partly received transaction on disk, and returns the highest
 * staged sequence number, or `null` for an empty list. Once this transaction
 * commits, the device can acknowledge that sequence number, because the staged
 * rows stay on disk after a crash and `recoverStagedPull` applies them on restart.
 */
export async function stagePulledChanges(
  conn: SQLiteConnection,
  events: readonly ChangeEvent[],
): Promise<bigint | null> {
  if (events.length === 0) return null
  await ensureStagedChangesTable(conn)

  let maxSeq: bigint | null = null
  await conn.transaction(async tx => {
    const stmt = await tx.prepare(upsertStagedChangeSql())
    for (const event of events) {
      await stmt.run(
        event.seq.toString(),
        event.table,
        event.type,
        event.rowId ?? '',
        eventTimestampToChangedAt(event.timestamp),
        event.oldRow === undefined ? null : JSON.stringify(encodeTaggedValues(event.oldRow)),
        event.type === 'delete' ? null : JSON.stringify(encodeTaggedValues(event.row)),
        event.origin ?? '',
        event.txId ?? '',
        event.hlc ?? '',
        event.txEnd === true ? 1 : 0,
      )
      if (maxSeq === null || event.seq > maxSeq) {
        maxSeq = event.seq
      }
    }
  })
  return maxSeq
}

/**
 * Applies every complete staged transaction in sequence order, and returns the
 * sequence number of the last one that it applies, or `null` when no staged
 * transaction is complete. The function applies each transaction inside one
 * local transaction that also records the pull cursor through `withinTx`. It
 * deletes the staged rows once that transaction commits and `onChange` receives
 * their events, so after a crash, `recoverStagedPull` uses the recorded cursor
 * to delete the applied rows without applying them twice.
 */
export async function applyStagedTransactions(
  conn: SQLiteConnection,
  applier: BatchApplier,
  options: ApplyStagedOptions,
): Promise<bigint | null> {
  await ensureStagedChangesTable(conn)

  let appliedThrough: bigint | null = null
  while (true) {
    const head = await selectFirstStagedTransactionEnd(conn)
    if (head === null) break
    const endSeq = head.seq

    const result = await applier.applyStagedGroup({
      source: {
        nextBatch: afterSeq => readStagedEntries(conn, afterSeq, endSeq),
      },
      resolver: options.resolver,
      withinTx: options.withinTx === undefined ? undefined : tx => options.withinTx?.(tx, endSeq) ?? Promise.resolve(),
    })

    if (result.maxHlc !== '') {
      await applier.mergeHlc(result.maxHlc)
    }
    if (options.onChange) {
      await emitStagedEvents(conn, endSeq, options.onChange)
    }
    await deleteStagedChangesUpToSeq(conn, endSeq)
    appliedThrough = endSeq
  }
  return appliedThrough
}

export interface StagedRecovery {
  resumeSeq: bigint | null
  appliedSeq: bigint | null
  /**
   * The error that stopped the recovery apply, or `null`. The staged rows stay
   * in place after the failed apply, and the resume sequence still covers
   * them, so the caller opens the subscription regardless. When the server's
   * schema gate refuses that subscription, the refusal error states that the
   * device has to migrate first. The caller then has to retry the recovery
   * itself, because the resume sequence lies past the unapplied transaction,
   * so the server resends none of it.
   */
  applyError: unknown | null
}

/**
 * Brings the staging table back to a consistent state after a restart, and
 * returns the sequence number from which to resume the pull subscription.
 * The function deletes the staged rows at or below the recorded pull cursor,
 * since the device applied them before the restart. It then applies every
 * complete staged transaction and keeps an incomplete tail, so that the
 * resumed stream can finish that transaction.
 */
export async function recoverStagedPull(
  conn: SQLiteConnection,
  applier: BatchApplier,
  appliedFloor: bigint | null,
  options: ApplyStagedOptions,
): Promise<StagedRecovery> {
  await ensureStagedChangesTable(conn)
  if (appliedFloor !== null) {
    await deleteStagedChangesUpToSeq(conn, appliedFloor)
  }
  let appliedThrough: bigint | null = null
  let applyError: unknown | null = null
  try {
    appliedThrough = await applyStagedTransactions(conn, applier, options)
  } catch (err) {
    applyError = err
  }
  const stagedMax = await selectMaxStagedSeq(conn)

  const appliedSeq = maxSeq(appliedFloor, appliedThrough)
  return { resumeSeq: maxSeq(appliedSeq, stagedMax), appliedSeq, applyError }
}

function maxSeq(a: bigint | null, b: bigint | null): bigint | null {
  if (a === null) return b
  if (b === null) return a
  return a > b ? a : b
}

async function readStagedEntries(
  conn: SQLiteConnection,
  afterSeq: bigint,
  upToSeq: bigint,
): Promise<StagedGroupEntry[]> {
  const rows = await selectStagedChangesInRange(conn, afterSeq, upToSeq, STAGED_APPLY_BATCH_ROWS)
  return rows.map(row => ({ seq: BigInt(row.seq), change: rowToReplicationChange(row) }))
}

function rowToReplicationChange(row: StagedChangeRow): ReplicationChange {
  return {
    table: row.table_name,
    operation: row.operation as ReplicationChange['operation'],
    rowId: row.row_id,
    primaryKey: {},
    hlc: row.hlc,
    txId: row.tx_id,
    nodeId: row.node_id,
    newData: row.new_data === null ? null : (decodeTaggedValues(JSON.parse(row.new_data)) as Record<string, unknown>),
    oldData: row.old_data === null ? null : (decodeTaggedValues(JSON.parse(row.old_data)) as Record<string, unknown>),
  }
}

function rowToChangeEvent(row: StagedChangeRow): ChangeEvent {
  return {
    type: row.operation as ChangeEvent['type'],
    table: row.table_name,
    row: row.new_data === null ? {} : (decodeTaggedValues(JSON.parse(row.new_data)) as Record<string, unknown>),
    oldRow:
      row.old_data === null ? undefined : (decodeTaggedValues(JSON.parse(row.old_data)) as Record<string, unknown>),
    seq: BigInt(row.seq),
    timestamp: changedAtToEventTimestamp(row.changed_at),
    rowId: row.row_id,
    ...(row.node_id !== '' ? { origin: row.node_id } : {}),
    ...(row.hlc !== '' ? { hlc: row.hlc } : {}),
    ...(row.tx_id !== '' ? { txId: row.tx_id } : {}),
    ...(row.tx_end === 1 ? { txEnd: true } : {}),
  }
}

async function emitStagedEvents(
  conn: SQLiteConnection,
  upToSeq: bigint,
  onChange: (event: ChangeEvent) => void,
): Promise<void> {
  let cursor = 0n
  while (true) {
    const rows = await selectStagedChangesInRange(conn, cursor, upToSeq, STAGED_APPLY_BATCH_ROWS)
    if (rows.length === 0) return
    for (const row of rows) {
      cursor = BigInt(row.seq)
      const event = rowToChangeEvent(row)
      invokeCallerCallback(() => onChange(event))
    }
  }
}
