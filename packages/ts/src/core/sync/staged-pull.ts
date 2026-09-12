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
 * Writes pulled changes to the staging table in one transaction so that a device
 * holds an in-flight transaction on disk instead of in memory. Returns the
 * highest staged sequence; a commit here is what makes that sequence safe to
 * acknowledge, because the rows survive a crash and are applied on restart.
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
 * Applies every complete staged transaction in sequence order. Each group
 * runs in one local transaction that also records the pull cursor through
 * `withinTx`; the staged rows are deleted only after that transaction
 * commits and `onChange` has reported them, so a crash at any point either
 * re-applies nothing or re-runs a delete that is guarded by the recorded
 * cursor. Returns the sequence applied through, or null when no staged
 * transaction was complete.
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
   * The failure that stopped the recovery apply, or null. The staged rows
   * are untouched by the failure and the resume watermark still covers
   * them, so the caller opens the subscription anyway: a schema-gate refusal
   * there is what tells a device it must migrate before this apply can
   * succeed. The caller must then retry the recovery, because the resume
   * watermark is past the transaction this apply left unapplied, so the
   * server sends nothing that would prompt another try.
   */
  applyError: unknown | null
}

/**
 * Restores staging to a consistent state after a restart: staged rows at or
 * below the recorded pull cursor were already applied and are dropped, every
 * complete staged transaction is applied, and an incomplete tail is kept so
 * the resumed stream can finish it. Returns the sequence to resume the pull
 * subscription from.
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
