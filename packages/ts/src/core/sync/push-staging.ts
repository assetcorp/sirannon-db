import * as v from 'valibot'
import { decodeTaggedValues, encodeTaggedValues } from '../cdc/encoding.js'
import type { SQLiteConnection } from '../driver/types.js'
import {
  deleteStagedPushChanges,
  insertAppliedChangesFromStagedPush,
  prepareInsertAppliedChange,
  prepareInsertStagedPushChange,
  selectStagedPushChangesAfter,
} from '../system-catalog/index.js'
import type { StagedGroupEntry, StagedGroupSource } from './batch-applier.js'
import { ReplicationError } from './errors.js'
import type { ReplicationChange } from './types.js'

const STAGED_PUSH_READ_ROWS = 500

const ROW_VALUES_SCHEMA = v.record(v.string(), v.unknown())

const STORED_CHANGE_SCHEMA = v.object({
  table: v.string(),
  operation: v.picklist(['insert', 'update', 'delete', 'ddl']),
  rowId: v.string(),
  primaryKey: ROW_VALUES_SCHEMA,
  hlc: v.string(),
  txId: v.string(),
  nodeId: v.string(),
  newData: v.nullable(ROW_VALUES_SCHEMA),
  oldData: v.nullable(ROW_VALUES_SCHEMA),
  ddlStatement: v.optional(v.string()),
})

/**
 * Pairs one change that a device pushed with the sequences that the device gave it.
 *
 * @internal
 */
export interface PushEntry {
  change: ReplicationChange
  seqs: bigint[]
}

function encodeChange(change: ReplicationChange): string {
  return JSON.stringify({
    ...change,
    primaryKey: encodeTaggedValues(change.primaryKey),
    newData: change.newData === null ? null : encodeTaggedValues(change.newData),
    oldData: change.oldData === null ? null : encodeTaggedValues(change.oldData),
  })
}

function storedChangeError(sourceNodeId: string, detail: string): ReplicationError {
  return new ReplicationError(
    `A change that this server holds from device '${sourceNodeId}' is unreadable: ${detail}`,
    'REPLICATION_ERROR',
    { sourceNodeId },
  )
}

function decodeRowValues(sourceNodeId: string, values: Record<string, unknown>): Record<string, unknown> {
  const decoded = v.safeParse(ROW_VALUES_SCHEMA, decodeTaggedValues(values))
  if (!decoded.success) throw storedChangeError(sourceNodeId, v.summarize(decoded.issues))
  return decoded.output
}

function decodeChange(sourceNodeId: string, stored: string): ReplicationChange {
  let json: unknown
  try {
    json = JSON.parse(stored)
  } catch (err) {
    throw storedChangeError(sourceNodeId, err instanceof Error ? err.message : String(err))
  }
  const parsed = v.safeParse(STORED_CHANGE_SCHEMA, json)
  if (!parsed.success) throw storedChangeError(sourceNodeId, v.summarize(parsed.issues))
  const raw = parsed.output
  return {
    ...raw,
    primaryKey: decodeRowValues(sourceNodeId, raw.primaryKey),
    newData: raw.newData === null ? null : decodeRowValues(sourceNodeId, raw.newData),
    oldData: raw.oldData === null ? null : decodeRowValues(sourceNodeId, raw.oldData),
  }
}

/**
 * Stores the changes of a device transaction that the device has not finished pushing, in one SQLite transaction,
 * and leaves any change that the server already holds unchanged.
 *
 * @param conn - The writer connection.
 * @param sourceNodeId - The device that pushed the changes.
 * @param entries - The pushed changes, each with its sequences on the device.
 * @returns A promise that settles once SQLite has committed the changes.
 * @internal
 */
export async function stagePushedChanges(
  conn: SQLiteConnection,
  sourceNodeId: string,
  entries: readonly PushEntry[],
): Promise<void> {
  await conn.transaction(async tx => {
    const stmt = await prepareInsertStagedPushChange(tx)
    for (const entry of entries) {
      await stmt.run(sourceNodeId, entry.seqs[0].toString(), entry.change.txId, encodeChange(entry.change))
    }
  })
}

/**
 * Records a device's sequences as applied, in the transaction that applies their changes.
 *
 * @param tx - The writer connection, inside the transaction that applies the changes.
 * @param sourceNodeId - The device that made the changes.
 * @param seqs - The sequences to record, which can be empty.
 * @returns A promise that settles once SQLite has inserted the records.
 * @internal
 */
export async function recordAppliedSeqs(
  tx: SQLiteConnection,
  sourceNodeId: string,
  seqs: readonly bigint[],
): Promise<void> {
  if (seqs.length === 0) return
  const stmt = await prepareInsertAppliedChange(tx)
  const appliedAt = Date.now() / 1000
  for (const seq of seqs) {
    await stmt.run(sourceNodeId, seq.toString(), appliedAt)
  }
}

/**
 * Finishes a device transaction that arrived across several pushes: it records every stored change and every
 * change of the last push as applied, then deletes the stored changes, all in the transaction that applies them.
 *
 * @param tx - The writer connection, inside the transaction that applies the changes.
 * @param sourceNodeId - The device that made the changes.
 * @param tailSeqs - The sequences of the changes in the last push.
 * @returns A promise that settles once SQLite has written the records and deleted the stored changes.
 * @internal
 */
export async function completeStagedPush(
  tx: SQLiteConnection,
  sourceNodeId: string,
  tailSeqs: readonly bigint[],
): Promise<void> {
  await insertAppliedChangesFromStagedPush(tx, sourceNodeId, Date.now() / 1000)
  await recordAppliedSeqs(tx, sourceNodeId, tailSeqs)
  await deleteStagedPushChanges(tx, sourceNodeId)
}

/**
 * Hands the batch applier a device transaction that arrived across several pushes: first the changes that the server
 * stored, in pages of 500 oldest first, then the changes of the last push.
 *
 * @internal
 */
export class StagedPushSource implements StagedGroupSource {
  private stagedRead = false
  private tailRead = false

  constructor(
    private readonly conn: SQLiteConnection,
    private readonly sourceNodeId: string,
    private readonly tail: readonly PushEntry[],
  ) {}

  /**
   * Returns the next page of changes after a sequence.
   *
   * @param afterSeq - The sequence of the last change that the applier has received.
   * @returns The next changes, each with its sequence, or an empty array once the applier has received them all.
   * @throws {@link ReplicationError} when a stored change is not the JSON of a change.
   */
  async nextBatch(afterSeq: bigint): Promise<readonly StagedGroupEntry[]> {
    if (!this.stagedRead) {
      const rows = await selectStagedPushChangesAfter(this.conn, this.sourceNodeId, afterSeq, STAGED_PUSH_READ_ROWS)
      if (rows.length > 0) {
        return rows.map(row => ({
          seq: BigInt(String(row.source_seq)),
          change: decodeChange(this.sourceNodeId, row.change),
        }))
      }
      this.stagedRead = true
    }
    if (this.tailRead) return []
    this.tailRead = true
    return this.tail.map(entry => ({ seq: entry.seqs[0], change: entry.change }))
  }
}
