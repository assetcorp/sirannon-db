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
import type { ReplicationChange } from './types.js'

const STAGED_PUSH_READ_ROWS = 500

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

function decodeChange(stored: string): ReplicationChange {
  const raw = JSON.parse(stored) as ReplicationChange
  return {
    ...raw,
    primaryKey: decodeTaggedValues(raw.primaryKey) as Record<string, unknown>,
    newData: raw.newData === null ? null : (decodeTaggedValues(raw.newData) as Record<string, unknown>),
    oldData: raw.oldData === null ? null : (decodeTaggedValues(raw.oldData) as Record<string, unknown>),
  }
}

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

export async function completeStagedPush(
  tx: SQLiteConnection,
  sourceNodeId: string,
  tailSeqs: readonly bigint[],
): Promise<void> {
  await insertAppliedChangesFromStagedPush(tx, sourceNodeId, Date.now() / 1000)
  await recordAppliedSeqs(tx, sourceNodeId, tailSeqs)
  await deleteStagedPushChanges(tx, sourceNodeId)
}

export class StagedPushSource implements StagedGroupSource {
  private stagedRead = false
  private tailRead = false

  constructor(
    private readonly conn: SQLiteConnection,
    private readonly sourceNodeId: string,
    private readonly tail: readonly PushEntry[],
  ) {}

  async nextBatch(afterSeq: bigint): Promise<readonly StagedGroupEntry[]> {
    if (!this.stagedRead) {
      const rows = await selectStagedPushChangesAfter(this.conn, this.sourceNodeId, afterSeq, STAGED_PUSH_READ_ROWS)
      if (rows.length > 0) {
        return rows.map(row => ({ seq: BigInt(String(row.source_seq)), change: decodeChange(row.change) }))
      }
      this.stagedRead = true
    }
    if (this.tailRead) return []
    this.tailRead = true
    return this.tail.map(entry => ({ seq: entry.seqs[0], change: entry.change }))
  }
}
