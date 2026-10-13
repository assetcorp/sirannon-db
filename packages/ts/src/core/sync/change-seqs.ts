import { BatchValidationError } from './errors.js'
import type { ReplicationBatch } from './types.js'

export const MAX_CHANGE_SEQ = 9_223_372_036_854_775_807n

function assertSeqInRange(seq: bigint, label: string): void {
  if (seq < 0n || seq > MAX_CHANGE_SEQ) {
    throw new BatchValidationError(`${label} must be a sequence from 0 to ${MAX_CHANGE_SEQ}`)
  }
}

function explicitSeqs(batch: ReplicationBatch, changeSeqs: readonly bigint[]): bigint[][] {
  if (changeSeqs.length !== batch.changes.length) {
    throw new BatchValidationError('changeSeqs must hold one sequence for each change')
  }
  const seqs: bigint[][] = []
  let previous: bigint | null = null
  for (const [index, seq] of changeSeqs.entries()) {
    assertSeqInRange(seq, `changeSeqs[${index}]`)
    if (previous !== null && seq <= previous) {
      throw new BatchValidationError('changeSeqs must rise strictly')
    }
    previous = seq
    seqs.push([seq])
  }
  if (changeSeqs[0] !== batch.fromSeq || previous !== batch.toSeq) {
    throw new BatchValidationError('changeSeqs must start at fromSeq and end at toSeq')
  }
  return seqs
}

/**
 * Returns the source change-log sequences of each change in a batch, which the
 * applier checks against the ledger of applied changes and records once it
 * applies the change. A batch that carries `changeSeqs` names them. Without
 * `changeSeqs`, a range that holds one sequence per change numbers the changes
 * by position, and any other range gives the first change `fromSeq` and the
 * last change `toSeq`, because the batch fixes no other position.
 *
 * @param batch - The batch whose changes the caller numbers.
 * @returns One list of sequences per change, in the order of `batch.changes`.
 * @throws {@link BatchValidationError} when a sequence falls outside the range SQLite stores, or `changeSeqs` fails to match the batch.
 * @internal
 */
export function attributeChangeSeqs(batch: ReplicationBatch): bigint[][] {
  assertSeqInRange(batch.fromSeq, 'fromSeq')
  assertSeqInRange(batch.toSeq, 'toSeq')
  if (batch.fromSeq > batch.toSeq) {
    throw new BatchValidationError('fromSeq must not exceed toSeq')
  }
  if (batch.changeSeqs !== undefined) {
    return explicitSeqs(batch, batch.changeSeqs)
  }
  if (batch.txContinues === true) {
    throw new BatchValidationError('txContinues requires changeSeqs')
  }

  const count = batch.changes.length
  if (batch.toSeq - batch.fromSeq + 1n === BigInt(count)) {
    return batch.changes.map((_, index) => [batch.fromSeq + BigInt(index)])
  }
  return batch.changes.map((_, index) => {
    const seqs: bigint[] = []
    if (index === 0) seqs.push(batch.fromSeq)
    if (index === count - 1 && (index !== 0 || batch.toSeq !== batch.fromSeq)) seqs.push(batch.toSeq)
    return seqs
  })
}
