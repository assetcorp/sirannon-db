import { SirannonError } from '../errors.js'
import type { BackupChain, BackupChainBase, BackupChainChange, BackupChainRecord } from './chain.js'

/** What one restore has to read, and what it will produce.
 * @public
 */
export interface BackupRestorePlan {
  /** The chain to read. */
  chainId: string
  /** The full copy to start from. */
  base: BackupChainBase
  /** The change pieces to apply on top of it, oldest first. */
  changes: BackupChainChange[]
  /**
   * Epoch milliseconds the restored database will reflect. This is when the
   * last piece in the plan was captured, which is at or before the moment you
   * asked for.
   */
  restoresTo: number
}

/** How far back you still want to be able to restore.
 * @public
 */
export interface BackupSafeToDeleteOptions {
  /**
   * Epoch milliseconds of the earliest moment a restore must still reach.
   * Leave it out and the answer covers only the backups no restore could ever
   * use.
   */
  restorableFrom?: number
}

function chainError(message: string): SirannonError {
  return new SirannonError(message, 'BACKUP_CHAIN_BROKEN')
}

function firstMissingSequence(changes: readonly BackupChainChange[]): number | undefined {
  for (let expected = 1; expected <= changes.length; expected++) {
    if (changes[expected - 1]?.sequence !== expected) return expected
  }
  return undefined
}

function withBase(chains: readonly BackupChain[]): (BackupChain & { base: BackupChainBase })[] {
  return chains.filter((chain): chain is BackupChain & { base: BackupChainBase } => chain.base !== undefined)
}

/**
 * Works out what a restore to a given moment has to read: the newest full copy
 * finished at or before it, then every change piece captured from that copy up
 * to it.
 *
 * You get back the moment the result will actually reflect, which is when the
 * last piece was captured. One piece covers every write in the interval it was
 * taken over, so a restore arrives at a piece boundary, not at the exact
 * millisecond you named.
 *
 * A gap in the chain fails the plan with `BACKUP_CHAIN_BROKEN` and names the
 * missing piece, because a plan that stopped part-way through would leave a
 * database nobody could trust.
 *
 * @param chains - The chains a destination holds, as {@link readBackupChains} returns them.
 * @param moment - Epoch milliseconds you want back.
 * @returns The full copy, the change pieces to apply, and the moment the result reflects.
 *
 * @public
 */
export function planBackupRestore(chains: readonly BackupChain[], moment: number): BackupRestorePlan {
  const candidates = withBase(chains)
    .filter(chain => chain.base.finishedAt <= moment)
    .sort((left, right) => right.base.finishedAt - left.base.finishedAt)

  const chain = candidates[0]
  if (!chain) {
    const earliest = withBase(chains)
      .map(candidate => candidate.base.finishedAt)
      .sort((left, right) => left - right)[0]
    throw chainError(
      earliest === undefined
        ? 'The destination holds no full copy, so no moment can be restored'
        : `The destination holds no full copy finished at or before ${new Date(moment).toISOString()}; the earliest moment it can restore is ${new Date(earliest).toISOString()}`,
    )
  }

  const changes = chain.changes.filter(change => change.capturedAt <= moment)
  const missing = firstMissingSequence(changes)
  if (missing !== undefined) {
    throw chainError(
      `Chain '${chain.chainId}' is missing change piece ${missing}, so it cannot be applied past piece ${missing - 1}`,
    )
  }

  const last = changes[changes.length - 1]
  return {
    chainId: chain.chainId,
    base: chain.base,
    changes,
    restoresTo: last ? last.capturedAt : chain.base.finishedAt,
  }
}

/**
 * Tells you which backups no restore still needs, so you can delete them
 * knowing exactly what you give up.
 *
 * Two kinds are dead whatever you ask for: a chain whose full copy has gone,
 * and every change piece after a gap, since nothing can be replayed past the
 * gap. Name the earliest moment you still want to reach and the answer also
 * covers the older chains a newer full copy already spans.
 *
 * Sirannon lists them and deletes nothing. The destination is yours.
 *
 * @param chains - The chains a destination holds, as {@link readBackupChains} returns them.
 * @param options - How far back you still want to be able to restore.
 * @returns The records you may delete, oldest first.
 *
 * @public
 */
export function backupPiecesSafeToDelete(
  chains: readonly BackupChain[],
  options?: BackupSafeToDeleteOptions,
): BackupChainRecord[] {
  const restorableFrom = options?.restorableFrom
  const superseded =
    restorableFrom === undefined
      ? undefined
      : withBase(chains)
          .filter(chain => chain.base.finishedAt <= restorableFrom)
          .sort((left, right) => right.base.finishedAt - left.base.finishedAt)[0]

  const oldestFirst = [...chains].sort((left, right) => left.startedAt - right.startedAt)
  const deletable: BackupChainRecord[] = []

  for (const chain of oldestFirst) {
    if (!chain.base) {
      deletable.push(...chain.changes)
      continue
    }
    if (superseded && chain.base.finishedAt < superseded.base.finishedAt) {
      deletable.push(chain.base, ...chain.changes)
      continue
    }
    const missing = firstMissingSequence(chain.changes)
    if (missing !== undefined) {
      deletable.push(...chain.changes.filter(change => change.sequence > missing))
    }
  }

  return deletable
}
