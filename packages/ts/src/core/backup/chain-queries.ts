import { SirannonError } from '../errors.js'
import type { BackupChain, BackupChainBase, BackupChainChange, BackupChainRecord } from './chain.js'

/** The records that one restore reads, and the moment that the restored database reflects.
 * @public
 */
export interface BackupRestorePlan {
  /** The chain that the restore reads from. */
  chainId: string
  /** The full copy to start from. */
  base: BackupChainBase
  /** The change pieces to apply on top of it, oldest first. */
  changes: BackupChainChange[]
  /**
   * The moment, in epoch milliseconds, that the restored database reflects.
   * It is the capture time of the last change piece in the plan, or the finish
   * time of the full copy where the plan holds no change piece, so it falls at
   * or before the moment that you ask for.
   */
  restoresTo: number
}

/** The earliest moment that a restore must still reach.
 * @public
 */
export interface BackupSafeToDeleteOptions {
  /**
   * The earliest moment, in epoch milliseconds, that a restore must still
   * reach. When you leave it out, the answer lists only the records that no
   * restore can use.
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
 * Returns what a restore to a given moment reads: the newest full copy that
 * finished at or before that moment, and every change piece of its chain that
 * Sirannon captured up to it.
 *
 * The plan also states the moment that the restored database reflects, which
 * is the capture time of its last piece. Each change piece holds every write
 * from one capture interval, so a restore stops at a piece boundary, which can
 * fall before the exact millisecond that you name.
 *
 * This throws `BACKUP_CHAIN_BROKEN` where no full copy finished at or before
 * the moment. It throws the same code, naming the missing piece, where the
 * chain has a gap before that moment, since a restore that stopped at the gap
 * would lose every write after it.
 *
 * @param chains - The chains at a destination, as {@link readBackupChains} returns them.
 * @param moment - The moment to restore to, in epoch milliseconds.
 * @returns The full copy, the change pieces to apply, and the moment that the restored database reflects.
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
 * Returns the backup records that no restore needs any longer, so that you can
 * delete them from the destination yourself.
 *
 * The list always includes the change pieces of a chain whose full copy is
 * gone, and every change piece after a gap in a chain, since a restore can
 * replay nothing past the gap. When you pass `restorableFrom`, the list also
 * includes every chain whose full copy finished before the full copy that a
 * restore to that moment would start from.
 *
 * @param chains - The chains at a destination, as {@link readBackupChains} returns them.
 * @param options - The earliest moment that a restore must still reach.
 * @returns The records that you can delete, oldest chain first.
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
