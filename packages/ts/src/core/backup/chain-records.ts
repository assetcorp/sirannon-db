import type { BackupChainBase, BackupChainChange, BackupChainHead, BackupChainPosition } from './chain.js'

function isWholeNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0
}

function isAtLeastOne(value: unknown): value is number {
  return isWholeNumber(value) && value >= 1
}

function isTextOrAbsent(value: unknown): boolean {
  return value === undefined || typeof value === 'string'
}

/**
 * Checks that a value read back out of a destination holds every field of a
 * chain position, each of them a whole number, and frames that run forwards.
 *
 * @param value - The value to check.
 * @returns Whether it is a position Sirannon wrote.
 *
 * @internal
 */
export function isBackupChainPosition(value: unknown): value is BackupChainPosition {
  const position = value as BackupChainPosition
  return (
    isWholeNumber(position?.logSequence) &&
    isWholeNumber(position.salt1) &&
    isWholeNumber(position.salt2) &&
    isAtLeastOne(position.firstFrame) &&
    isAtLeastOne(position.lastFrame) &&
    position.lastFrame >= position.firstFrame
  )
}

/**
 * Checks that a value read back out of a destination is one line of the list of
 * chains.
 *
 * @param value - The value to check.
 * @returns Whether it is a line Sirannon wrote.
 *
 * @internal
 */
export function isBackupChainHead(value: unknown): value is BackupChainHead {
  const head = value as BackupChainHead
  return typeof head?.chainId === 'string' && isWholeNumber(head.startedAt) && isTextOrAbsent(head.previousChainId)
}

/**
 * Checks that a value read back out of a destination is a complete record of
 * the full copy at the head of a chain.
 *
 * @param value - The value to check.
 * @returns Whether it is a record Sirannon wrote.
 *
 * @internal
 */
export function isBackupChainBase(value: unknown): value is BackupChainBase {
  const base = value as BackupChainBase
  return (
    base?.kind === 'full' &&
    typeof base.chainId === 'string' &&
    typeof base.name === 'string' &&
    typeof base.runId === 'string' &&
    isWholeNumber(base.finishedAt) &&
    isWholeNumber(base.pieceCount) &&
    isAtLeastOne(base.pieceBytes) &&
    isWholeNumber(base.bytesWritten) &&
    isTextOrAbsent(base.fingerprint)
  )
}

/**
 * Checks that a value read back out of a destination is a complete record of
 * one change piece.
 *
 * @param value - The value to check.
 * @returns Whether it is a record Sirannon wrote.
 *
 * @internal
 */
export function isBackupChainChange(value: unknown): value is BackupChainChange {
  const change = value as BackupChainChange
  return (
    change?.kind === 'change' &&
    typeof change.chainId === 'string' &&
    typeof change.name === 'string' &&
    typeof change.runId === 'string' &&
    isAtLeastOne(change.sequence) &&
    isBackupChainPosition(change.position) &&
    isWholeNumber(change.capturedAt) &&
    isAtLeastOne(change.frameCount) &&
    isWholeNumber(change.pieceCount) &&
    isAtLeastOne(change.pieceBytes) &&
    isWholeNumber(change.bytesWritten) &&
    typeof change.checkpointed === 'boolean' &&
    isTextOrAbsent(change.fingerprint)
  )
}
