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
 * Checks that a value from a destination holds every field of a chain position
 * as a whole number, with frames counted from one and a last frame at or after
 * the first.
 *
 * @param value - The value to check.
 * @returns Whether the value has the shape of a chain position.
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
 * Checks that a value from a destination has the shape of one entry in the list
 * of chains.
 *
 * @param value - The value to check.
 * @returns Whether the value has the shape of a list entry.
 *
 * @internal
 */
export function isBackupChainHead(value: unknown): value is BackupChainHead {
  const head = value as BackupChainHead
  return typeof head?.chainId === 'string' && isWholeNumber(head.startedAt) && isTextOrAbsent(head.previousChainId)
}

/**
 * Checks that a value from a destination holds every field of the record for
 * the full copy at the head of a chain.
 *
 * @param value - The value to check.
 * @returns Whether the value has the shape of a full-copy record.
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
 * Checks that a value from a destination holds every field of the record for
 * one change piece.
 *
 * @param value - The value to check.
 * @returns Whether the value has the shape of a change-piece record.
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
