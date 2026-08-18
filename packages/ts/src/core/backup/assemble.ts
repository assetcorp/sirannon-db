import { createHash } from 'node:crypto'
import { open } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import type { BackupDestination, BackupPiece } from './destination.js'
import type { BackupRunReport } from './report.js'

/** What one assembled file took to build.
 * @public
 */
export interface AssembleResult {
  /** Bytes the assembly wrote. */
  bytesWritten: number
  /** Pieces the assembly read. */
  pieceCount: number
  /** SHA-256 of the assembled file, where the run recorded one to check it against. */
  fingerprint?: string
}

function destinationError(message: string): SirannonError {
  return new SirannonError(message, 'BACKUP_DESTINATION_ERROR')
}

function assertChainIsWhole(pieces: BackupPiece[], report: BackupRunReport): void {
  if (pieces.length === 0) {
    throw destinationError(`The destination holds no pieces named '${report.destinationName}'`)
  }
  for (let expected = 0; expected < report.pieceCount; expected++) {
    if (pieces[expected]?.index !== expected) {
      throw destinationError(
        `The destination is missing piece ${expected} of '${report.destinationName}', so the file cannot be assembled`,
      )
    }
  }
  if (pieces.length > report.pieceCount) {
    throw destinationError(
      `The destination holds ${pieces.length} pieces of '${report.destinationName}' where the run wrote ${report.pieceCount}, so a later piece belongs to a different run`,
    )
  }
}

/**
 * Builds a local file from the pieces a destination holds, fetching one piece
 * at a time and writing it where its index places it, so a piece SQLite never
 * wrote leaves zeros rather than moving every later byte.
 *
 * @param destination - Where the pieces are read from.
 * @param report - What the run that wrote those pieces recorded, which the assembly checks its result against.
 * @param destPath - Path the assembled file is written to.
 * @returns The bytes and pieces the assembly wrote, and the fingerprint it computed.
 *
 * @public
 */
export async function assembleFromDestination(
  destination: BackupDestination,
  report: BackupRunReport,
  destPath: string,
): Promise<AssembleResult> {
  const name = report.destinationName
  const pieces = [...(await destination.listPieces(name))].sort((a, b) => a.index - b.index)
  assertChainIsWhole(pieces, report)

  const digest = report.fingerprint === undefined ? null : createHash('sha256')
  const file = await open(destPath, 'w')
  let bytesWritten = 0
  try {
    for (const piece of pieces) {
      const bytes = await destination.readPiece(name, piece.index)
      await file.write(bytes, 0, bytes.byteLength, piece.index * report.pieceBytes)
      digest?.update(bytes)
      bytesWritten += bytes.byteLength
    }
  } finally {
    await file.close()
  }

  if (bytesWritten !== report.bytesWritten) {
    throw destinationError(
      `The pieces of '${name}' hold ${bytesWritten} bytes where the run wrote ${report.bytesWritten}`,
    )
  }
  const fingerprint = digest?.digest('hex')
  if (fingerprint !== undefined && fingerprint !== report.fingerprint) {
    throw destinationError(`The pieces of '${name}' do not match the fingerprint the run recorded`)
  }
  return { bytesWritten, pieceCount: pieces.length, ...(fingerprint ? { fingerprint } : {}) }
}
