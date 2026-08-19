import { createHash } from 'node:crypto'
import { open } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import type { BackupDestination } from './destination.js'

/** What one file sent in pieces left at the destination.
 * @internal
 */
export interface SentPieces {
  /** How many pieces the transfer came to. */
  pieceCount: number
  /** How many bytes it stored. */
  bytesWritten: number
  /** SHA-256 of what it sent, where the caller asked for one. */
  fingerprint?: string
}

function destinationError(name: string, index: number, err: unknown): SirannonError {
  if (err instanceof SirannonError) return err
  return new SirannonError(
    `The destination refused piece ${index} of '${name}': ${err instanceof Error ? err.message : String(err)}`,
    'BACKUP_DESTINATION_ERROR',
  )
}

/**
 * Sends a local file to a destination in fixed-size pieces. Every piece brings
 * a report, so a caller watching a long transfer can see it move.
 *
 * @param sourcePath - File to read.
 * @param destination - Where the pieces go.
 * @param name - Name to store them under.
 * @param pieceBytes - Size of one whole piece, in bytes.
 * @param fingerprint - Whether to fingerprint what goes out.
 * @param report - Called after each piece with the running counts.
 * @returns What reached the destination, and the fingerprint where one was asked for.
 */
export async function sendFileInPieces(
  sourcePath: string,
  destination: BackupDestination,
  name: string,
  pieceBytes: number,
  fingerprint: boolean,
  report: (piecesWritten: number, bytesWritten: number) => void,
): Promise<SentPieces> {
  const file = await open(sourcePath, 'r')
  const digest = fingerprint ? createHash('sha256') : null
  let index = 0
  let bytesWritten = 0
  try {
    const buffer = Buffer.allocUnsafe(pieceBytes)
    for (;;) {
      let filled = 0
      while (filled < pieceBytes) {
        const { bytesRead } = await file.read(buffer, filled, pieceBytes - filled, index * pieceBytes + filled)
        if (bytesRead === 0) break
        filled += bytesRead
      }
      if (filled === 0) break
      const piece = new Uint8Array(filled)
      piece.set(buffer.subarray(0, filled))
      digest?.update(piece)
      try {
        await destination.writePiece(name, index, piece)
      } catch (err) {
        throw destinationError(name, index, err)
      }
      index++
      bytesWritten += filled
      report(index, bytesWritten)
      if (filled < pieceBytes) break
    }
  } finally {
    await file.close()
  }
  return { pieceCount: index, bytesWritten, ...(digest ? { fingerprint: digest.digest('hex') } : {}) }
}
