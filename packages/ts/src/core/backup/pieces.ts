import { createHash } from 'node:crypto'
import { open } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import type { BackupDestination } from './destination.js'

/** The pieces and bytes that Sirannon stores at a destination for one file.
 * @internal
 */
export interface SentPieces {
  /** The number of pieces that Sirannon stores. */
  pieceCount: number
  /** The number of bytes that Sirannon stores. */
  bytesWritten: number
  /** The SHA-256 of the bytes that Sirannon sends, present where the caller asks for one. */
  fingerprint?: string
}

/**
 * Wraps an error from the destination of a caller in a
 * `BACKUP_DESTINATION_ERROR`, and passes a `SirannonError` through unchanged.
 * The message names the piece that failed and its file.
 *
 * @param name - The name of the file that the piece is part of.
 * @param index - The position of the piece in the file.
 * @param err - The error that the destination throws.
 * @returns The error to report for that piece.
 */
export function destinationPieceError(name: string, index: number, err: unknown): SirannonError {
  if (err instanceof SirannonError) return err
  return new SirannonError(
    `The destination refused piece ${index} of '${name}': ${err instanceof Error ? err.message : String(err)}`,
    'BACKUP_DESTINATION_ERROR',
  )
}

/**
 * Reads back every stored piece of a file and returns the SHA-256 of those
 * pieces in index order. A streamed copy never holds the whole file, so
 * Sirannon can compute its fingerprint only this way.
 *
 * @param destination - The destination that holds the pieces.
 * @param name - The name that Sirannon stores the pieces under.
 * @param pieceCount - The number of stored pieces.
 * @returns The SHA-256 of the file that those pieces assemble into.
 */
export async function fingerprintStoredPieces(
  destination: BackupDestination,
  name: string,
  pieceCount: number,
): Promise<string> {
  const digest = createHash('sha256')
  for (let index = 0; index < pieceCount; index++) {
    try {
      digest.update(await destination.readPiece(name, index))
    } catch (err) {
      throw destinationPieceError(name, index, err)
    }
  }
  return digest.digest('hex')
}

/**
 * Sends a local file to a destination in fixed-size pieces, and calls `report`
 * after each piece so that a caller can follow a long transfer.
 *
 * @param sourcePath - The file to send.
 * @param destination - The destination that stores the pieces.
 * @param name - The name to store the pieces under.
 * @param pieceBytes - The size of one whole piece, in bytes.
 * @param fingerprint - Whether to compute the SHA-256 of the bytes that Sirannon sends.
 * @param report - Called after each piece with the running counts.
 * @returns The pieces and bytes that Sirannon stores, and the fingerprint where the caller asks for one.
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
        throw destinationPieceError(name, index, err)
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
