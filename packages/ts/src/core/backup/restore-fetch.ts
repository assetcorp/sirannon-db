import { createHash } from 'node:crypto'
import { SirannonError } from '../errors.js'
import type { BackupDestination, BackupPiece } from './destination.js'

/** The record of one file that a backup stores at a destination.
 * @internal
 */
export interface StoredFile {
  /** The name that Sirannon stores the pieces under. */
  name: string
  /** The number of stored pieces. */
  pieceCount: number
  /** The size of one whole piece, in bytes. */
  pieceBytes: number
  /** The number of stored bytes. */
  bytesWritten: number
  /** The SHA-256 of the file that those pieces assemble into, where the record holds one. */
  fingerprint?: string
}

/** The bytes and pieces that one fetch reads from a destination.
 * @internal
 */
export interface FetchedFile {
  /** The number of bytes that Sirannon reads. */
  bytesFetched: number
  /** The number of pieces that Sirannon reads. */
  pieceCount: number
  /** The SHA-256 of the bytes that Sirannon reads, present where the record holds a fingerprint to check against. */
  fingerprint?: string
}

function destinationError(message: string, err?: unknown): SirannonError {
  if (err instanceof SirannonError) return err
  const detail = err instanceof Error ? `: ${err.message}` : ''
  return new SirannonError(`${message}${detail}`, 'BACKUP_DESTINATION_ERROR')
}

function assertNoPieceIsMissing(pieces: readonly BackupPiece[], file: StoredFile): void {
  if (pieces.length === 0) {
    throw destinationError(`The destination holds no pieces named '${file.name}'`)
  }
  for (let expected = 0; expected < file.pieceCount; expected++) {
    if (pieces[expected]?.index !== expected) {
      throw destinationError(
        `The destination is missing piece ${expected} of '${file.name}', so the file cannot be assembled`,
      )
    }
  }
  if (pieces.length > file.pieceCount) {
    throw destinationError(
      `The destination holds ${pieces.length} pieces of '${file.name}' where the run wrote ${file.pieceCount}, so a later piece belongs to a different run`,
    )
  }
}

/**
 * Lists the pieces of one stored file at a destination, and throws a
 * `BACKUP_DESTINATION_ERROR` where a piece is missing or where the destination
 * holds more pieces than the record states.
 *
 * Callers list the pieces before they open a local file, so that a missing
 * piece fails the restore before the local file changes.
 *
 * @param destination - The destination that holds the pieces.
 * @param file - The record of the backup that stored the pieces.
 * @returns The pieces, in index order.
 *
 * @internal
 */
export async function listStoredFilePieces(destination: BackupDestination, file: StoredFile): Promise<BackupPiece[]> {
  let listed: BackupPiece[]
  try {
    listed = [...(await destination.listPieces(file.name))]
  } catch (err) {
    throw destinationError(`The destination could not list the pieces of '${file.name}'`, err)
  }
  listed.sort((left, right) => left.index - right.index)
  assertNoPieceIsMissing(listed, file)
  return listed
}

/**
 * Reads one stored file from a destination a piece at a time, and passes each
 * piece to `take` as soon as Sirannon reads it. Sirannon holds one piece in
 * memory at a time, so a restore of a large database needs little memory.
 *
 * After the last piece, Sirannon checks the byte count and the fingerprint that
 * it computes against the record of the backup, since that record holds one
 * fingerprint for the whole file.
 *
 * @param destination - The destination that holds the pieces.
 * @param file - The record of the backup that stored the pieces.
 * @param pieces - The pieces that {@link listStoredFilePieces} returns, in index order.
 * @param take - Called with each piece in that order.
 * @returns The bytes and pieces that Sirannon reads, and the fingerprint that it computes.
 *
 * @internal
 */
export async function fetchStoredFile(
  destination: BackupDestination,
  file: StoredFile,
  pieces: readonly BackupPiece[],
  take: (index: number, bytes: Uint8Array) => Promise<void>,
): Promise<FetchedFile> {
  const digest = file.fingerprint === undefined ? null : createHash('sha256')
  let bytesFetched = 0
  for (const piece of pieces) {
    let bytes: Uint8Array
    try {
      bytes = await destination.readPiece(file.name, piece.index)
    } catch (err) {
      throw destinationError(`The destination could not return piece ${piece.index} of '${file.name}'`, err)
    }
    digest?.update(bytes)
    bytesFetched += bytes.byteLength
    await take(piece.index, bytes)
  }

  const fingerprint = digest?.digest('hex')
  if (bytesFetched !== file.bytesWritten) {
    throw destinationError(
      `The pieces of '${file.name}' hold ${bytesFetched} bytes where the run wrote ${file.bytesWritten}`,
    )
  }
  if (fingerprint !== undefined && fingerprint !== file.fingerprint) {
    throw destinationError(`The pieces of '${file.name}' do not match the fingerprint the run recorded`)
  }
  return { bytesFetched, pieceCount: pieces.length, ...(fingerprint === undefined ? {} : { fingerprint }) }
}
