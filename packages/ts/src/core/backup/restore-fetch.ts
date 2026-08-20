import { createHash } from 'node:crypto'
import { SirannonError } from '../errors.js'
import type { BackupDestination, BackupPiece } from './destination.js'

/** One file a backup run stored at a destination, as that run recorded it.
 * @internal
 */
export interface StoredFile {
  /** Name the pieces are stored under. */
  name: string
  /** How many pieces the run stored. */
  pieceCount: number
  /** Size of one whole piece, in bytes. */
  pieceBytes: number
  /** How many bytes the run stored. */
  bytesWritten: number
  /** SHA-256 of the file those pieces assemble into, where the run recorded one. */
  fingerprint?: string
}

/** What one fetch read back out of a destination.
 * @internal
 */
export interface FetchedFile {
  /** Bytes the fetch read. */
  bytesFetched: number
  /** Pieces it read. */
  pieceCount: number
  /** SHA-256 of what it read, where the record it checked against states one. */
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
 * Asks a destination which pieces of one stored file it holds, and refuses the
 * file where a piece is missing or where a longer run left an extra one behind.
 *
 * A caller lists before it opens a file of its own, so that a destination
 * missing a piece is refused while the caller's path is still untouched.
 *
 * @param destination - Where the pieces are stored.
 * @param file - What the run that stored them recorded.
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
 * Reads one stored file back out of a destination, a piece at a time, and hands
 * each piece straight on. Sirannon holds nothing but the piece in hand, which
 * is what keeps a restore of a large database inside a small amount of memory.
 *
 * Sirannon then checks the bytes it read and the fingerprint it computed
 * against the record the backup left behind. A backup records one fingerprint
 * for the whole file, so Sirannon runs that check once it has read the last
 * piece.
 *
 * @param destination - Where the pieces are read from.
 * @param file - What the run that stored them recorded.
 * @param pieces - The pieces {@link listStoredFilePieces} found, in index order.
 * @param take - Called with each piece in that order.
 * @returns What the fetch read, and the fingerprint it computed.
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
