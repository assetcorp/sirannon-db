import { open, rm } from 'node:fs/promises'
import type { BackupDestination } from './destination.js'
import type { BackupRunReport } from './report.js'
import { fetchStoredFile, listStoredFilePieces, type StoredFile } from './restore-fetch.js'
import { writeFully } from './write-fully.js'

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

/**
 * Builds a local file from the pieces a destination holds, fetching one piece
 * at a time and writing it where its index places it. Where SQLite never wrote
 * a piece that stretch of the file stays zero, and every later byte goes to the
 * offset its own index sets.
 *
 * Sirannon checks the listing before it opens the local file, so that a
 * destination missing a piece is refused while the path named here is still
 * untouched. Once the file is open, Sirannon removes it after any failure,
 * because a database missing its middle would otherwise stay on disk as though
 * the assembly had finished.
 *
 * @param destination - Where the pieces are read from.
 * @param file - What the run that wrote those pieces recorded, which the assembly checks its result against.
 * @param destPath - Path the assembled file is written to.
 * @param onPiece - Called after each piece with the running counts.
 * @returns The bytes and pieces the assembly wrote, and the fingerprint it computed.
 *
 * @internal
 */
export async function assembleStoredFile(
  destination: BackupDestination,
  file: StoredFile,
  destPath: string,
  onPiece?: (piecesRead: number, bytesWritten: number) => void,
): Promise<AssembleResult> {
  const pieces = await listStoredFilePieces(destination, file)
  try {
    const handle = await open(destPath, 'w')
    let piecesRead = 0
    let bytesWritten = 0
    try {
      const fetched = await fetchStoredFile(destination, file, pieces, async (index, bytes) => {
        await writeFully(handle, destPath, bytes, bytes.byteLength, index * file.pieceBytes)
        piecesRead++
        bytesWritten += bytes.byteLength
        onPiece?.(piecesRead, bytesWritten)
      })
      return {
        bytesWritten: fetched.bytesFetched,
        pieceCount: fetched.pieceCount,
        ...(fetched.fingerprint === undefined ? {} : { fingerprint: fetched.fingerprint }),
      }
    } finally {
      await handle.close()
    }
  } catch (err) {
    await rm(destPath, { force: true }).catch(() => {})
    throw err
  }
}

/**
 * Builds a local file from the pieces a destination holds, checking the result
 * against what the run that wrote them reported.
 *
 * @param destination - Where the pieces are read from.
 * @param report - What the run that wrote those pieces recorded.
 * @param destPath - Path the assembled file is written to.
 * @returns The bytes and pieces the assembly wrote, and the fingerprint it computed.
 *
 * @public
 */
export function assembleFromDestination(
  destination: BackupDestination,
  report: BackupRunReport,
  destPath: string,
): Promise<AssembleResult> {
  return assembleStoredFile(destination, { ...report, name: report.destinationName }, destPath)
}
