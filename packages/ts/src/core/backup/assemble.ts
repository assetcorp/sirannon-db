import { open, rm } from 'node:fs/promises'
import type { BackupDestination } from './destination.js'
import type { BackupRunReport } from './report.js'
import { fetchStoredFile, listStoredFilePieces, type StoredFile } from './restore-fetch.js'
import { writeFully } from './write-fully.js'

/** The byte and piece counts for one file that Sirannon assembles from a destination.
 * @public
 */
export interface AssembleResult {
  /** The number of bytes that Sirannon writes to the local file. */
  bytesWritten: number
  /** The number of pieces that Sirannon reads from the destination. */
  pieceCount: number
  /** The SHA-256 of the assembled file, present only where the backup report holds a fingerprint to check it against. */
  fingerprint?: string
}

/**
 * Rebuilds a local file from the pieces at a destination, fetching one piece at
 * a time and writing each one at the offset that its index sets.
 *
 * Sirannon lists the pieces before it opens the local file, so that it can
 * refuse a destination with a missing piece while the file at `destPath` stays
 * as it was. Once the file is open, Sirannon deletes it after any failure, so
 * that a partly written database never stays on disk.
 *
 * @param destination - The destination that holds the pieces.
 * @param file - The record of the backup that stored those pieces, which Sirannon checks the result against.
 * @param destPath - The path that Sirannon writes the assembled file to.
 * @param onPiece - Called after each piece with the running counts.
 * @returns The bytes and pieces that Sirannon writes, and the fingerprint that it computes.
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
 * Rebuilds a local file from the pieces at a destination, and checks the result
 * against the report of the backup that stored them.
 *
 * @param destination - The destination that holds the pieces.
 * @param report - The report of the backup that stored those pieces.
 * @param destPath - The path that Sirannon writes the assembled file to.
 * @returns The bytes and pieces that Sirannon writes, and the fingerprint that it computes.
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
