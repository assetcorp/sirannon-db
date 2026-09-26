import { rm } from 'node:fs/promises'
import type { SQLiteConnection, SQLiteDriver } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import type { BackupChainChange } from './chain.js'
import { checkpointLog } from './checkpoint.js'
import type { BackupDestination } from './destination.js'
import { fetchStoredFile, listStoredFilePieces } from './restore-fetch.js'
import { type ExpectedLogHeader, RestoreLogWriter } from './restore-log.js'
import { LOG_FRAME_HEADER_BYTES, LOG_HEADER_BYTES } from './wal-format.js'

/** The destination, rebuilt database, and change pieces for one batch of a restore.
 * @internal
 */
export interface ChangeBatchRequest {
  /** The destination that holds the pieces. */
  destination: BackupDestination
  /** The driver that Sirannon opens the rebuilt database through. */
  driver: SQLiteDriver
  /** The path of the rebuilt database. */
  destPath: string
  /** The size of one page of that database, in bytes. */
  pageSize: number
  /** The checkpoint sequence number to write into the log for this batch. */
  logSequence: number
  /** The change pieces to apply, oldest first. */
  batch: readonly BackupChainChange[]
  /** Called with the size of every piece that Sirannon fetches for the batch. */
  onPiece: (byteLength: number) => void
}

function chainError(message: string): SirannonError {
  return new SirannonError(message, 'BACKUP_CHAIN_BROKEN')
}

function headerBytesOf(change: BackupChainChange): number {
  return change.position.firstFrame === 1 ? LOG_HEADER_BYTES : 0
}

function expectedLogHeaderOf(change: BackupChainChange, pageSize: number): ExpectedLogHeader | undefined {
  if (change.position.firstFrame !== 1) return undefined
  return {
    pageSize,
    logSequence: change.position.logSequence,
    salt1: change.position.salt1,
    salt2: change.position.salt2,
  }
}

/**
 * Throws where the change pieces of a chain leave a gap in the log. The first
 * piece must start at frame one. Within one generation of the log, each later
 * piece must start at the frame after the last frame of the piece before it,
 * and a piece in a new generation must start at frame one. Any other start
 * means that no backup holds some frames, and applying pieces across the gap
 * would mix pages from after the gap with pages that the missing frames should
 * have replaced.
 *
 * @param changes - The change pieces that a restore plans to apply, oldest first.
 * @param chainId - The chain of those pieces, which the error message quotes.
 * @throws A `BACKUP_CHAIN_BROKEN` that names the first piece after the gap.
 *
 * @internal
 */
export function assertChangePiecesRunOn(changes: readonly BackupChainChange[], chainId: string): void {
  let previous: BackupChainChange | undefined
  for (const change of changes) {
    if (previous === undefined) {
      assertStartsAt(change, chainId, 1, 'the full copy underneath it')
      previous = change
      continue
    }
    const sameRun =
      previous.position.logSequence === change.position.logSequence &&
      previous.position.salt1 === change.position.salt1 &&
      previous.position.salt2 === change.position.salt2
    const expected = sameRun ? previous.position.lastFrame + 1 : 1
    const follows = `change piece ${previous.sequence}, which ends at frame ${previous.position.lastFrame}`
    assertStartsAt(change, chainId, expected, follows)
    previous = change
  }
}

function assertStartsAt(change: BackupChainChange, chainId: string, expected: number, follows: string): void {
  if (change.position.firstFrame === expected) return
  throw chainError(
    `Change piece ${change.sequence} of chain '${chainId}' starts at frame ${change.position.firstFrame} where it has to start at frame ${expected} to follow ${follows}, so the frames in between are in no backup`,
  )
}

function assertPieceFits(change: BackupChainChange, chainId: string, frameBytes: number): void {
  const expected = headerBytesOf(change) + change.frameCount * frameBytes
  if (change.bytesWritten !== expected) {
    throw chainError(
      `Change piece ${change.sequence} of chain '${chainId}' holds ${change.bytesWritten} bytes where ${change.frameCount} frames of a database with ${frameBytes - LOG_FRAME_HEADER_BYTES}-byte pages come to ${expected}, so it was taken from a different database`,
    )
  }
}

async function readPageCount(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('PRAGMA page_count')
  const row = await stmt.get<{ page_count: number | bigint }>()
  return row ? Number(row.page_count) : 0
}

/**
 * Opens the rebuilt database and reads its page count, which is the cheapest
 * check that SQLite accepts the file.
 *
 * @param driver - The driver to open the database through.
 * @param destPath - The path of the rebuilt database.
 * @returns The number of pages in the database.
 *
 * @internal
 */
export async function countDatabasePages(driver: SQLiteDriver, destPath: string): Promise<number> {
  const conn = await driver.open(destPath, { walMode: false, walAutoCheckpoint: 0 })
  try {
    return await readPageCount(conn)
  } finally {
    await conn.close()
  }
}

async function foldLogIntoDatabase(driver: SQLiteDriver, destPath: string, databasePages: number): Promise<void> {
  const conn = await driver.open(destPath, { walMode: false, walAutoCheckpoint: 0 })
  try {
    const reached = await readPageCount(conn)
    if (reached !== databasePages) {
      throw chainError(
        `Replaying the change pieces left '${destPath}' at ${reached} pages where the last frame of the batch commits ${databasePages}, so SQLite read none of those frames`,
      )
    }
    const checkpoint = await checkpointLog(conn)
    if (!checkpoint.emptied) {
      throw new SirannonError(
        `The checkpoint after a batch left ${checkpoint.framesInLog} frames in the log beside '${destPath}', so the restore stopped rather than let the log grow`,
        'BACKUP_ERROR',
      )
    }
  } finally {
    await conn.close()
  }
}

/**
 * Applies one batch of change pieces to the rebuilt database.
 *
 * Sirannon fetches each change piece one stored piece at a time and writes each
 * stored piece straight into the log, so it holds one stored piece in memory at
 * a time. Once the whole batch is in the log, SQLite reads the log when
 * Sirannon opens the database, and a checkpoint then copies the frames into the
 * database file and empties the log before the next batch begins.
 *
 * @param request - The change pieces to apply, and the database to apply them to.
 * @returns The number of log frames in the batch.
 *
 * @internal
 */
export async function applyChangeBatch(request: ChangeBatchRequest): Promise<number> {
  const logPath = `${request.destPath}-wal`
  await rm(`${request.destPath}-shm`, { force: true })
  const writer = await RestoreLogWriter.create(logPath, request.pageSize, request.logSequence)

  try {
    for (const change of request.batch) {
      assertPieceFits(change, change.chainId, writer.frameBytes)
      const pieces = await listStoredFilePieces(request.destination, change)
      writer.beginPiece(headerBytesOf(change), expectedLogHeaderOf(change, request.pageSize))
      await fetchStoredFile(request.destination, change, pieces, async (_index, bytes) => {
        await writer.add(bytes)
        request.onPiece(bytes.byteLength)
      })
    }
    const written = await writer.finish()
    await foldLogIntoDatabase(request.driver, request.destPath, written.databasePages)
    return written.frameCount
  } catch (err) {
    await writer.abandon()
    await rm(logPath, { force: true }).catch(() => {})
    throw err
  }
}
