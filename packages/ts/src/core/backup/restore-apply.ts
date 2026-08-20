import { rm } from 'node:fs/promises'
import type { SQLiteConnection, SQLiteDriver } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import type { BackupChainChange } from './chain.js'
import { checkpointLog } from './checkpoint.js'
import type { BackupDestination } from './destination.js'
import { fetchStoredFile, listStoredFilePieces } from './restore-fetch.js'
import { type ExpectedLogHeader, RestoreLogWriter } from './restore-log.js'
import { LOG_FRAME_HEADER_BYTES, LOG_HEADER_BYTES } from './wal-format.js'

/** What one batch of change pieces replays into.
 * @internal
 */
export interface ChangeBatchRequest {
  /** Where the pieces are read from. */
  destination: BackupDestination
  /** Driver the rebuilt database opens through. */
  driver: SQLiteDriver
  /** Path of the rebuilt database. */
  destPath: string
  /** Size of one page of that database, in bytes. */
  pageSize: number
  /** Checkpoint sequence number to record in the log this batch writes. */
  logSequence: number
  /** The pieces to replay, oldest first. */
  batch: readonly BackupChainChange[]
  /** Called with the size of every piece the batch fetches. */
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
 * Refuses a chain whose change pieces leave a hole in the log. Two pieces of
 * one run of the log must meet frame by frame, and a piece opening a fresh run
 * must start at frame one. Anything else means frames that reached no backup,
 * and replaying across the hole would mix pages from after the gap with the
 * pages the missing frames should have replaced.
 *
 * @param changes - The pieces a restore plans to replay, oldest first.
 * @param chainId - The chain they belong to, which the error names.
 * @throws A `BACKUP_CHAIN_BROKEN` naming the piece the frames stop short of.
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
 * Opens the rebuilt database and reads how many pages it holds, which is the
 * cheapest proof that SQLite accepts the file.
 *
 * @param driver - Driver to open the database through.
 * @param destPath - Path of the rebuilt database.
 * @returns Pages the database holds.
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

/**
 * Opens the rebuilt database so SQLite replays the log beside it, checks that
 * it reached the size the last frame commits to, and folds the log back into
 * the file.
 *
 * The size check is what turns a log SQLite quietly ignored into an error. A
 * checkpoint reports success over a log it never read, and the count of pages
 * is the only evidence that SQLite replayed the frames.
 *
 * @param driver - Driver to open the database through.
 * @param destPath - Path of the rebuilt database.
 * @param databasePages - Pages the last frame of the batch commits to.
 */
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
 * Replays one batch of change pieces onto the rebuilt database.
 *
 * Sirannon fetches each change piece one stored piece at a time and writes
 * every one of them straight into the log, so it holds a single stored piece
 * rather than the batch. Once the whole batch is written, SQLite replays it and
 * a checkpoint folds it into the database file. The log is empty again before
 * the next batch begins.
 *
 * @param request - The pieces to replay, and where they go.
 * @returns How many frames the batch held.
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
