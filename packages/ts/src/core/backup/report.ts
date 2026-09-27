import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import type { BackupChainPosition } from './chain.js'
import type { BackupDestination } from './destination.js'

export const DEFAULT_PIECE_BYTES = 16 * 1024 * 1024

/**
 * Returns the name to store the pieces of a backup under when the caller names none.
 *
 * @returns A name that includes the current time as an ISO timestamp.
 *
 * @internal
 */
export function defaultDestinationName(): string {
  return `backup-${new Date().toISOString().replace(/[:.]/g, '-')}.db`
}

/**
 * Throws a `BACKUP_ERROR` for a piece size that is not a positive whole number.
 *
 * @param pieceBytes - The size of one whole piece, in bytes.
 *
 * @internal
 */
export function assertPieceBytes(pieceBytes: number): void {
  if (!Number.isInteger(pieceBytes) || pieceBytes <= 0) {
    throw new SirannonError(
      `Piece size must be a positive whole number of bytes, and it was ${pieceBytes}`,
      'BACKUP_ERROR',
    )
  }
}

/**
 * Reads the page size of the database that a connection has open, which a
 * report states next to the number of pages copied.
 *
 * @param conn - The connection to read from.
 * @returns The size of one page, in bytes, or zero where SQLite returns no row.
 *
 * @internal
 */
export async function readPageSize(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('PRAGMA page_size')
  const row = await stmt.get<{ page_size: number | bigint }>()
  return row ? Number(row.page_size) : 0
}

/**
 * Reads the path of the main database file that a connection has open, which
 * a report names as the source of its copy.
 *
 * @param conn - The connection to read from.
 * @returns The path of that file, or an empty string where SQLite reports none.
 *
 * @internal
 */
export async function readMainDatabasePath(conn: SQLiteConnection): Promise<string> {
  const stmt = await conn.prepare('PRAGMA database_list')
  const rows = await stmt.all<{ name: string; file: string | null }>()
  const main = rows.find(row => row.name === 'main')
  return main?.file ?? ''
}

/**
 * The position of the write-ahead log of a database at one moment.
 *
 * A full copy records this position, so that you can tell which generation of
 * the log the database was on when the copy finished. SQLite restarts the log
 * after a checkpoint that empties it, and it writes a new pair of salts into
 * the log at each restart.
 *
 * The backup cycle runs a checkpoint after each capture and none between a full
 * copy and the first change piece of its chain, so those two records hold the
 * same salts. A change piece holds later salts than the piece before it
 * whenever the checkpoint between the two empties the log. A reader can keep a
 * checkpoint from emptying the log, which leaves the log on its current
 * generation, so two consecutive change pieces can hold the same salts.
 *
 * @public
 */
export interface BackupLogPosition {
  /** The checkpoint sequence of the log. SQLite adds one to it each time it restarts the log. */
  logSequence: number
  /** The first salt of that log. Together with `salt2`, it identifies the generation of the log. */
  salt1: number
  /** The second salt of that log. */
  salt2: number
  /** The last frame that commits a transaction, counted from one, or zero where the log holds no such frame. */
  lastFrame: number
}

/** The progress of one backup, which Sirannon reports after each step of the copy and after each piece that it stores.
 * @public
 */
export interface BackupProgress {
  /** The identifier of the backup that this progress describes. */
  runId: string
  /** Whether Sirannon is copying pages or sending pieces. */
  phase: 'copy' | 'transfer'
  /** The total number of pages to copy. For a change piece, this is the number of log frames in the piece. */
  totalPages: number
  /**
   * The number of pages left to copy. On the staged route, every report of the
   * transfer phase states zero, since the copy finishes before the transfer
   * starts. On the streamed route, the two phases overlap, so a transfer report
   * states the pages left at the latest step.
   */
  remainingPages: number
  /** The number of times so far that SQLite restarts the copy from page one. A change piece reports zero, since Sirannon reads the log for it and copies no pages. */
  restarts: number
  /** The number of pieces stored at the destination so far. */
  piecesWritten: number
  /** The number of bytes stored at the destination so far. */
  bytesWritten: number
}

/** The report of one finished backup to a destination.
 * @public
 */
export interface BackupRunReport {
  /** The identifier that Sirannon reports the progress of this backup under. */
  runId: string
  /** The identifier of the source database. */
  databaseId: string
  /** The path of the source database file. */
  sourcePath: string
  /** Whether the backup holds the whole database or the log frames written since the previous capture. */
  kind: 'full' | 'change'
  /** The chain that this backup is part of. A full copy begins a chain, and every later change piece extends it. */
  chainId: string
  /** Whether Sirannon sends the bytes through a local file, which is `staged`, or without one, which is `streamed`. */
  route: 'staged' | 'streamed'
  /**
   * The name that Sirannon stores the pieces under at the destination. Sirannon
   * writes only this name at the destination, because the journal that SQLite
   * opens next to a copy stays on local disk.
   */
  destinationName: string
  /** The moment, in epoch milliseconds, that the backup started. */
  startedAt: number
  /** The moment, in epoch milliseconds, that the backup finished. */
  finishedAt: number
  /** The length of the whole backup, in milliseconds. */
  durationMs: number
  /** The time, in milliseconds, that SQLite spends copying pages. */
  copyMs: number
  /** The time, in milliseconds, that Sirannon spends storing pieces at the destination. */
  transferMs: number
  /** The number of pages that SQLite copies. */
  pageCount: number
  /** The size of one page, in bytes. */
  pageSize: number
  /** The number of bytes that Sirannon stores at the destination. */
  bytesWritten: number
  /** The number of pieces that Sirannon stores at the destination. */
  pieceCount: number
  /** The size of one whole piece, in bytes. */
  pieceBytes: number
  /** The number of times that SQLite restarts the copy from page one. */
  restarts: number
  /** The range of log frames that a change piece holds. A full copy leaves this field out. */
  position?: BackupChainPosition
  /**
   * The position of the write-ahead log when a full copy finishes. Sirannon
   * reads the log after SQLite copies the last page, so if a writer commits
   * between those two steps, this position can include a frame that the copy
   * lacks. A change piece states its range of log frames in
   * {@link BackupRunReport.position}, and a database without a write-ahead log
   * leaves out both fields.
   */
  logPosition?: BackupLogPosition
  /** The SHA-256 of the copied file, present while fingerprinting is on. */
  fingerprint?: string
}

/** The report of one finished copy to a local file.
 * @public
 */
export interface BackupFileReport {
  /** The identifier of this copy. */
  runId: string
  /** The identifier of the source database. */
  databaseId: string
  /** The path of the source database file. */
  sourcePath: string
  /** The absolute path of the copy. */
  destPath: string
  /** The moment, in epoch milliseconds, that the copy started. */
  startedAt: number
  /** The moment, in epoch milliseconds, that the copy finished. */
  finishedAt: number
  /** The length of the copy, in milliseconds. */
  durationMs: number
  /** The number of pages that SQLite copies. */
  pageCount: number
  /** The size of one page, in bytes. */
  pageSize: number
  /** The size of the copy, in bytes. */
  byteLength: number
  /** The number of times that SQLite restarts the copy from page one. */
  restarts: number
}

/**
 * The result of one copy to a local file. The controller for the database adds
 * the database identifier and the source path to build a
 * {@link BackupFileReport}.
 *
 * @internal
 */
export type BackupFileCopy = Omit<BackupFileReport, 'databaseId' | 'sourcePath'>

/** The settings for one backup to a destination that the caller supplies.
 * @public
 */
export interface BackupToDestinationOptions {
  /** The destination that stores the pieces, and that a restore reads them from. */
  destination: BackupDestination
  /** The name to store the pieces under. Defaults to a name with a timestamp. */
  name?: string
  /** The chain that this copy begins. Defaults to a random identifier. */
  chainId?: string
  /**
   * The size of one whole piece, in bytes. Defaults to 16 MiB. For a streamed
   * copy, the size must divide by 512, because Sirannon passes SQLite whole
   * 512-byte blocks.
   */
  pieceBytes?: number
  /** The number of pages that SQLite copies in one step. */
  pagesPerStep?: number
  /** The number of restarts from page one that Sirannon allows before it fails the copy. */
  restartLimit?: number
  /** The number of milliseconds that can pass without a completed step before Sirannon fails the copy. */
  stallTimeoutMs?: number
  /** The number of milliseconds that one call to the destination can take before Sirannon fails the backup. Defaults to 10 minutes, and zero removes the deadline. */
  destinationTimeoutMs?: number
  /** The number of steps that can pass without SQLite copying a new page before Sirannon fails the copy. */
  noProgressStepLimit?: number
  /** The directory that the staged route writes its local file in. Defaults to the temporary directory of the system. */
  stagingDir?: string
  /**
   * Whether Sirannon computes the SHA-256 of the backup. Defaults to true. For a
   * streamed copy, Sirannon reads every piece back from the destination to
   * compute it, because it never holds the whole file, so you pay for that
   * extra read from remote storage.
   */
  fingerprint?: boolean
  /** Called after each step of the copy and after each piece that Sirannon stores. */
  onProgress?: (progress: BackupProgress) => void
}

/** The options of the caller for one backup, plus the details of the source database and the first-step callback.
 * @internal
 */
export interface BackupRunRequest extends BackupToDestinationOptions {
  /** The identifier of the source database. */
  databaseId: string
  /** The path of the source database file. */
  sourcePath: string
  /** Called once after the first step of the copy, so that the caller can release the writer. */
  onFirstStep?: () => void
}
