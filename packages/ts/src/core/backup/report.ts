import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import type { BackupChainPosition } from './chain.js'
import type { BackupDestination } from './destination.js'

export const DEFAULT_PIECE_BYTES = 16 * 1024 * 1024

/**
 * Names the file a run stores its pieces under where the caller named none.
 *
 * @returns A name carrying the moment the run started.
 *
 * @internal
 */
export function defaultDestinationName(): string {
  return `backup-${new Date().toISOString().replace(/[:.]/g, '-')}.db`
}

/**
 * Refuses a piece size that no run could store its bytes in.
 *
 * @param pieceBytes - Bytes one whole piece is meant to hold.
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
 * Reads the page size of the database behind a connection, which a report
 * states alongside the pages a copy moved.
 *
 * @param conn - Connection to read from.
 * @returns Bytes one page holds, or zero where SQLite reported none.
 *
 * @internal
 */
export async function readPageSize(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('PRAGMA page_size')
  const row = await stmt.get<{ page_size: number | bigint }>()
  return row ? Number(row.page_size) : 0
}

/** How far one backup run has moved, reported at step resolution.
 * @public
 */
export interface BackupProgress {
  /** Identifier of the run this progress belongs to. */
  runId: string
  /** Whether the run is copying pages or sending pieces. */
  phase: 'copy' | 'transfer'
  /** Pages the copy has to move in total. A change piece reports the number of log frames it covers. */
  totalPages: number
  /** Pages the copy has yet to move. Every report of the transfer phase states zero, since the copy has finished by then. */
  remainingPages: number
  /** Times the copy has returned to page one. A change piece reports zero, since it reads the log and copies no pages. */
  restarts: number
  /** Pieces the run has stored at the destination. */
  piecesWritten: number
  /** Bytes the run has stored at the destination. */
  bytesWritten: number
}

/** What one finished backup run produced.
 * @public
 */
export interface BackupRunReport {
  /** Identifier this run reports its progress under. */
  runId: string
  /** Database the copy was taken from. */
  databaseId: string
  /** File the copy was taken from. */
  sourcePath: string
  /** What the run wrote: the whole database, or the log frames written since the run before it. */
  kind: 'full' | 'change'
  /** The chain this run belongs to. A full copy begins one, and every change piece after it extends the same one. */
  chainId: string
  /** Whether the bytes reached the destination through a local file or without one. */
  route: 'staged' | 'streamed'
  /**
   * Name the pieces are stored under at the destination. A run writes this one
   * name and no other, because the journal SQLite opens beside a copy stays on
   * local disk.
   */
  destinationName: string
  /** Epoch milliseconds the run started at. */
  startedAt: number
  /** Epoch milliseconds the run finished at. */
  finishedAt: number
  /** Milliseconds the whole run took. */
  durationMs: number
  /** Milliseconds SQLite spent copying pages. */
  copyMs: number
  /** Milliseconds the pieces took to reach the destination. */
  transferMs: number
  /** Pages the copy moved. */
  pageCount: number
  /** Bytes one page holds. */
  pageSize: number
  /** Bytes the run stored at the destination. */
  bytesWritten: number
  /** Pieces the run stored at the destination. */
  pieceCount: number
  /** Bytes one whole piece holds. */
  pieceBytes: number
  /** Times the copy returned to page one. */
  restarts: number
  /** The stretch of log a change piece covers. A full copy has none. */
  position?: BackupChainPosition
  /** SHA-256 of the copied file, unless the caller turned it off. */
  fingerprint?: string
}

/** How Sirannon runs one backup to a caller-supplied destination.
 * @public
 */
export interface BackupToDestinationOptions {
  /** Where the pieces go and where a restore reads them from. */
  destination: BackupDestination
  /** Name the pieces are stored under. Defaults to a timestamped name. */
  name?: string
  /** The chain this copy begins. Defaults to an identifier the run mints for itself. */
  chainId?: string
  /**
   * Bytes one whole piece holds. Defaults to 16 MiB. A streamed copy hands
   * SQLite whole 512-byte blocks, so it needs a size that divides by 512.
   */
  pieceBytes?: number
  /** Pages SQLite moves in one step. */
  pagesPerStep?: number
  /** Restarts the copy survives before it stops with an error. */
  restartLimit?: number
  /** Milliseconds the copy may move no pages before it stops with an error. */
  stallTimeoutMs?: number
  /** Milliseconds one call to the destination may take before the run stops with an error. Defaults to 10 minutes, and zero leaves the calls unbounded. */
  destinationTimeoutMs?: number
  /** Steps the copy may take without moving a page it had not already moved. */
  noProgressStepLimit?: number
  /** Directory the staged route writes its local file in. Defaults to the system temporary directory. */
  stagingDir?: string
  /**
   * Whether the run fingerprints what it wrote. Defaults to true. A streamed
   * copy reads every piece back from the destination to compute it, because it
   * never holds the whole file, so a run to remote storage pays for that read.
   */
  fingerprint?: boolean
  /** Called at step resolution while the run proceeds. */
  onProgress?: (progress: BackupProgress) => void
}

/** What one backup run needs beyond the caller's own options.
 * @internal
 */
export interface BackupRunRequest extends BackupToDestinationOptions {
  /** Database the copy is taken from. */
  databaseId: string
  /** File the copy is taken from. */
  sourcePath: string
  /** Called once the copy's first step is done, so the writer lock can be handed back. */
  onFirstStep?: () => void
}
