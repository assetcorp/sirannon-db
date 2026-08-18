import type { BackupDestination } from './destination.js'

export const DEFAULT_PIECE_BYTES = 16 * 1024 * 1024

/** How far one backup run has moved, reported at step resolution.
 * @public
 */
export interface BackupProgress {
  /** Identifier of the run this progress belongs to. */
  runId: string
  /** Whether the run is copying pages or sending pieces. */
  phase: 'copy' | 'transfer'
  /** Pages the copy has to move in total. */
  totalPages: number
  /** Pages the copy has yet to move. */
  remainingPages: number
  /** Times the copy has returned to page one. */
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
  /** What the run copied, which is the whole database. */
  kind: 'full'
  /** Whether the bytes reached the destination through a local file or without one. */
  route: 'staged' | 'streamed'
  /**
   * Name the pieces are stored under at the destination. The staged route
   * writes this one name and no other, because SQLite's journal beside the
   * copy stays on local disk and goes when the staging directory goes. A route
   * that sends a second name to the destination records both, so that a
   * restore finds them.
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
  /** Bytes one whole piece holds. Defaults to 16 MiB. */
  pieceBytes?: number
  /** Pages SQLite moves in one step. */
  pagesPerStep?: number
  /** Restarts the copy survives before it stops with an error. */
  restartLimit?: number
  /** Milliseconds the copy may move no pages before it stops with an error. */
  stallTimeoutMs?: number
  /** Steps the copy may take without moving a page it had not already moved. */
  noProgressStepLimit?: number
  /** Directory the staged route writes its local file in. Defaults to the system temporary directory. */
  stagingDir?: string
  /** Whether the run fingerprints what it wrote. Defaults to true. */
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
