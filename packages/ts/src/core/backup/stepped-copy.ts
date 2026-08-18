import type { DatabaseCopyStep, SQLiteConnection } from '../driver/types.js'
import { BackupError, SirannonError } from '../errors.js'

export const DEFAULT_PAGES_PER_STEP = 256
export const DEFAULT_RESTART_LIMIT = 3
export const DEFAULT_STALL_TIMEOUT_MS = 30_000
export const DEFAULT_NO_PROGRESS_STEP_LIMIT = 256

/** How far a stepped copy has moved, reported after every step.
 * @public
 */
export interface SteppedCopyProgress {
  /** Pages the copy has to move in total. */
  totalPages: number
  /** Pages the copy has yet to move. */
  remainingPages: number
  /** Times the copy has returned to page one. */
  restarts: number
}

/** How Sirannon runs one stepped copy.
 * @internal
 */
export interface SteppedCopyOptions {
  destPath: string
  pagesPerStep?: number
  restartLimit?: number
  stallTimeoutMs?: number
  noProgressStepLimit?: number
  onStep?: (progress: SteppedCopyProgress) => void
}

/** What one finished stepped copy moved.
 * @internal
 */
export interface SteppedCopyResult {
  pageCount: number
  restarts: number
}

function restartLimitError(restarts: number, destPath: string): SirannonError {
  return new SirannonError(
    `The copy to '${destPath}' returned to page one ${restarts} times because another connection wrote to the source database or ran a RESTART or TRUNCATE checkpoint on it. ` +
      'Close any other connection that writes to this file, route its writes through Sirannon, and run the copy again.',
    'BACKUP_RESTARTED',
  )
}

function noProgressError(steps: number, destPath: string): SirannonError {
  return new SirannonError(
    `The copy to '${destPath}' moved no page it had not already moved across ${steps} steps. ` +
      'Another connection restarts the copy on every step, or the source grows faster than the copy moves it. ' +
      'Close any other connection that writes to this file, or run the copy when the write rate is lower.',
    'BACKUP_RESTARTED',
  )
}

function stallError(stallTimeoutMs: number, destPath: string): SirannonError {
  return new SirannonError(
    `The copy to '${destPath}' moved no pages for ${stallTimeoutMs}ms. ` +
      'SQLite steps the copy once per turn of the event loop, so a caller that never lets the loop reach its timers and immediates holds the copy still. ' +
      'Let the event loop run between writes, or raise the stall timeout for a host this slow.',
    'BACKUP_STALLED',
  )
}

function copiedPages(step: DatabaseCopyStep): number {
  return step.totalPages - step.remainingPages
}

function hasRestarted(step: DatabaseCopyStep, previous: DatabaseCopyStep | null): boolean {
  if (!previous) return false
  return copiedPages(step) < copiedPages(previous)
}

/**
 * Copies the database behind a connection to a file, one step at a time, and
 * stops when SQLite has returned the copy to page one more often than the
 * limit allows.
 *
 * @param conn - Connection the copy runs on, which must be the connection that writes.
 * @param options - Destination path, step size, restart limit, and the progress callback.
 * @returns The pages the copy moved and the number of restarts it survived.
 */
export async function copyDatabaseStepwise(
  conn: SQLiteConnection,
  options: SteppedCopyOptions,
): Promise<SteppedCopyResult> {
  if (!conn.copyDatabase) {
    throw new SirannonError(
      'This driver opens connections without a stepped copy call, so it cannot copy a database while writes continue',
      'BACKUP_UNSUPPORTED',
    )
  }

  const restartLimit = options.restartLimit ?? DEFAULT_RESTART_LIMIT
  const stallTimeoutMs = options.stallTimeoutMs ?? DEFAULT_STALL_TIMEOUT_MS
  const noProgressStepLimit = options.noProgressStepLimit ?? DEFAULT_NO_PROGRESS_STEP_LIMIT
  let previous: DatabaseCopyStep | null = null
  let restarts = 0
  let furthestCopied = -1
  let stepsWithoutProgress = 0
  let stopped: SirannonError | null = null

  let stallTimer: ReturnType<typeof setTimeout> | null = null
  let reportStall: (err: SirannonError) => void = () => {}
  const stalled = new Promise<never>((_, reject) => {
    reportStall = reject
  })
  const armStall = () => {
    if (stallTimeoutMs <= 0) return
    if (stallTimer) clearTimeout(stallTimer)
    stallTimer = setTimeout(() => {
      stopped = stallError(stallTimeoutMs, options.destPath)
      reportStall(stopped)
    }, stallTimeoutMs)
    stallTimer.unref?.()
  }
  armStall()

  const copy = conn
    .copyDatabase({
      destPath: options.destPath,
      pagesPerStep: options.pagesPerStep ?? DEFAULT_PAGES_PER_STEP,
      onStep: step => {
        if (stopped) throw stopped
        armStall()
        if (hasRestarted(step, previous)) {
          restarts++
          if (restarts > restartLimit) {
            stopped = restartLimitError(restarts, options.destPath)
            throw stopped
          }
        }
        if (copiedPages(step) > furthestCopied) {
          furthestCopied = copiedPages(step)
          stepsWithoutProgress = 0
        } else if (step.remainingPages > 0 && ++stepsWithoutProgress > noProgressStepLimit) {
          stopped = noProgressError(stepsWithoutProgress, options.destPath)
          throw stopped
        }
        previous = step
        options.onStep?.({ totalPages: step.totalPages, remainingPages: step.remainingPages, restarts })
      },
    })
    .catch((err: unknown) => {
      if (stopped) throw stopped
      throw err instanceof SirannonError
        ? err
        : new BackupError(`Copy to '${options.destPath}' failed: ${err instanceof Error ? err.message : String(err)}`)
    })

  try {
    const final = await Promise.race([copy, stalled])
    return { pageCount: final.totalPages, restarts }
  } finally {
    if (stallTimer) clearTimeout(stallTimer)
    copy.catch(() => {})
  }
}
