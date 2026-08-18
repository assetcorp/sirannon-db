import { join } from 'node:path'
import { SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import type { PendingCapture } from './cycle-state.js'
import { logFrameOffset } from './wal-format.js'
import { copyLogRange, cursorChecksum, type LogCursor, readLogFileHeader, sameLog, scanLogFrames } from './wal-log.js'

/** What one capture reads from, and where it puts what it finds.
 * @internal
 */
export interface CaptureRequest {
  /** Path of the database file. The error message names it. */
  sourcePath: string
  /** Path of its write-ahead log. */
  logPath: string
  /** Directory to stage the frames in. */
  stagingDir: string
  /** The chain this piece extends. */
  chainId: string
  /** Prefix to name it with at the destination. */
  namePrefix: string
  /** Where it comes in its chain, counted from one. */
  sequence: number
  /** Where the previous capture stopped. At the head of a chain there is none. */
  cursor: LogCursor | null
  /**
   * Whether the database closed with its whole log captured. SQLite deletes the
   * log as it closes, so a fresh log is expected on the way back up.
   */
  expectNewLog: boolean
}

/**
 * Works out where a capture stages its frames. The cycle rebuilds this path
 * from the chain position each time, so no path it reads back off disk ever
 * reaches an unlink.
 *
 * @param stagingDir - Directory the cycle stages captures in.
 * @param sequence - Where the piece comes in its chain, counted from one.
 * @returns Path of that file.
 */
export function stagedCapturePath(stagingDir: string, sequence: number): string {
  return join(stagingDir, `capture-${sequence}.wal`)
}

function rewoundError(request: CaptureRequest, detail: string): SirannonError {
  return new SirannonError(
    `The write-ahead log of '${request.sourcePath}' ${detail}, so the frames written before it did are in no backup. ` +
      'Another connection checkpointed the log, or the database file was replaced. ' +
      'Route every write and every checkpoint through Sirannon, and take a fresh full copy so a new chain starts from a known state.',
    'BACKUP_LOG_REWOUND',
  )
}

/**
 * Reads the log frames a database has written since the previous capture and
 * stages them in a file of their own. The checkpoint that follows empties the
 * log, so they have to be out of it by then.
 *
 * The first capture of a chain takes the 32-byte log header along with the
 * frames. That is what lets a restore hand the piece straight to SQLite as a
 * log it can recover from.
 *
 * A log that restarted before this capture reached it means writes went into no
 * backup at all, and that fails with `BACKUP_LOG_REWOUND`.
 *
 * @param request - Where to read, where to stage, and where the previous capture stopped.
 * @returns The staged capture, or undefined where the log holds nothing new.
 */
export async function captureLogFrames(request: CaptureRequest): Promise<PendingCapture | undefined> {
  const startedAt = Date.now()
  const cursor = request.cursor
  const header = await readLogFileHeader(request.logPath)

  const holdsEveryCapturedFrame = cursor !== null && header !== undefined && sameLog(header, cursor)
  const newLogAllowed = cursor === null || cursor.checkpointed || request.expectNewLog

  if (!holdsEveryCapturedFrame && !newLogAllowed && cursor) {
    throw rewoundError(
      request,
      header
        ? `restarted at checkpoint sequence ${header.logSequence} while the chain reaches frame ${cursor.lastFrame} of sequence ${cursor.logSequence}`
        : `holds no frames while the chain reaches frame ${cursor.lastFrame} of sequence ${cursor.logSequence}`,
    )
  }
  if (!header) return undefined

  const from =
    cursor && holdsEveryCapturedFrame
      ? { frame: cursor.lastFrame, checksum: cursorChecksum(cursor) }
      : { frame: 0, checksum: header.checksum }

  const scan = await scanLogFrames(request.logPath, header, from)
  if (scan.lastCommitFrame <= from.frame) {
    if (from.frame > 0 && scan.lastCommitFrame < from.frame) {
      throw rewoundError(
        request,
        `now ends at frame ${scan.lastCommitFrame} while the chain reaches frame ${from.frame}`,
      )
    }
    return undefined
  }

  const firstFrame = from.frame + 1
  const startOffset = firstFrame === 1 ? 0 : logFrameOffset(firstFrame, header.frameBytes)
  const byteLength = await copyLogRange(
    request.logPath,
    startOffset,
    scan.endOffset,
    stagedCapturePath(request.stagingDir, request.sequence),
  )
  const capturedAt = Date.now()

  return {
    name: `${request.namePrefix}-${request.chainId}-${String(request.sequence).padStart(6, '0')}.wal`,
    runId: randomHex(8),
    sequence: request.sequence,
    position: {
      logSequence: header.logSequence,
      salt1: header.salt1,
      salt2: header.salt2,
      firstFrame,
      lastFrame: scan.lastCommitFrame,
    },
    cursor: {
      logSequence: header.logSequence,
      salt1: header.salt1,
      salt2: header.salt2,
      lastFrame: scan.lastCommitFrame,
      checksum1: scan.checksum.first,
      checksum2: scan.checksum.second,
      checkpointed: false,
    },
    startedAt,
    capturedAt,
    copyMs: capturedAt - startedAt,
    frameCount: scan.lastCommitFrame - from.frame,
    byteLength,
    pageSize: header.pageSize,
  }
}
