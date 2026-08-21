import { readdir, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import { checkpointLog } from './checkpoint.js'
import type { BackupCycleRequest } from './cycle-options.js'
import { type BackupCycleState, type PendingCapture, removeCycleState, writeCycleState } from './cycle-state.js'
import { logFrameOffset } from './wal-format.js'

const STAGED_CAPTURE_PREFIX = 'capture-'
const STAGED_CAPTURE_SUFFIX = '.wal'

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
 * Names the file a capture stages its frames in. The cycle rebuilds this path
 * from the chain position each time, so no path it reads back off disk ever
 * reaches an unlink.
 *
 * @param stagingDir - Directory the cycle stages captures in.
 * @param sequence - Where the piece comes in its chain, counted from one.
 * @returns Path of that file.
 */
export function stagedCapturePath(stagingDir: string, sequence: number): string {
  return join(stagingDir, `${STAGED_CAPTURE_PREFIX}${sequence}${STAGED_CAPTURE_SUFFIX}`)
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

/** What one turn of the cycle reads its frames with, and what it records them against.
 * @internal
 */
export interface CaptureTurnRequest {
  /** Destination, naming, and the locks the turn takes its checkpoint under. */
  request: BackupCycleRequest
  /** What the cycle records about the chain it is extending, which this advances. */
  state: BackupCycleState
  /** Path of the database's write-ahead log. */
  logPath: string
  /** Directory Sirannon stages the frames in. */
  stagingDir: string
  /** What the pieces are named after at the destination. */
  namePrefix: string
}

/**
 * Reads the frames written since the previous turn and then checkpoints the
 * log, both with nothing else holding the writer lock.
 *
 * The order is what makes the capture safe. SQLite lets a checkpoint overwrite
 * frames nothing has read yet, and it reports success either way, so the frames
 * reach local disk first and the checkpoint follows inside the same held
 * writer. Sirannon writes the state file after each of those two steps, which
 * is what lets a turn interrupted between them start again where it stopped.
 *
 * @param turn - The cycle's request, its state, and where Sirannon stages the frames.
 *
 * @internal
 */
export async function captureAndCheckpointTurn(turn: CaptureTurnRequest): Promise<void> {
  const { request, state, stagingDir } = turn
  await request.runExclusive(async () => {
    const captured = await captureLogFrames({
      sourcePath: request.sourcePath,
      logPath: turn.logPath,
      stagingDir,
      chainId: state.chainId,
      namePrefix: turn.namePrefix,
      sequence: state.records,
      cursor: state.cursor,
      expectNewLog: state.closedCleanly,
    })

    if (captured) {
      state.pending = captured
      await writeCycleState(stagingDir, state)
    }

    const checkpointed = (await checkpointLog(request.acquireWriter())).emptied
    const cursor = captured?.cursor ?? state.cursor
    if (cursor) cursor.checkpointed = checkpointed
    state.closedCleanly = false
    await writeCycleState(stagingDir, state)
  })
}

/**
 * Discards the chain a staging directory was built around, by removing the
 * state file and every set of frames staged against that chain.
 *
 * A restore calls this before it replaces the database file, since the rebuilt
 * file's log continues none of the old chain and the frames staged against that
 * chain then belong to no backup. Files left there would occupy disk for a
 * chain nothing extends.
 *
 * @param stagingDir - Directory the cycle staged its captures in.
 *
 * @internal
 */
export async function discardStagedChain(stagingDir: string): Promise<void> {
  await removeCycleState(stagingDir)
  const entries = await readdir(stagingDir).catch(() => [])
  for (const entry of entries) {
    if (entry.startsWith(STAGED_CAPTURE_PREFIX) && entry.endsWith(STAGED_CAPTURE_SUFFIX)) {
      await rm(join(stagingDir, entry), { force: true })
    }
  }
}
