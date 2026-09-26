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

/** The paths, chain position, and log cursor for one capture.
 * @internal
 */
export interface CaptureRequest {
  /** The path of the database file, which the error message quotes. */
  sourcePath: string
  /** The path of its write-ahead log. */
  logPath: string
  /** The directory to stage the frames in. */
  stagingDir: string
  /** The chain that this piece extends. */
  chainId: string
  /** The prefix of the piece name at the destination. */
  namePrefix: string
  /** The position of the piece in its chain, counted from one. */
  sequence: number
  /** The point in the log where the previous capture stopped, or null at the head of a chain. */
  cursor: LogCursor | null
  /**
   * Whether the database closed with its whole log captured. SQLite deletes the
   * log when the database closes, so Sirannon accepts a new log when the
   * database opens again.
   */
  expectNewLog: boolean
}

/**
 * Returns the path of the file that a capture stages its frames in. The cycle
 * builds this path from the position in the chain each time, so that Sirannon
 * deletes only a path that it builds itself.
 *
 * @param stagingDir - The directory that the cycle stages captures in.
 * @param sequence - The position of the piece in its chain, counted from one.
 * @returns The path of that file.
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
 * Copies the log frames that a database writes after the previous capture into
 * a staging file of their own. The checkpoint after the capture empties the
 * log, so Sirannon must copy the frames out before that checkpoint.
 *
 * A capture that starts at frame one includes the 32-byte log header with the
 * frames, so that a restore can pass the piece to SQLite as a log to recover
 * from.
 *
 * When the log restarts before this capture reads it, no backup holds the
 * writes in the lost frames, so this function throws `BACKUP_LOG_REWOUND`.
 *
 * @param request - The paths to read from and stage in, and the point where the previous capture stopped.
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

/** The settings, state, and paths for the capture step of one turn.
 * @internal
 */
export interface CaptureTurnRequest {
  /** The destination, the naming, and the locks that the turn holds during its checkpoint. */
  request: BackupCycleRequest
  /** The state that the cycle records for the chain that it extends, which this step advances. */
  state: BackupCycleState
  /** The path of the write-ahead log of the database. */
  logPath: string
  /** The directory that Sirannon stages the frames in. */
  stagingDir: string
  /** The prefix of the piece names at the destination. */
  namePrefix: string
}

/**
 * Copies the frames written since the previous turn and then checkpoints the
 * log, while no other operation holds the writer.
 *
 * The capture must come first, because SQLite lets a checkpoint overwrite
 * frames that Sirannon has yet to read, and it reports success either way.
 * Sirannon therefore copies the frames to local disk before the checkpoint,
 * and it holds the writer for both steps. Sirannon writes the state file after
 * each step, so that a turn interrupted between them can resume where it
 * stopped.
 *
 * @param turn - The request and state of the cycle, and the paths for the frames.
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
 * Discards the chain in a staging directory by deleting the state file and
 * every staged capture.
 *
 * The restore route of the server calls this before it replaces the database
 * file, since the log of the rebuilt file continues no part of the old chain,
 * so no backup can use the frames staged for that chain. Those files would
 * otherwise take up disk space for a chain that no capture extends.
 *
 * @param stagingDir - The directory that the cycle stages its captures in.
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
