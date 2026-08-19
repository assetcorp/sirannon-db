import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { type BackupChainPosition, isBackupChainPosition } from './chain.js'
import type { LogCursor } from './wal-log.js'

const STATE_FILE_NAME = 'cycle.json'

/** A capture sitting on local disk that has yet to reach the destination.
 * @internal
 */
export interface PendingCapture {
  /** Name to store the frames under at the destination. */
  name: string
  /** Identifier the capture reports its progress under. */
  runId: string
  /** Where this piece comes in its chain, counted from one. */
  sequence: number
  /** The stretch of log it covers. */
  position: BackupChainPosition
  /** Where the capture stopped. The next one starts from here. */
  cursor: LogCursor
  /** Epoch milliseconds it started. */
  startedAt: number
  /** Epoch milliseconds it finished reading the log. */
  capturedAt: number
  /** How long that read took, in milliseconds. */
  copyMs: number
  /** How many frames it holds. */
  frameCount: number
  /** How big it is, in bytes. */
  byteLength: number
  /** Size of one database page, in bytes. */
  pageSize: number
}

/** What the cycle remembers on local disk between one turn and the next.
 * @internal
 */
export interface BackupCycleState {
  /** Name the list of chains is stored under. */
  chainName: string
  /** The chain being extended. */
  chainId: string
  /** Epoch milliseconds that chain started. */
  chainStartedAt: number
  /** How many records the chain holds, counting its full copy. */
  records: number
  /** Where the last capture stopped in the log. */
  cursor: LogCursor | null
  /** A capture still waiting to reach the destination. */
  pending: PendingCapture | null
  /**
   * Whether the database closed with its whole log captured. SQLite deletes the
   * log as it closes, so without this flag a fresh log after a restart would be
   * indistinguishable from one that lost frames.
   */
  closedCleanly: boolean
}

function isNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function isCursor(value: unknown): value is LogCursor {
  const cursor = value as LogCursor
  return (
    isNumber(cursor?.logSequence) &&
    isNumber(cursor.salt1) &&
    isNumber(cursor.salt2) &&
    isNumber(cursor.lastFrame) &&
    isNumber(cursor.checksum1) &&
    isNumber(cursor.checksum2) &&
    typeof cursor.checkpointed === 'boolean'
  )
}

function isPendingCapture(value: unknown): value is PendingCapture {
  const pending = value as PendingCapture
  return (
    typeof pending?.name === 'string' &&
    typeof pending.runId === 'string' &&
    isNumber(pending.sequence) &&
    isBackupChainPosition(pending.position) &&
    isCursor(pending.cursor) &&
    isNumber(pending.startedAt) &&
    isNumber(pending.capturedAt) &&
    isNumber(pending.copyMs) &&
    isNumber(pending.frameCount) &&
    isNumber(pending.byteLength) &&
    isNumber(pending.pageSize)
  )
}

function isCycleState(value: unknown): value is BackupCycleState {
  const state = value as BackupCycleState
  return (
    typeof state?.chainName === 'string' &&
    typeof state.chainId === 'string' &&
    isNumber(state.chainStartedAt) &&
    isNumber(state.records) &&
    (state.cursor === null || isCursor(state.cursor)) &&
    (state.pending === null || isPendingCapture(state.pending)) &&
    typeof state.closedCleanly === 'boolean'
  )
}

/**
 * Names the file the cycle keeps its state in.
 *
 * @param stagingDir - Directory the cycle stages captures in.
 * @returns Path of the state file.
 */
export function cycleStatePath(stagingDir: string): string {
  return join(stagingDir, STATE_FILE_NAME)
}

/**
 * Reads back what the cycle last recorded about the chain it was extending.
 * A database that has never run one has nothing here yet.
 *
 * @param stagingDir - Directory the cycle stages captures in.
 * @returns The state, or undefined where there is none.
 */
export async function readCycleState(stagingDir: string): Promise<BackupCycleState | undefined> {
  try {
    const text = await readFile(cycleStatePath(stagingDir), 'utf8')
    const state = JSON.parse(text) as unknown
    return isCycleState(state) ? state : undefined
  } catch {
    return undefined
  }
}

/**
 * Records where the cycle has got to. The write goes to a file beside the real
 * one and is then renamed over it, so a crash half way through leaves the
 * previous state intact and readable.
 *
 * @param stagingDir - Directory the cycle stages captures in.
 * @param state - Where the cycle has got to.
 */
export async function writeCycleState(stagingDir: string, state: BackupCycleState): Promise<void> {
  const path = cycleStatePath(stagingDir)
  await mkdir(dirname(path), { recursive: true })
  const staged = `${path}.writing`
  await writeFile(staged, JSON.stringify(state), 'utf8')
  await rename(staged, path)
}
