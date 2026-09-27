import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import type { BackupChainPosition } from './chain.js'
import { isBackupChainPosition } from './chain-records.js'
import type { LogCursor } from './wal-log.js'

const STATE_FILE_NAME = 'cycle.json'

/** A capture on local disk that Sirannon has yet to send to the destination.
 * @internal
 */
export interface PendingCapture {
  /** The name to store the frames under at the destination. */
  name: string
  /** The identifier that Sirannon reports the progress of this capture under. */
  runId: string
  /** The position of this piece in its chain, counted from one. */
  sequence: number
  /** The range of log frames that this capture holds. */
  position: BackupChainPosition
  /** The point in the log where this capture stopped, which the next capture starts from. */
  cursor: LogCursor
  /** The moment, in epoch milliseconds, that the capture started. */
  startedAt: number
  /** The moment, in epoch milliseconds, that Sirannon finished reading the log. */
  capturedAt: number
  /** The time that the read took, in milliseconds. */
  copyMs: number
  /** The number of log frames in the capture. */
  frameCount: number
  /** The size of the capture, in bytes. */
  byteLength: number
  /** The size of one database page, in bytes. */
  pageSize: number
}

/** The state that the cycle stores on local disk between one turn and the next.
 * @internal
 */
export interface BackupCycleState {
  /** The name that Sirannon stores the list of chains under. */
  chainName: string
  /** The chain that the cycle extends. */
  chainId: string
  /** The moment, in epoch milliseconds, that the chain started. */
  chainStartedAt: number
  /** The index of the chain in the list, so that a check can read that one record. */
  headIndex?: number
  /** The number of records in the chain, including its full copy. */
  records: number
  /** The point in the log where the last capture stopped. */
  cursor: LogCursor | null
  /** A capture that Sirannon has yet to send to the destination. */
  pending: PendingCapture | null
  /**
   * Whether the database closed with its whole log captured. SQLite deletes the
   * log when the database closes, so this flag lets Sirannon tell a new log
   * after a restart apart from a log that lost frames.
   */
  closedCleanly: boolean
}

function isWholeNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0
}

function isCursor(value: unknown): value is LogCursor {
  const cursor = value as LogCursor
  return (
    isWholeNumber(cursor?.logSequence) &&
    isWholeNumber(cursor.salt1) &&
    isWholeNumber(cursor.salt2) &&
    isWholeNumber(cursor.lastFrame) &&
    isWholeNumber(cursor.checksum1) &&
    isWholeNumber(cursor.checksum2) &&
    typeof cursor.checkpointed === 'boolean'
  )
}

function isPendingCapture(value: unknown): value is PendingCapture {
  const pending = value as PendingCapture
  return (
    typeof pending?.name === 'string' &&
    typeof pending.runId === 'string' &&
    isWholeNumber(pending.sequence) &&
    pending.sequence >= 1 &&
    isBackupChainPosition(pending.position) &&
    isCursor(pending.cursor) &&
    isWholeNumber(pending.startedAt) &&
    isWholeNumber(pending.capturedAt) &&
    isWholeNumber(pending.copyMs) &&
    isWholeNumber(pending.frameCount) &&
    isWholeNumber(pending.byteLength) &&
    isWholeNumber(pending.pageSize)
  )
}

function isCycleState(value: unknown): value is BackupCycleState {
  const state = value as BackupCycleState
  return (
    typeof state?.chainName === 'string' &&
    typeof state.chainId === 'string' &&
    isWholeNumber(state.chainStartedAt) &&
    (state.headIndex === undefined || isWholeNumber(state.headIndex)) &&
    isWholeNumber(state.records) &&
    (state.cursor === null || isCursor(state.cursor)) &&
    (state.pending === null || isPendingCapture(state.pending)) &&
    typeof state.closedCleanly === 'boolean'
  )
}

/**
 * Returns the path of the file that the cycle stores its state in.
 *
 * @param stagingDir - The directory that the cycle stages captures in.
 * @returns The path of the state file.
 */
export function cycleStatePath(stagingDir: string): string {
  return join(stagingDir, STATE_FILE_NAME)
}

/**
 * Reads the state that the cycle last recorded for the chain that it extends.
 * A database that has never run a cycle has no state file.
 *
 * @param stagingDir - The directory that the cycle stages captures in.
 * @returns The state, or undefined where the file is missing or invalid.
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
 * Stores the current state of the cycle. Sirannon writes a temporary file next
 * to the state file and then renames it over the state file, so that a crash
 * during the write leaves the previous state intact.
 *
 * @param stagingDir - The directory that the cycle stages captures in.
 * @param state - The current state of the cycle.
 */
export async function writeCycleState(stagingDir: string, state: BackupCycleState): Promise<void> {
  const path = cycleStatePath(stagingDir)
  await mkdir(dirname(path), { recursive: true })
  const staged = `${path}.writing`
  await writeFile(staged, JSON.stringify(state), 'utf8')
  await rename(staged, path)
}

/**
 * Deletes the state file, so that the cycle stops tracking the chain that it
 * was extending. A node calls this when it stops taking the backups of its
 * group, so that once Sirannon picks that node again, its next turn starts a
 * new chain with a full copy.
 *
 * @param stagingDir - The directory that the cycle stages captures in.
 */
export async function removeCycleState(stagingDir: string): Promise<void> {
  await rm(cycleStatePath(stagingDir), { force: true })
}
