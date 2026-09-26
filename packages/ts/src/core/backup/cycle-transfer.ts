import { existsSync } from 'node:fs'
import { rm } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import {
  appendChainHead,
  appendChainRecord,
  type BackupChainBase,
  type BackupChainChange,
  type BackupChainHead,
} from './chain.js'
import { stagedCapturePath } from './cycle-capture.js'
import type { BackupCycleRequest } from './cycle-options.js'
import { type BackupCycleState, type PendingCapture, writeCycleState } from './cycle-state.js'
import { sendFileInPieces } from './pieces.js'
import { type BackupRunReport, DEFAULT_PIECE_BYTES } from './report.js'

/** A chain that the cycle starts.
 * @internal
 */
export interface StartedChain {
  /** The identifier of the new chain. */
  chainId: string
  /** The moment, in epoch milliseconds, that the chain started. */
  startedAt: number
  /** The index of the chain in the list of chains, counted from zero. */
  headIndex: number
  /** The report of the full copy at the head of the chain. */
  report: BackupRunReport
}

/**
 * Copies the whole database to the destination and records that copy as the
 * head of a new chain. Every later change piece in the chain applies on top of
 * this copy.
 *
 * Sirannon stores the record of the copy before it adds the chain to the list,
 * so that a chain enters the list only after its full copy is recorded.
 *
 * @param request - The destination, the naming, and the function that takes the full copy.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param namePrefix - The prefix of the name of the copy.
 * @param previousChainId - The chain that the new chain replaces, where the cycle holds one.
 * @returns The new chain, and the report of its full copy.
 */
export async function startChain(
  request: BackupCycleRequest,
  chainName: string,
  namePrefix: string,
  previousChainId?: string,
): Promise<StartedChain> {
  const chainId = randomHex(8)
  const startedAt = Date.now()

  const report = await request.fullCopy({
    destination: request.destination,
    name: `${namePrefix}-${chainId}-full.db`,
    chainId,
    ...(request.pieceBytes === undefined ? {} : { pieceBytes: request.pieceBytes }),
    ...(request.fingerprint === undefined ? {} : { fingerprint: request.fingerprint }),
    ...(request.stagingDir === undefined ? {} : { stagingDir: request.stagingDir }),
    ...(request.pagesPerStep === undefined ? {} : { pagesPerStep: request.pagesPerStep }),
    ...(request.restartLimit === undefined ? {} : { restartLimit: request.restartLimit }),
    ...(request.stallTimeoutMs === undefined ? {} : { stallTimeoutMs: request.stallTimeoutMs }),
    ...(request.destinationTimeoutMs === undefined ? {} : { destinationTimeoutMs: request.destinationTimeoutMs }),
    ...(request.noProgressStepLimit === undefined ? {} : { noProgressStepLimit: request.noProgressStepLimit }),
    ...(request.onProgress === undefined ? {} : { onProgress: request.onProgress }),
  })

  const base: BackupChainBase = {
    kind: 'full',
    chainId,
    name: report.destinationName,
    runId: report.runId,
    finishedAt: report.finishedAt,
    pieceCount: report.pieceCount,
    pieceBytes: report.pieceBytes,
    bytesWritten: report.bytesWritten,
    ...(report.fingerprint ? { fingerprint: report.fingerprint } : {}),
  }
  await appendChainRecord(request.destination, chainName, base, 0)

  const head: BackupChainHead = {
    chainId,
    startedAt,
    ...(previousChainId ? { previousChainId } : {}),
  }
  const headIndex = await appendChainHead(request.destination, chainName, head)

  return { chainId, startedAt, headIndex, report }
}

/**
 * Sends a staged capture to the destination and records it as the next piece
 * of its chain. The record states the range of log frames in the piece, so
 * that a restore can tell where the piece fits.
 *
 * @param request - The destination, the naming, and the source database of the capture.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param chainId - The chain that this piece extends.
 * @param pending - The staged capture.
 * @param recordIndex - The index of its record in the chain, counted from zero.
 * @param stagedPath - The file that holds the staged frames.
 * @returns The report of the transfer.
 */
export async function transferCapture(
  request: BackupCycleRequest,
  chainName: string,
  chainId: string,
  pending: PendingCapture,
  recordIndex: number,
  stagedPath: string,
): Promise<BackupRunReport> {
  const pieceBytes = request.pieceBytes ?? DEFAULT_PIECE_BYTES
  const transferStartedAt = Date.now()
  const sent = await sendFileInPieces(
    stagedPath,
    request.destination,
    pending.name,
    pieceBytes,
    request.fingerprint ?? true,
    (piecesWritten, bytesWritten) =>
      request.onProgress?.({
        runId: pending.runId,
        phase: 'transfer',
        totalPages: pending.frameCount,
        remainingPages: 0,
        restarts: 0,
        piecesWritten,
        bytesWritten,
      }),
  )
  const finishedAt = Date.now()

  const record: BackupChainChange = {
    kind: 'change',
    chainId,
    name: pending.name,
    runId: pending.runId,
    sequence: pending.sequence,
    position: pending.position,
    capturedAt: pending.capturedAt,
    frameCount: pending.frameCount,
    pieceCount: sent.pieceCount,
    pieceBytes,
    bytesWritten: sent.bytesWritten,
    checkpointed: pending.cursor.checkpointed,
    ...(sent.fingerprint ? { fingerprint: sent.fingerprint } : {}),
  }
  await appendChainRecord(request.destination, chainName, record, recordIndex)

  return {
    runId: pending.runId,
    databaseId: request.databaseId,
    sourcePath: request.sourcePath,
    kind: 'change',
    chainId,
    route: 'staged',
    destinationName: pending.name,
    startedAt: pending.startedAt,
    finishedAt,
    durationMs: finishedAt - pending.startedAt,
    copyMs: pending.copyMs,
    transferMs: finishedAt - transferStartedAt,
    pageCount: pending.frameCount,
    pageSize: pending.pageSize,
    bytesWritten: sent.bytesWritten,
    pieceCount: sent.pieceCount,
    pieceBytes,
    restarts: 0,
    position: pending.position,
    ...(sent.fingerprint ? { fingerprint: sent.fingerprint } : {}),
  }
}

/**
 * Sends the capture that a previous turn staged on local disk, and records it
 * as the next piece of its chain. The cycle calls this before it reads the log
 * again, so that the destination receives the pieces in the order of the
 * writes that they hold.
 *
 * @param request - The destination, the naming, and the source database of the capture.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param stagingDir - The directory that holds the staged capture.
 * @param state - The state that the cycle records for the chain, which this function advances.
 * @returns The report of the transfer, or undefined where no capture is staged.
 *
 * @internal
 */
export async function sendStagedCapture(
  request: BackupCycleRequest,
  chainName: string,
  stagingDir: string,
  state: BackupCycleState | null,
): Promise<BackupRunReport | undefined> {
  const pending = state?.pending
  if (!state || !pending) return undefined

  const stagedPath = stagedCapturePath(stagingDir, pending.sequence)
  if (!existsSync(stagedPath)) {
    throw new SirannonError(
      `The frames staged for change piece ${pending.sequence} of chain '${state.chainId}' are no longer in '${stagedPath}', so the writes they carried are in no backup. ` +
        'Leave the staging directory to Sirannon, and take a fresh full copy so a new chain starts from a known state.',
      'BACKUP_CHAIN_BROKEN',
    )
  }

  const report = await transferCapture(request, chainName, state.chainId, pending, state.records, stagedPath)
  state.records++
  state.cursor = pending.cursor
  state.pending = null
  await writeCycleState(stagingDir, state)
  await rm(stagedPath, { force: true })
  request.onRun?.(report)
  return report
}

/**
 * Starts a new chain with a full copy and returns the state that the cycle
 * records for that chain.
 *
 * A cycle calls this when it holds no chain, once a chain passes its full-copy
 * interval, and after a turn fails with `BACKUP_LOG_REWOUND` or
 * `BACKUP_CHAIN_BROKEN`.
 *
 * @param request - The destination, the naming, and the function that takes the full copy.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param namePrefix - The prefix of the name of the copy.
 * @param stagingDir - The directory that the cycle stages its captures in.
 * @param previousChainId - The chain that this one replaces, where the cycle holds one.
 * @returns The state to record for the new chain, and the report of its full copy.
 *
 * @internal
 */
export async function beginReplacementChain(
  request: BackupCycleRequest,
  chainName: string,
  namePrefix: string,
  stagingDir: string,
  previousChainId?: string,
): Promise<{ state: BackupCycleState; report: BackupRunReport }> {
  const started = await startChain(request, chainName, namePrefix, previousChainId)
  const state: BackupCycleState = {
    chainName,
    chainId: started.chainId,
    chainStartedAt: started.startedAt,
    headIndex: started.headIndex,
    records: 1,
    cursor: null,
    pending: null,
    closedCleanly: false,
  }
  await writeCycleState(stagingDir, state)
  return { state, report: started.report }
}
