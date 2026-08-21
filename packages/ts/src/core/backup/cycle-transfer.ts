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

/** A chain the cycle has started.
 * @internal
 */
export interface StartedChain {
  /** Identifier of the new chain. */
  chainId: string
  /** Epoch milliseconds it started. */
  startedAt: number
  /** What the full copy at its head wrote. */
  report: BackupRunReport
}

/**
 * Copies the whole database to the destination and records that copy as the
 * head of a new chain. Every change piece after it names this copy as the one
 * it builds on.
 *
 * The copy's own record goes out before the chain joins the list, so a listing
 * never turns up a chain whose full copy is missing.
 *
 * @param request - Destination, naming, and the full copy to run.
 * @param chainName - Name the list of chains is stored under.
 * @param namePrefix - What to name the copy after.
 * @param previousChainId - The chain it replaces, where the cycle was extending one.
 * @returns The new chain, and what its full copy wrote.
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
  await appendChainHead(request.destination, chainName, head)

  return { chainId, startedAt, report }
}

/**
 * Sends a staged capture to the destination and records it as the next piece of
 * its chain. The record names the stretch of log the piece covers, which is
 * what tells a restore where it fits.
 *
 * @param request - Destination, naming, and the database the capture came from.
 * @param chainName - Name the list of chains is stored under.
 * @param chainId - The chain this piece extends.
 * @param pending - The staged capture.
 * @param recordIndex - Where its record goes in the chain, counted from zero.
 * @param stagedPath - File the capture staged its frames in.
 * @returns What the transfer wrote.
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
    () => {},
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
 * Sends the capture a previous turn staged on local disk, and records it as the
 * next piece of its chain. The cycle runs this before it reads the log again,
 * so the destination holds the pieces in the order the database wrote them.
 *
 * @param request - Destination, naming, and the database the capture came from.
 * @param chainName - Name the list of chains is stored under.
 * @param stagingDir - Directory the capture was staged in.
 * @param state - What the cycle remembers about the chain, which this advances.
 * @returns What the transfer wrote, or undefined where no capture was waiting.
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
