import { rm } from 'node:fs/promises'
import { join } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import { destinationPieceError, fingerprintStoredPieces } from './pieces.js'
import {
  assertPieceBytes,
  type BackupProgress,
  type BackupRunReport,
  type BackupRunRequest,
  DEFAULT_PIECE_BYTES,
  defaultDestinationName,
  readPageSize,
} from './report.js'
import { copyDatabaseStepwise, DEFAULT_PAGES_PER_STEP } from './stepped-copy.js'
import { BackupStreamHost } from './vfs/stream-host.js'

const VFS_NAME = 'sirannon'
const PIECE_BLOCK_BYTES = 512
const DRAIN_POLL_MS = 1
const STILL_TAKING_REPORT_MS = 5
const REPORTS_BEFORE_THE_EXTENSION_STOPS_WAITING = 6
const MICROSECONDS_PER_MS = 1000

/** What one runtime needs before a copy can reach a destination without a local file.
 * @internal
 */
export interface BackupStreamingSupport {
  /** Absolute path of the compiled extension that carries the bytes. */
  extensionPath: string
  /** Opens the connection the extension's statements run on. */
  openConnection(): Promise<SQLiteConnection>
}

function destinationUri(streamId: number): string {
  return `file:sirannon-stream-${streamId}?vfs=${VFS_NAME}`
}

/**
 * Sizes one step and the queue behind it so that a step never fills the queue.
 * A step that filled it would wait for pieces no caller can take until that
 * step returns, and the run would never finish.
 *
 * @param requestedPagesPerStep - Pages the caller asked SQLite to move in one step.
 * @param pieceBytes - Bytes one whole piece holds.
 * @param pageSize - Bytes one page holds.
 * @returns The pages one step moves and the pieces the extension holds.
 */
function sizeTheSteps(
  requestedPagesPerStep: number,
  pieceBytes: number,
  pageSize: number,
): { pagesPerStep: number; maxQueuedPieces: number } {
  const pagesPerPiece = Math.max(1, Math.floor(pieceBytes / pageSize))
  const pagesPerStep = Math.max(1, Math.min(requestedPagesPerStep, pagesPerPiece))
  const piecesPerStep = Math.max(1, Math.ceil((pagesPerStep * pageSize) / pieceBytes))
  return { pagesPerStep, maxQueuedPieces: piecesPerStep + 1 }
}

function assertWholeBlocks(pieceBytes: number): void {
  if (pieceBytes % PIECE_BLOCK_BYTES !== 0) {
    throw new SirannonError(
      `A streamed copy hands SQLite whole ${PIECE_BLOCK_BYTES}-byte blocks, so its piece size must divide by ${PIECE_BLOCK_BYTES}, and it was ${pieceBytes}`,
      'BACKUP_ERROR',
    )
  }
}

async function removeFilesNamedAfterTheUri(uri: string): Promise<void> {
  const stray = join(process.cwd(), uri)
  await rm(stray, { force: true }).catch(() => undefined)
  await rm(`${stray}-journal`, { force: true }).catch(() => undefined)
}

async function assertTheStreamCarriedTheCopy(
  host: BackupStreamHost,
  streamId: number,
  bytesStreamed: number,
  pageCount: number,
  uri: string,
): Promise<void> {
  const failure = await recordedFailure(host, streamId)
  if (failure) throw failure
  if (bytesStreamed === 0 && pageCount > 0) {
    await removeFilesNamedAfterTheUri(uri)
    throw new SirannonError(
      'SQLite read the destination as an ordinary file name rather than as a URI, so the copy reached no destination. ' +
        'This runtime parses URI file names only where the operator sets SQLITE_USE_URI=1 in the environment before the SQLite module loads.',
      'BACKUP_UNSUPPORTED',
    )
  }
}

async function recordedFailure(host: BackupStreamHost, streamId: number): Promise<SirannonError | null> {
  const failure = await host.failure(streamId).catch(() => null)
  return failure ? new SirannonError(`The streamed copy stopped because ${failure}.`, 'BACKUP_ERROR') : null
}

/**
 * Copies a database to a caller-supplied destination as SQLite writes it, so
 * the run needs no local disk. Sirannon names its own virtual file system on
 * the copy, and the pieces travel to the destination while the copy is still
 * moving pages.
 *
 * @param conn - Connection the copy runs on, which must be the connection that writes.
 * @param request - Destination, naming, sizing, and progress reporting for this run.
 * @param support - What this runtime needs to carry the bytes without a local file.
 * @returns What the run copied, how long each part took, and how often the copy restarted.
 */
export async function copyToDestinationStreamed(
  conn: SQLiteConnection,
  request: BackupRunRequest,
  support: BackupStreamingSupport,
): Promise<BackupRunReport> {
  const runId = randomHex(8)
  const name = request.name ?? defaultDestinationName()
  const pieceBytes = request.pieceBytes ?? DEFAULT_PIECE_BYTES
  assertPieceBytes(pieceBytes)
  assertWholeBlocks(pieceBytes)

  const startedAt = Date.now()
  const pageSize = await readPageSize(conn)
  const { pagesPerStep, maxQueuedPieces } = sizeTheSteps(
    request.pagesPerStep ?? DEFAULT_PAGES_PER_STEP,
    pieceBytes,
    pageSize,
  )
  const copyRunsOffCallerThread = conn.copyRunsOffCallerThread === true
  const host = await BackupStreamHost.start(support.openConnection, support.extensionPath)
  let streamId: number
  try {
    streamId = await host.open(
      pieceBytes,
      maxQueuedPieces,
      copyRunsOffCallerThread,
      STILL_TAKING_REPORT_MS * REPORTS_BEFORE_THE_EXTENSION_STOPS_WAITING * MICROSECONDS_PER_MS,
    )
  } catch (err) {
    await host.stop().catch(() => undefined)
    throw err
  }
  const uri = destinationUri(streamId)
  const stillTaking = copyRunsOffCallerThread
    ? setInterval(() => {
        void host.reportStillTaking(streamId).catch(() => undefined)
      }, STILL_TAKING_REPORT_MS)
    : null
  stillTaking?.unref?.()
  const stopReporting = () => {
    if (stillTaking) clearInterval(stillTaking)
  }

  let firstStepSeen = false
  let copyLeftRunning: Promise<unknown> | null = null
  let writingPiece = false
  let copying = true
  let discarding = false
  let piecesWritten = 0
  let bytesWritten = 0
  let transferMs = 0
  let totalPages = 0
  let remainingPages = 0
  let restarts = 0
  let pumpFailure: unknown = null

  const emit = (progress: Omit<BackupProgress, 'runId'>) => request.onProgress?.({ runId, ...progress })

  const pump = async (): Promise<void> => {
    for (;;) {
      const piece = await host.take(streamId)
      if (!piece) {
        if (!copying) return
        await new Promise(resolve => setTimeout(resolve, DRAIN_POLL_MS))
        continue
      }
      if (discarding) continue
      const writeStartedAt = Date.now()
      writingPiece = true
      try {
        await request.destination.writePiece(name, piece.index, piece.bytes)
      } catch (err) {
        pumpFailure = destinationPieceError(name, piece.index, err)
        discarding = true
        continue
      } finally {
        writingPiece = false
        transferMs += Date.now() - writeStartedAt
      }
      piecesWritten++
      bytesWritten += piece.bytes.byteLength
      emit({ phase: 'transfer', totalPages, remainingPages, restarts, piecesWritten, bytesWritten })
    }
  }

  const pumping = pump()
    .catch((err: unknown) => {
      pumpFailure = err
    })
    .finally(stopReporting)

  try {
    const copyStartedAt = Date.now()
    const copy = await copyDatabaseStepwise(conn, {
      destPath: uri,
      pagesPerStep,
      ...(request.restartLimit === undefined ? {} : { restartLimit: request.restartLimit }),
      ...(request.stallTimeoutMs === undefined ? {} : { stallTimeoutMs: request.stallTimeoutMs }),
      ...(request.noProgressStepLimit === undefined ? {} : { noProgressStepLimit: request.noProgressStepLimit }),
      pauseWhile: () => writingPiece,
      onCopyLeftRunning: running => {
        copyLeftRunning = running
      },
      onStep: step => {
        if (pumpFailure) throw pumpFailure
        if (!firstStepSeen) {
          firstStepSeen = true
          request.onFirstStep?.()
        }
        totalPages = step.totalPages
        remainingPages = step.remainingPages
        restarts = step.restarts
        emit({ phase: 'copy', ...step, piecesWritten, bytesWritten })
      },
    })
    const copyMs = Date.now() - copyStartedAt

    const bytesStreamed = await host.finish(streamId)
    copying = false
    await pumping
    if (pumpFailure) throw pumpFailure
    await assertTheStreamCarriedTheCopy(host, streamId, bytesStreamed, copy.pageCount, uri)

    const fingerprint =
      request.fingerprint === false
        ? undefined
        : await fingerprintStoredPieces(request.destination, name, piecesWritten)
    const finishedAt = Date.now()

    return {
      runId,
      databaseId: request.databaseId,
      sourcePath: request.sourcePath,
      kind: 'full',
      chainId: request.chainId ?? randomHex(8),
      route: 'streamed',
      destinationName: name,
      startedAt,
      finishedAt,
      durationMs: finishedAt - startedAt,
      copyMs,
      transferMs,
      pageCount: copy.pageCount,
      pageSize,
      bytesWritten,
      pieceCount: piecesWritten,
      pieceBytes,
      restarts: copy.restarts,
      ...(fingerprint ? { fingerprint } : {}),
    }
  } catch (err) {
    throw (await recordedFailure(host, streamId)) ?? err
  } finally {
    discarding = true
    const release = async () => {
      stopReporting()
      copying = false
      await pumping
      await host.close(streamId).catch(() => undefined)
      await host.stop().catch(() => undefined)
    }
    if (copyLeftRunning) void (copyLeftRunning as Promise<unknown>).then(release, release)
    else await release()
  }
}
