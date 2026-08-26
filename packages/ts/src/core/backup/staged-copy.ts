import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { randomHex } from '../random-hex.js'
import { reportQuietly } from './cycle-callbacks.js'
import { sendFileInPieces } from './pieces.js'
import {
  assertPieceBytes,
  type BackupProgress,
  type BackupRunReport,
  type BackupRunRequest,
  DEFAULT_PIECE_BYTES,
  defaultDestinationName,
  readPageSize,
} from './report.js'
import { copyDatabaseStepwise } from './stepped-copy.js'
import { readLogPosition } from './wal-log.js'

const STAGED_FILE_NAME = 'copy.db'

/**
 * Copies a database to a caller-supplied destination by writing one local file
 * and sending it on in fixed-size pieces, so the run needs local disk equal to
 * the backup.
 *
 * @param conn - Connection the copy runs on, which must be the connection that writes.
 * @param request - Destination, naming, sizing, and progress reporting for this run.
 * @returns What the run copied, how long each part took, and how often the copy restarted.
 */
export async function copyToDestinationStaged(
  conn: SQLiteConnection,
  request: BackupRunRequest,
): Promise<BackupRunReport> {
  const runId = randomHex(8)
  const name = request.name ?? defaultDestinationName()
  const pieceBytes = request.pieceBytes ?? DEFAULT_PIECE_BYTES
  assertPieceBytes(pieceBytes)

  const startedAt = Date.now()
  const stagingRoot = await mkdtemp(join(request.stagingDir ?? tmpdir(), 'sirannon-backup-'))
  const stagedPath = join(stagingRoot, STAGED_FILE_NAME)
  let firstStepSeen = false
  let copyLeftRunning: Promise<unknown> | null = null

  const emit = (progress: Omit<BackupProgress, 'runId'>) => reportQuietly(request.onProgress, { runId, ...progress })

  try {
    const pageSize = await readPageSize(conn)
    const copyStartedAt = Date.now()
    const copy = await copyDatabaseStepwise(conn, {
      destPath: stagedPath,
      ...(request.pagesPerStep === undefined ? {} : { pagesPerStep: request.pagesPerStep }),
      ...(request.restartLimit === undefined ? {} : { restartLimit: request.restartLimit }),
      ...(request.stallTimeoutMs === undefined ? {} : { stallTimeoutMs: request.stallTimeoutMs }),
      ...(request.noProgressStepLimit === undefined ? {} : { noProgressStepLimit: request.noProgressStepLimit }),
      onCopyLeftRunning: copy => {
        copyLeftRunning = copy
      },
      onStep: step => {
        if (!firstStepSeen) {
          firstStepSeen = true
          request.onFirstStep?.()
        }
        emit({ phase: 'copy', ...step, piecesWritten: 0, bytesWritten: 0 })
      },
    })
    const copyMs = Date.now() - copyStartedAt
    const logPosition = await readLogPosition(request.sourcePath)

    const transferStartedAt = Date.now()
    const sent = await sendFileInPieces(
      stagedPath,
      request.destination,
      name,
      pieceBytes,
      request.fingerprint ?? true,
      (piecesWritten, bytesWritten) =>
        emit({
          phase: 'transfer',
          totalPages: copy.pageCount,
          remainingPages: 0,
          restarts: copy.restarts,
          piecesWritten,
          bytesWritten,
        }),
    )
    const transferMs = Date.now() - transferStartedAt
    const finishedAt = Date.now()

    return {
      runId,
      databaseId: request.databaseId,
      sourcePath: request.sourcePath,
      kind: 'full',
      chainId: request.chainId ?? randomHex(8),
      route: 'staged',
      destinationName: name,
      startedAt,
      finishedAt,
      durationMs: finishedAt - startedAt,
      copyMs,
      transferMs,
      pageCount: copy.pageCount,
      pageSize,
      bytesWritten: sent.bytesWritten,
      pieceCount: sent.pieceCount,
      pieceBytes,
      restarts: copy.restarts,
      ...(logPosition ? { logPosition } : {}),
      ...(sent.fingerprint ? { fingerprint: sent.fingerprint } : {}),
    }
  } finally {
    const removeStaging = () => rm(stagingRoot, { recursive: true, force: true }).catch(() => {})
    if (copyLeftRunning) void (copyLeftRunning as Promise<unknown>).then(removeStaging, removeStaging)
    else await removeStaging()
  }
}
