import { createHash } from 'node:crypto'
import { mkdtemp, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import type { BackupDestination } from './destination.js'
import { type BackupProgress, type BackupRunReport, type BackupRunRequest, DEFAULT_PIECE_BYTES } from './report.js'
import { copyDatabaseStepwise } from './stepped-copy.js'

const STAGED_FILE_NAME = 'copy.db'

function destinationError(name: string, index: number, err: unknown): SirannonError {
  return new SirannonError(
    `The destination refused piece ${index} of '${name}': ${err instanceof Error ? err.message : String(err)}`,
    'BACKUP_DESTINATION_ERROR',
  )
}

async function readPageSize(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('PRAGMA page_size')
  const row = await stmt.get<{ page_size: number | bigint }>()
  return row ? Number(row.page_size) : 0
}

async function sendPieces(
  stagedPath: string,
  destination: BackupDestination,
  name: string,
  pieceBytes: number,
  fingerprint: boolean,
  report: (piecesWritten: number, bytesWritten: number) => void,
): Promise<{ pieceCount: number; bytesWritten: number; fingerprint?: string }> {
  const file = await open(stagedPath, 'r')
  const digest = fingerprint ? createHash('sha256') : null
  let index = 0
  let bytesWritten = 0
  try {
    const buffer = Buffer.allocUnsafe(pieceBytes)
    for (;;) {
      let filled = 0
      while (filled < pieceBytes) {
        const { bytesRead } = await file.read(buffer, filled, pieceBytes - filled, index * pieceBytes + filled)
        if (bytesRead === 0) break
        filled += bytesRead
      }
      if (filled === 0) break
      const piece = new Uint8Array(filled)
      piece.set(buffer.subarray(0, filled))
      digest?.update(piece)
      try {
        await destination.writePiece(name, index, piece)
      } catch (err) {
        throw destinationError(name, index, err)
      }
      index++
      bytesWritten += filled
      report(index, bytesWritten)
      if (filled < pieceBytes) break
    }
  } finally {
    await file.close()
  }
  return { pieceCount: index, bytesWritten, ...(digest ? { fingerprint: digest.digest('hex') } : {}) }
}

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
  const name = request.name ?? `backup-${new Date().toISOString().replace(/[:.]/g, '-')}.db`
  const pieceBytes = request.pieceBytes ?? DEFAULT_PIECE_BYTES
  if (!Number.isInteger(pieceBytes) || pieceBytes <= 0) {
    throw new SirannonError(
      `Piece size must be a positive whole number of bytes, and it was ${pieceBytes}`,
      'BACKUP_ERROR',
    )
  }

  const startedAt = Date.now()
  const stagingRoot = await mkdtemp(join(request.stagingDir ?? tmpdir(), 'sirannon-backup-'))
  const stagedPath = join(stagingRoot, STAGED_FILE_NAME)
  let firstStepSeen = false
  let copyLeftRunning: Promise<unknown> | null = null

  const emit = (progress: Omit<BackupProgress, 'runId'>) => request.onProgress?.({ runId, ...progress })

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

    const transferStartedAt = Date.now()
    const sent = await sendPieces(
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
      ...(sent.fingerprint ? { fingerprint: sent.fingerprint } : {}),
    }
  } finally {
    const removeStaging = () => rm(stagingRoot, { recursive: true, force: true }).catch(() => {})
    if (copyLeftRunning) void (copyLeftRunning as Promise<unknown>).then(removeStaging, removeStaging)
    else await removeStaging()
  }
}
