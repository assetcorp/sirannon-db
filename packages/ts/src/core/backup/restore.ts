import { rename, rm, stat } from 'node:fs/promises'
import type { SQLiteDriver } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { assembleStoredFile } from './assemble.js'
import type { BackupChainChange } from './chain.js'
import { DEFAULT_CHAIN_NAME, readBackupChains } from './chain.js'
import { planBackupRestore } from './chain-queries.js'
import { checkpointLog } from './checkpoint.js'
import { reportQuietly } from './cycle-callbacks.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from './destination-deadline.js'
import { applyChangeBatch, assertChangePiecesRunOn, countDatabasePages } from './restore-apply.js'
import { readDatabaseHeader } from './restore-log.js'
import {
  type BackupRestoreOptions,
  type BackupRestoreProgress,
  type BackupRestoreReport,
  DEFAULT_RESTORE_BATCH_SIZE,
  MAX_RESTORE_BATCH_SIZE,
} from './restore-options.js'

function assertBatchSize(batchSize: number): void {
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAX_RESTORE_BATCH_SIZE) {
    throw new SirannonError(
      `A restore replays between one and ${MAX_RESTORE_BATCH_SIZE} change pieces per batch, and the batch size given was ${batchSize}`,
      'BACKUP_ERROR',
    )
  }
}

const BUILD_SUFFIX = '.restoring'

async function removeLogBeside(path: string): Promise<void> {
  await rm(`${path}-wal`, { force: true }).catch(() => {})
  await rm(`${path}-shm`, { force: true }).catch(() => {})
}

async function removeDatabaseAndLog(path: string): Promise<void> {
  await rm(path, { force: true }).catch(() => {})
  await removeLogBeside(path)
}

async function foldLogIntoDatabase(driver: SQLiteDriver, path: string): Promise<boolean> {
  const existing = await stat(path).catch(() => undefined)
  if (!existing) return true
  const log = await stat(`${path}-wal`).catch(() => undefined)
  if (!log || log.size === 0) return true
  const conn = await driver.open(path, { walMode: false, walAutoCheckpoint: 0 }).catch(() => undefined)
  if (!conn) return false
  const folded = await checkpointLog(conn).catch(() => undefined)
  await conn.close().catch(() => undefined)
  return folded?.emptied === true
}

async function assertPathIsFree(destPath: string, replaceExisting: boolean): Promise<void> {
  if (replaceExisting) return
  const existing = await stat(destPath).catch(() => undefined)
  if (!existing) return
  throw new SirannonError(
    `A restore would replace '${destPath}', and its write-ahead log with it. Pass replaceExisting to say that is what you want, or name a path this process holds nothing at.`,
    'BACKUP_ERROR',
  )
}

function batchesOf(changes: readonly BackupChainChange[], batchSize: number): BackupChainChange[][] {
  const batches: BackupChainChange[][] = []
  for (let start = 0; start < changes.length; start += batchSize) {
    batches.push(changes.slice(start, start + batchSize))
  }
  return batches
}

/**
 * Rebuilds a database at the moment that you name, and writes it to a path
 * that you choose.
 *
 * Sirannon reads the chain records at your destination and takes the newest
 * full copy that finished at or before that moment. It then applies every
 * change piece that it captured after that copy up to the same moment,
 * fetching one stored piece and applying it before it requests the next, so a
 * restore holds one stored piece in memory however large the database is.
 *
 * A gap fails the call in one of two ways. A missing change piece in the chain
 * throws `BACKUP_CHAIN_BROKEN`, naming the sequence number where the chain
 * stops, and a missing stored piece of a file throws
 * `BACKUP_DESTINATION_ERROR`, naming that piece. Sirannon also checks each file
 * against the byte count and the fingerprint in its record, both of which cover
 * the whole file.
 *
 * Sirannon builds the database next to the path that you name, and renames it
 * onto that path after the checkpoint of the last batch. When the restore fails
 * or the machine stops part-way, the path therefore keeps its previous
 * contents. Where a database already exists at that path, Sirannon checkpoints
 * its write-ahead log into it before the rename, so that a crash between those
 * two steps leaves that database complete. Where the checkpoint cannot empty
 * that log, because another connection holds the database or SQLite cannot open
 * the file, Sirannon deletes that database and its log, so that a crash at that
 * point leaves the path empty, with no database there that lacks its last
 * commits. A database at that path stops the call unless you set
 * `replaceExisting`, because the rename replaces that database entirely.
 *
 * The restore needs disk space for the finished database, one stored piece,
 * and the log that Sirannon writes for one batch of change pieces, whose size
 * `batchSize` sets.
 *
 * @param options - The destination to read from, the moment to restore to, and the path for the result.
 * @returns The chain that Sirannon reads, the moment that the rebuilt database reflects, and the counts of what the restore fetches and applies.
 *
 * @public
 */
export async function restoreBackup(options: BackupRestoreOptions): Promise<BackupRestoreReport> {
  const startedAt = Date.now()
  const batchSize = options.batchSize ?? DEFAULT_RESTORE_BATCH_SIZE
  assertBatchSize(batchSize)

  const destination = destinationWithDeadline(
    options.destination,
    options.destinationTimeoutMs ?? DEFAULT_DESTINATION_TIMEOUT_MS,
  )
  const chains = await readBackupChains(destination, options.chainName ?? DEFAULT_CHAIN_NAME)
  const plan = planBackupRestore(chains, options.moment ?? Date.now())
  assertChangePiecesRunOn(plan.changes, plan.chainId)

  const destPath = options.destPath
  await assertPathIsFree(destPath, options.replaceExisting === true)
  const buildPath = `${destPath}${BUILD_SUFFIX}`
  await removeDatabaseAndLog(buildPath)

  let piecesFetched = 0
  let bytesFetched = 0
  let changesApplied = 0
  let framesApplied = 0
  const batches = batchesOf(plan.changes, batchSize)
  const reportProgress = (phase: BackupRestoreProgress['phase']): void =>
    reportQuietly(options.onProgress, {
      phase,
      piecesFetched,
      bytesFetched,
      changesApplied,
      changesTotal: plan.changes.length,
    })

  try {
    await assembleStoredFile(destination, plan.base, buildPath, (pieces, bytes) => {
      piecesFetched = pieces
      bytesFetched = bytes
      reportProgress('full-copy')
    })

    const header = await readDatabaseHeader(buildPath)
    if (!header.walMode && plan.changes.length > 0) {
      throw new SirannonError(
        `The full copy assembled into '${buildPath}' records no write-ahead log, so SQLite would ignore the change pieces rather than replay them`,
        'BACKUP_CHAIN_BROKEN',
      )
    }

    let logSequence = 0
    for (const batch of batches) {
      logSequence++
      framesApplied += await applyChangeBatch({
        destination,
        driver: options.driver,
        destPath: buildPath,
        pageSize: header.pageSize,
        logSequence,
        batch,
        onPiece: byteLength => {
          piecesFetched++
          bytesFetched += byteLength
          reportProgress('changes')
        },
      })
      changesApplied += batch.length
      reportProgress('changes')
    }

    await countDatabasePages(options.driver, buildPath)
    const logFoldedIn = await foldLogIntoDatabase(options.driver, destPath)
    if (logFoldedIn) await removeLogBeside(destPath)
    else await removeDatabaseAndLog(destPath)
    await rename(buildPath, destPath)
  } catch (err) {
    await removeDatabaseAndLog(buildPath)
    throw err
  }

  const finishedAt = Date.now()

  return {
    chainId: plan.chainId,
    destPath,
    baseName: plan.base.name,
    restoresTo: plan.restoresTo,
    pieceCount: piecesFetched,
    bytesFetched,
    changesApplied,
    framesApplied,
    batchCount: batches.length,
    startedAt,
    finishedAt,
    durationMs: finishedAt - startedAt,
  }
}
