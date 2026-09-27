import { existsSync, lstatSync, mkdirSync, readdirSync, rmSync, statSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError, SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from './destination-deadline.js'
import { type BackupFileCopy, type BackupRunReport, type BackupRunRequest, readPageSize } from './report.js'
import { copyToDestinationStaged } from './staged-copy.js'
import { copyDatabaseStepwise, type SteppedCopyResult } from './stepped-copy.js'
import { type BackupStreamingSupport, copyToDestinationStreamed } from './streamed-copy.js'

const BACKUP_FILE_PREFIX = 'backup'

function once(action: () => void): () => void {
  let done = false
  return () => {
    if (done) return
    done = true
    action()
  }
}

function removeQuietly(path: string): void {
  try {
    rmSync(path, { force: true })
  } catch {}
}

function removeOnceCopyStops(copy: Promise<unknown>, path: string): void {
  const remove = () => removeQuietly(path)
  copy.then(remove, remove)
}

function backupFailure(destPath: string, err: unknown): Error {
  if (err instanceof SirannonError) return err
  return new BackupError(`Backup to '${destPath}' failed: ${err instanceof Error ? err.message : String(err)}`)
}

function hasControlCharacters(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i)
    if (code <= 0x1f) return true
  }
  return false
}

export class BackupManager {
  constructor(private readonly streaming?: BackupStreamingSupport) {}

  /**
   * Copies the database that a connection has open to a file, while other
   * callers go on reading from and writing to that database.
   *
   * @param conn - The writer connection, which SQLite runs the copy on.
   * @param destPath - The path to write the copy to. Sirannon refuses a path where a file already exists.
   * @param onFirstStep - Called once after the first step of the copy, so that the caller can release the writer.
   * @returns The pages that SQLite copies, the time that the copy takes, and the number of times that SQLite restarts it from page one.
   */
  async backup(conn: SQLiteConnection, destPath: string, onFirstStep?: () => void): Promise<BackupFileCopy> {
    if (hasControlCharacters(destPath)) {
      throw new BackupError('Backup path contains invalid characters')
    }

    const segments = destPath.split(/[/\\]/)
    if (segments.includes('..')) {
      throw new BackupError('Backup path must not contain directory traversal segments')
    }

    const resolved = resolve(destPath)
    const dir = dirname(resolved)

    if (!existsSync(dir)) {
      try {
        mkdirSync(dir, { recursive: true })
      } catch (err) {
        throw new BackupError(
          `Failed to create backup directory '${dir}': ${err instanceof Error ? err.message : String(err)}`,
        )
      }
    }

    if (existsSync(resolved)) {
      throw new BackupError(`Backup destination '${destPath}' already exists`)
    }

    let pageSize: number
    try {
      pageSize = await readPageSize(conn)
    } catch (err) {
      throw backupFailure(destPath, err)
    }

    const startedAt = Date.now()
    const copy = await this.copyOrClearUp(conn, resolved, destPath, onFirstStep)
    const finishedAt = Date.now()

    return {
      runId: randomHex(8),
      destPath: resolved,
      startedAt,
      finishedAt,
      durationMs: finishedAt - startedAt,
      pageCount: copy.pageCount,
      pageSize,
      byteLength: this.fileBytes(resolved, destPath),
      restarts: copy.restarts,
    }
  }

  /**
   * Reads the size of the file that a copy writes.
   *
   * @param resolved - The absolute path of that file.
   * @param destPath - The path that the caller names, which Sirannon quotes in any error.
   * @returns The size of the file, in bytes.
   */
  private fileBytes(resolved: string, destPath: string): number {
    try {
      return statSync(resolved).size
    } catch (err) {
      throw backupFailure(destPath, err)
    }
  }

  /**
   * Runs the copy and deletes its file when the copy fails, so that a partly
   * written database never stays on disk. Rotation would otherwise count that
   * file among the copies it keeps, and it would delete a complete copy to make
   * room for it.
   *
   * When the stall deadline passes, Sirannon stops waiting on the copy while
   * SQLite goes on writing to the file. Deleting the file under a live copy
   * would leave a truncated file in place, and on Windows the delete would fail.
   * Sirannon therefore deletes the file once the copy stops, although the caller
   * receives the failure straight away.
   */
  private async copyOrClearUp(
    conn: SQLiteConnection,
    resolved: string,
    destPath: string,
    onFirstStep?: () => void,
  ): Promise<SteppedCopyResult> {
    const abandoned: { copy: Promise<unknown> | null } = { copy: null }
    try {
      return await copyDatabaseStepwise(conn, {
        destPath: resolved,
        onStep: onFirstStep ? once(onFirstStep) : undefined,
        onCopyLeftRunning: copy => {
          abandoned.copy = copy
        },
      })
    } catch (err) {
      if (abandoned.copy) removeOnceCopyStops(abandoned.copy, resolved)
      else removeQuietly(resolved)
      throw backupFailure(destPath, err)
    }
  }

  async copyToDestination(conn: SQLiteConnection, request: BackupRunRequest): Promise<BackupRunReport> {
    const bounded = {
      ...request,
      destination: destinationWithDeadline(
        request.destination,
        request.destinationTimeoutMs ?? DEFAULT_DESTINATION_TIMEOUT_MS,
      ),
    }
    return this.streaming
      ? copyToDestinationStreamed(conn, bounded, this.streaming)
      : copyToDestinationStaged(conn, bounded)
  }

  streamsToDestination(): boolean {
    return this.streaming !== undefined
  }

  generateFilename(): string {
    const ts = new Date().toISOString().replace(/[:.]/g, '-')
    return `${BACKUP_FILE_PREFIX}-${ts}.db`
  }

  rotate(dir: string, maxFiles: number): void {
    if (maxFiles <= 0) return

    const resolved = resolve(dir)
    if (!existsSync(resolved)) return

    let entries: { path: string; mtimeMs: number }[]
    try {
      entries = readdirSync(resolved)
        .filter(f => f.startsWith(`${BACKUP_FILE_PREFIX}-`) && f.endsWith('.db'))
        .map(f => {
          const filePath = join(resolved, f)
          return { path: filePath, mtimeMs: lstatSync(filePath).mtimeMs }
        })
        .sort((a, b) => b.mtimeMs - a.mtimeMs)
    } catch (err) {
      throw new BackupError(
        `Failed to list backup files in '${dir}': ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    for (const entry of entries.slice(maxFiles)) {
      try {
        rmSync(entry.path, { force: true })
      } catch {}
    }
  }
}
