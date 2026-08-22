import { existsSync, lstatSync, mkdirSync, readdirSync, rmSync, statSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError, SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline, withinDeadline } from './destination-deadline.js'
import { type BackupFileCopy, type BackupRunReport, type BackupRunRequest, readPageSize } from './report.js'
import { copyToDestinationStaged } from './staged-copy.js'
import { copyDatabaseStepwise, type SteppedCopyResult } from './stepped-copy.js'
import { type BackupStreamingSupport, copyToDestinationStreamed } from './streamed-copy.js'

const BACKUP_FILE_PREFIX = 'backup'
const ABANDONED_COPY_GRACE_MS = 5_000
const STILL_COPYING = new SirannonError('The copy is still running', 'BACKUP_STALLED')

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
   * Copies the database behind a connection to a file, while that database
   * stays open for reads and writes.
   *
   * @param conn - Connection the copy runs on, which must be the connection that writes.
   * @param destPath - Path to write the copy to. A file already there stops the copy.
   * @param onFirstStep - Called once the copy's first step is done, so the caller can hand the writer back.
   * @returns What the copy moved, how long it took, and how often it restarted.
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
   * Reads the size of the file a copy wrote.
   *
   * @param resolved - Absolute path of that file.
   * @param destPath - Path the caller named, which the error states.
   * @returns Bytes it holds.
   */
  private fileBytes(resolved: string, destPath: string): number {
    try {
      return statSync(resolved).size
    } catch (err) {
      throw backupFailure(destPath, err)
    }
  }

  /**
   * Runs the copy and removes the file it was writing where it fails, so a
   * database missing its later pages never stays on disk as though the copy had
   * finished.
   *
   * A stalled copy keeps writing to that file after the stall timeout has
   * stopped waiting on it, so this waits five seconds for the copy to settle
   * before it removes anything. A removal that raced a live copy would leave a
   * truncated file behind, and on Windows it would fail outright.
   *
   * SQLite offers no way to cancel a copy under way, so a copy still running
   * after those five seconds keeps the file for now and Sirannon removes it
   * once that copy stops. Leaving it there for good would put a database
   * missing its later pages among the backups, where rotation would count it as
   * one and evict a whole copy to keep it.
   */
  private async copyOrClearUp(
    conn: SQLiteConnection,
    resolved: string,
    destPath: string,
    onFirstStep?: () => void,
  ): Promise<SteppedCopyResult> {
    let stillRunning: Promise<unknown> | null = null
    try {
      return await copyDatabaseStepwise(conn, {
        destPath: resolved,
        onStep: onFirstStep ? once(onFirstStep) : undefined,
        onCopyLeftRunning: copy => {
          stillRunning = copy
        },
      })
    } catch (err) {
      if (await this.copyHasStopped(stillRunning)) removeQuietly(resolved)
      else void (stillRunning as Promise<unknown> | null)?.finally(() => removeQuietly(resolved))
      throw backupFailure(destPath, err)
    }
  }

  /**
   * Waits five seconds for a copy the caller has stopped waiting on.
   *
   * @param stillRunning - The copy left running, or null where the run holds none.
   * @returns Whether the file it was writing is safe to remove now.
   */
  private async copyHasStopped(stillRunning: Promise<unknown> | null): Promise<boolean> {
    if (stillRunning === null) return true
    return withinDeadline(
      stillRunning.then(
        () => true,
        () => true,
      ),
      ABANDONED_COPY_GRACE_MS,
      () => STILL_COPYING,
    ).catch(() => false)
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
