import { existsSync, lstatSync, mkdirSync, readdirSync, rmSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError, SirannonError } from '../errors.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from './destination-deadline.js'
import type { BackupRunReport, BackupRunRequest } from './report.js'
import { copyToDestinationStaged } from './staged-copy.js'
import { copyDatabaseStepwise } from './stepped-copy.js'
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

function hasControlCharacters(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i)
    if (code <= 0x1f) return true
  }
  return false
}

export class BackupManager {
  constructor(private readonly streaming?: BackupStreamingSupport) {}

  async backup(conn: SQLiteConnection, destPath: string, onFirstStep?: () => void): Promise<void> {
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

    try {
      await copyDatabaseStepwise(conn, { destPath: resolved, onStep: onFirstStep ? once(onFirstStep) : undefined })
    } catch (err) {
      try {
        rmSync(resolved, { force: true })
      } catch {}
      if (err instanceof SirannonError) throw err
      throw new BackupError(`Backup to '${destPath}' failed: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  copyToDestination(conn: SQLiteConnection, request: BackupRunRequest): Promise<BackupRunReport> {
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
