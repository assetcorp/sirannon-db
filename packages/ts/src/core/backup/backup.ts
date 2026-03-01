import { existsSync, lstatSync, mkdirSync, readdirSync, rmSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import type Database from 'better-sqlite3'
import { BackupError } from '../errors.js'

type SqliteDb = InstanceType<typeof Database>

const BACKUP_FILE_PREFIX = 'backup'

function hasControlCharacters(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i)
    if (code <= 0x1f && code !== 0x09 && code !== 0x0a && code !== 0x0d) {
      return true
    }
  }
  return false
}

export class BackupManager {
  /**
   * Creates a one-shot backup of the database using VACUUM INTO.
   * Produces a fresh, defragmented copy at the specified destination path.
   *
   * The destination file must not already exist. Parent directories are
   * created automatically when missing.
   *
   * Note: there is a narrow TOCTOU window between the existence check and
   * the VACUUM INTO statement. In the unlikely event that another process
   * creates a file at the same path during that window, VACUUM INTO may
   * silently overwrite it depending on the SQLite version.
   */
  backup(db: SqliteDb, destPath: string): void {
    if (hasControlCharacters(destPath)) {
      throw new BackupError('Backup path contains invalid characters')
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

    // Prevent accidental overwrites. Some SQLite versions silently replace
    // existing files on VACUUM INTO, so we guard against that explicitly.
    if (existsSync(resolved)) {
      throw new BackupError(`Backup destination '${destPath}' already exists`)
    }

    // Escape single quotes for the SQLite string literal.
    // VACUUM INTO requires a literal path; parameterised binding is not supported.
    const escaped = resolved.replace(/'/g, "''")

    try {
      db.exec(`VACUUM INTO '${escaped}'`)
    } catch (err) {
      // Remove any partial file left behind by a failed VACUUM INTO.
      try {
        rmSync(resolved, { force: true })
      } catch {
        // Best-effort cleanup
      }
      throw new BackupError(`Backup to '${destPath}' failed: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  /**
   * Generates a timestamped backup filename.
   *
   * Format: `backup-YYYY-MM-DDTHH-MM-SS-mmmZ.db`
   *
   * Colons and dots in the ISO timestamp are replaced with hyphens so the
   * filename is safe on every major filesystem.
   */
  generateFilename(): string {
    const ts = new Date().toISOString().replace(/[:.]/g, '-')
    return `${BACKUP_FILE_PREFIX}-${ts}.db`
  }

  /**
   * Removes old backup files in {@link dir}, keeping the {@link maxFiles}
   * most recent entries. Only files matching the `backup-*.db` naming
   * convention are considered; other files in the directory are left alone.
   *
   * When {@link maxFiles} is zero or negative the method is a no-op.
   * A non-existent directory is silently ignored.
   */
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
      } catch {
        // Best-effort deletion; individual failures must not block the rest.
      }
    }
  }
}
