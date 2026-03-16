import { existsSync, lstatSync, mkdirSync, readdirSync, rmSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError } from '../errors.js'

const BACKUP_FILE_PREFIX = 'backup'

function hasControlCharacters(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i)
    if (code <= 0x1f) return true
  }
  return false
}

export class BackupManager {
  async backup(conn: SQLiteConnection, destPath: string): Promise<void> {
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

    if (existsSync(resolved)) {
      throw new BackupError(`Backup destination '${destPath}' already exists`)
    }

    const escaped = resolved.replace(/'/g, "''")

    try {
      await conn.exec(`VACUUM INTO '${escaped}'`)
    } catch (err) {
      try {
        rmSync(resolved, { force: true })
      } catch {
        // Best-effort cleanup
      }
      throw new BackupError(`Backup to '${destPath}' failed: ${err instanceof Error ? err.message : String(err)}`)
    }
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
      } catch {
        // Best-effort deletion; individual failures must not block the rest.
      }
    }
  }
}
