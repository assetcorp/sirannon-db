import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { testDriver } from '../helpers/test-driver.js'

interface TempDirRef {
  path: string
}

export function tempDirPerTest(): TempDirRef {
  const ref: TempDirRef = { path: '' }

  beforeEach(() => {
    ref.path = mkdtempSync(join(tmpdir(), 'sirannon-backup-'))
  })

  afterEach(() => {
    rmSync(ref.path, { recursive: true, force: true })
  })

  return ref
}

export async function createTestDb(tempDir: string): Promise<SQLiteConnection> {
  const dbPath = join(tempDir, 'source.db')
  const conn = await testDriver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')
  await conn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  await conn.exec("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  await conn.exec("INSERT INTO users (name, age) VALUES ('Bob', 25)")
  return conn
}

export function useCronTimers(): void {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'] })
}

export async function settleUntil(check: () => boolean, turns = 5000): Promise<void> {
  for (let turn = 0; turn < turns; turn++) {
    if (check()) return
    await new Promise(resolve => setImmediate(resolve))
  }
}

export interface CountingManager {
  manager: BackupManager
  completed: () => number
}

export function countingManager(): CountingManager {
  const manager = new BackupManager()
  const runBackup = manager.backup.bind(manager)
  let completed = 0
  manager.backup = async (conn, destPath, onFirstStep) => {
    await runBackup(conn, destPath, onFirstStep)
    completed++
  }
  return { manager, completed: () => completed }
}
