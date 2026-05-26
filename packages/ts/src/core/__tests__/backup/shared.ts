import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import { testDriver } from '../helpers/test-driver.js'

interface TempDirRef {
  path: string
}

export function useTempDir(): TempDirRef {
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
