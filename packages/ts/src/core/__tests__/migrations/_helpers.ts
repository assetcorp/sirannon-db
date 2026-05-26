import { mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach } from 'vitest'
import { Database } from '../../database.js'
import { testDriver } from '../helpers/test-driver.js'

export const ctx: { tempDir: string; migrationsDir: string } = {
  tempDir: '',
  migrationsDir: '',
}

export function registerMigrationsFixtures(): void {
  beforeEach(() => {
    ctx.tempDir = mkdtempSync(join(tmpdir(), 'sirannon-migrations-'))
    ctx.migrationsDir = join(ctx.tempDir, 'migrations')
    mkdirSync(ctx.migrationsDir)
  })

  afterEach(() => {
    rmSync(ctx.tempDir, { recursive: true, force: true })
  })
}

export async function createTestDb(): Promise<Database> {
  const dbPath = join(ctx.tempDir, 'test.db')
  return Database.create('test', dbPath, testDriver)
}
