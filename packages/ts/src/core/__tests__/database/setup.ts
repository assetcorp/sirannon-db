import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach } from 'vitest'
import { Database } from '../../database.js'
import { testDriver } from '../helpers/test-driver.js'

export const state: { tempDir: string; openDbs: Database[] } = {
  tempDir: '',
  openDbs: [],
}

export function getTempDir(): string {
  return state.tempDir
}

beforeEach(() => {
  state.tempDir = mkdtempSync(join(tmpdir(), 'sirannon-db-'))
})

afterEach(async () => {
  for (const db of state.openDbs) {
    try {
      if (!db.closed) await db.close()
    } catch {
      /* best-effort cleanup */
    }
  }
  state.openDbs.length = 0
  rmSync(state.tempDir, { recursive: true, force: true })
})

export async function createTestDb(options?: { readOnly?: boolean; readPoolSize?: number }): Promise<Database> {
  const dbPath = join(state.tempDir, 'test.db')

  if (options?.readOnly) {
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    await setup.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await setup.close()
    const db = await Database.create('test', dbPath, testDriver, { readOnly: true })
    state.openDbs.push(db)
    return db
  }

  const db = await Database.create('test', dbPath, testDriver, options)
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  state.openDbs.push(db)
  return db
}
