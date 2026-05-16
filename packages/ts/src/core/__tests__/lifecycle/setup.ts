import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach } from 'vitest'
import { Database } from '../../database.js'
import type { LifecycleCallbacks } from '../../lifecycle/manager.js'
import { testDriver } from '../helpers/test-driver.js'

export const state: { tempDir: string } = { tempDir: '' }

export function getTempDir(): string {
  return state.tempDir
}

beforeEach(() => {
  state.tempDir = mkdtempSync(join(tmpdir(), 'sirannon-lifecycle-'))
})

afterEach(() => {
  rmSync(state.tempDir, { recursive: true, force: true })
})

export function createRegistry() {
  const dbs = new Map<string, Database>()

  const callbacks: LifecycleCallbacks = {
    async open(id: string, path: string, options?) {
      const db = await Database.create(id, path, testDriver, options)
      db.addCloseListener(() => {
        dbs.delete(id)
      })
      dbs.set(id, db)
      return db
    },
    async close(id: string) {
      const db = dbs.get(id)
      if (!db) throw new Error(`Database '${id}' not found`)
      await db.close()
    },
    count: () => dbs.size,
    has: (id: string) => dbs.has(id),
  }

  return { dbs, callbacks }
}
