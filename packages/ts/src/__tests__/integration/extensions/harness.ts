import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import {
  compiledExtensionPath,
  EXTENSION_PROBE_FUNCTION,
  EXTENSION_PROBE_VALUE,
} from '../../helpers/compiled-extension.js'

export const PROBE = EXTENSION_PROBE_FUNCTION
export const PROBE_VALUE = EXTENSION_PROBE_VALUE
export const PROBE_LIBRARY = compiledExtensionPath()

export const DRIVER_PLACEMENTS = [
  { label: 'better-sqlite3, writes on the calling thread', driver: () => betterSqlite3(), writerWorker: false },
  { label: 'better-sqlite3, writes on the writer worker', driver: () => betterSqlite3(), writerWorker: true },
  { label: 'node:sqlite, writes on the calling thread', driver: () => nodeSqlite(), writerWorker: false },
  { label: 'node:sqlite, writes on the writer worker', driver: () => nodeSqlite(), writerWorker: true },
]

export interface Workspace {
  dir: string
  path(name: string): string
  registry(driver: SQLiteDriver, options?: Partial<SirannonOptions>): Sirannon
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-extensions-${label}-`))
  const registries: Sirannon[] = []
  return {
    dir,
    path: name => join(dir, name),
    registry: (driver, options) => {
      const sirannon = new Sirannon({ driver, ...options })
      registries.push(sirannon)
      return sirannon
    },
    dispose: async () => {
      for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export function readRaw<T = Record<string, unknown>>(path: string, sql: string): T[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    return raw.prepare(sql).all() as T[]
  } finally {
    raw.close()
  }
}

export async function probeAnswers(db: Database): Promise<unknown> {
  return (await db.queryOne<{ value: unknown }>(`SELECT ${PROBE}() AS value`))?.value
}

export function probeFailure(db: Database): Promise<string> {
  return db.query(`SELECT ${PROBE}() AS value`).then(
    () => 'the probe function answered',
    (err: Error) => err.message,
  )
}
