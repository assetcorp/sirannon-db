import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { Sirannon } from '../../../core/sirannon.js'
import type { SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { releaseHeldWrite } from '../../helpers/fault-drivers/sleeping-driver.js'

export const HELD_WRITE_DEADLINE_MS = 2_000
export const CRASH_DEADLINE_MS = 10_000
const WAIT_STEP_MS = 50
const WAIT_LIMIT_MS = 30_000

export interface Workspace {
  path(name: string): string
  releaseFile(name: string): string
  registry(options?: Partial<SirannonOptions>): Sirannon
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-writer-worker-${label}-`))
  const registries: Sirannon[] = []
  const releaseFiles: string[] = []
  return {
    path: name => join(dir, name),
    releaseFile: name => {
      const file = join(dir, `${name}.release`)
      releaseFiles.push(file)
      return file
    },
    registry: options => {
      const sirannon = new Sirannon({ driver: betterSqlite3(), ...options })
      registries.push(sirannon)
      return sirannon
    },
    dispose: async () => {
      for (const file of releaseFiles.splice(0)) releaseHeldWrite(file)
      for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export function readRaw<T = Record<string, unknown>>(path: string, sql: string, params: unknown[] = []): T[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    raw.defaultSafeIntegers(true)
    return raw.prepare(sql).all(...params) as T[]
  } finally {
    raw.close()
  }
}

export function columnRaw(path: string, sql: string, params: unknown[] = []): unknown[] {
  return readRaw<Record<string, unknown>>(path, sql, params).map(row => Object.values(row)[0])
}

export function integrityRaw(path: string): unknown[] {
  return columnRaw(path, 'PRAGMA integrity_check')
}

export function codeOf(outcome: PromiseSettledResult<unknown>): string {
  if (outcome.status === 'fulfilled') return 'ok'
  return String((outcome.reason as { code?: unknown }).code)
}

export function nextTurn(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

export async function eventually(check: () => Promise<boolean> | boolean, what: string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise(resolve => setTimeout(resolve, WAIT_STEP_MS))
  }
}
