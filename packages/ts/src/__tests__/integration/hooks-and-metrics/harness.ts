import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'

export interface Settled {
  rejected: boolean
  value: unknown
}

export interface UnhandledRejections {
  reasons: unknown[]
  stop(): void
}

export interface ScratchDirectory {
  path(name: string): string
  remove(): void
}

export function scratchDirectory(prefix: string): ScratchDirectory {
  const root = mkdtempSync(join(tmpdir(), prefix))
  return {
    path: name => join(root, name),
    remove: () => rmSync(root, { recursive: true, force: true }),
  }
}

export function writeFile(path: string, script: string): void {
  const db = new BetterSqlite3(path)
  try {
    db.pragma('journal_mode = WAL')
    db.exec(script)
  } finally {
    db.close()
  }
}

export function readFile<T = Record<string, unknown>>(path: string, sql: string, params: unknown[] = []): T[] {
  const db = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    return db.prepare(sql).all(...params) as T[]
  } finally {
    db.close()
  }
}

export function tablesIn(path: string): string[] {
  return readFile<{ name: string }>(
    path,
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sirannon%' ESCAPE '\\' ORDER BY name",
  ).map(row => row.name)
}

export function settle(promise: Promise<unknown>): Promise<Settled> {
  return promise.then(
    value => ({ rejected: false, value }),
    (reason: unknown) => ({ rejected: true, value: reason }),
  )
}

export function watchUnhandledRejections(): UnhandledRejections {
  const reasons: unknown[] = []
  const record = (reason: unknown): void => {
    reasons.push(reason)
  }
  process.on('unhandledRejection', record)
  return { reasons, stop: () => process.off('unhandledRejection', record) }
}

export function elapse(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function waitUntil(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`condition unmet after ${timeoutMs} ms`)
    await elapse(10)
  }
}
