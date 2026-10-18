import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent, DatabaseOptions, Subscription, SubscriptionBuilder } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'

export const FAST_POLL_MS = 5
export const QUIET_PERIOD_MS = 150
const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 6_000

export interface Placement {
  label: string
  driver: () => SQLiteDriver
  writerWorker: boolean
}

export const PLACEMENTS: Placement[] = [
  { label: 'better-sqlite3 with writes on the calling thread', driver: () => betterSqlite3(), writerWorker: false },
  { label: 'better-sqlite3 with writes on the writer worker', driver: () => betterSqlite3(), writerWorker: true },
  { label: 'node:sqlite with writes on the calling thread', driver: () => nodeSqlite(), writerWorker: false },
  { label: 'node:sqlite with writes on the writer worker', driver: () => nodeSqlite(), writerWorker: true },
]

export interface Workspace {
  path(name: string): string
  registry(driver?: SQLiteDriver): Sirannon
  open(name: string, options?: DatabaseOptions, driver?: SQLiteDriver): Promise<Database>
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-change-capture-${label}-`))
  const registries: Sirannon[] = []
  const registry = (driver?: SQLiteDriver): Sirannon => {
    const sirannon = new Sirannon({ driver: driver ?? betterSqlite3() })
    registries.push(sirannon)
    return sirannon
  }
  return {
    path: name => join(dir, `${name}.db`),
    registry,
    open: (name, options, driver) =>
      registry(driver).open(name, join(dir, `${name}.db`), { cdcPollInterval: FAST_POLL_MS, ...options }),
    dispose: async () => {
      for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export interface Collected<T = Record<string, unknown>> {
  events: ChangeEvent<T>[]
  errors: Error[]
  subscription: Subscription
}

export function collect<T = Record<string, unknown>>(builder: SubscriptionBuilder): Collected<T> {
  const events: ChangeEvent<T>[] = []
  const errors: Error[] = []
  const subscription = builder.subscribe<T>(event => events.push(event), { onError: error => errors.push(error) })
  return { events, errors, subscription }
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await pause(WAIT_STEP_MS)
  }
}

export async function waitForCount(collected: { events: unknown[] }, count: number): Promise<void> {
  await waitFor(() => collected.events.length >= count, `${count} change events`)
  await pause(QUIET_PERIOD_MS)
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

export function writeRaw(path: string, sql: string, params: unknown[] = []): void {
  const raw = new BetterSqlite3(path, { fileMustExist: true })
  try {
    raw.pragma('busy_timeout = 5000')
    if (params.length === 0) raw.exec(sql)
    else raw.prepare(sql).run(...params)
  } finally {
    raw.close()
  }
}

export interface RejectionWatch {
  reasons: unknown[]
  stop(): void
}

export function watchUnhandledRejections(): RejectionWatch {
  const reasons: unknown[] = []
  const record = (reason: unknown) => reasons.push(reason)
  process.on('unhandledRejection', record)
  return { reasons, stop: () => process.off('unhandledRejection', record) }
}

export async function settleCallbacks(): Promise<void> {
  for (let turn = 0; turn < 3; turn++) await new Promise(resolve => setImmediate(resolve))
}
