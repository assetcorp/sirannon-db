import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent, DatabaseOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { encodeHlc } from '../../e2e/lib/device-sync-oracle.js'

export const FAST_POLL_MS = 1
const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 8_000
export const NOTES_DDL = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'
export const SERVER_NODE = 'f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0'

export interface Workspace {
  path(name: string): string
  registry(driver?: SQLiteDriver): Sirannon
  open(name: string, options?: DatabaseOptions, driver?: SQLiteDriver): Promise<Database>
  openNotes(name: string, options?: DatabaseOptions, driver?: SQLiteDriver): Promise<Database>
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-device-sync-${label}-`))
  const registries: Sirannon[] = []
  const registry = (driver?: SQLiteDriver): Sirannon => {
    const sirannon = new Sirannon({ driver: driver ?? betterSqlite3() })
    registries.push(sirannon)
    return sirannon
  }
  const open = (name: string, options?: DatabaseOptions, driver?: SQLiteDriver) =>
    registry(driver).open(name, join(dir, `${name}.db`), options)
  return {
    path: name => join(dir, `${name}.db`),
    registry,
    open,
    openNotes: async (name, options, driver) => {
      const db = await open(name, options, driver)
      await db.execute(NOTES_DDL)
      await db.watch('notes')
      return db
    },
    dispose: async () => {
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

export function tableExists(path: string, table: string): boolean {
  return readRaw(path, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [table]).length === 1
}

export function metaValue(path: string, key: string): string | null {
  if (!tableExists(path, '_sirannon_meta')) return null
  const [row] = readRaw<{ value: string }>(path, 'SELECT value FROM _sirannon_meta WHERE key = ?', [key])
  return row?.value ?? null
}

export interface ChangeLogRow {
  seq: bigint
  table_name: string
  operation: string
  row_id: string
  node_id: string
  tx_id: string
  hlc: string
}

export function changeLog(path: string): ChangeLogRow[] {
  if (!tableExists(path, '_sirannon_changes')) return []
  return readRaw<ChangeLogRow>(
    path,
    'SELECT seq, table_name, operation, row_id, node_id, tx_id, hlc FROM _sirannon_changes ORDER BY seq',
  )
}

export function noteIds(path: string): number[] {
  return readRaw<{ id: bigint }>(path, 'SELECT id FROM notes ORDER BY id').map(row => Number(row.id))
}

export function stagedCount(path: string): number {
  if (!tableExists(path, '_sirannon_staged_changes')) return 0
  return Number(readRaw<{ n: bigint }>(path, 'SELECT count(*) AS n FROM _sirannon_staged_changes')[0].n)
}

export interface PulledChange {
  seq: number
  id: number
  body?: string
  txId: string
  txEnd?: boolean
  table?: string
  type?: ChangeEvent['type']
  hlc?: string
  origin?: string
}

export function pulled(change: PulledChange): ChangeEvent {
  const type = change.type ?? 'insert'
  const row = { id: change.id, body: change.body ?? `pulled ${change.id}` }
  return {
    type,
    table: change.table ?? 'notes',
    row: type === 'delete' ? {} : row,
    ...(type === 'insert' ? {} : { oldRow: row }),
    seq: BigInt(change.seq),
    timestamp: Date.now(),
    rowId: String(change.id),
    txId: change.txId,
    origin: change.origin ?? SERVER_NODE,
    hlc: change.hlc ?? encodeHlc(Date.now(), 0, change.origin ?? SERVER_NODE),
    ...(change.txEnd === true ? { txEnd: true } : {}),
  }
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
