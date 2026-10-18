import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const SQLITE_HEADER_WRITE_VERSION_OFFSET = 18
const SQLITE_HEADER_READ_VERSION_OFFSET = 19
const SQLITE_ROLLBACK_FORMAT = 1
const SQLITE_WAL_FORMAT = 2
const WAL_HEADER_BYTES = 32
const WAL_FRAME_HEADER_BYTES = 24
const WAL_PAGE_SIZE_OFFSET = 8
const WAL_SALT_OFFSET = 16
const WAL_FRAME_COMMIT_SIZE_OFFSET = 4
const WAL_FRAME_SALT_OFFSET = 8
const WAL_SALT_BYTES = 8

export const SQLITE_SYNCHRONOUS = { off: 0n, normal: 1n, full: 2n, extra: 3n } as const
export const READINGS_SCHEMA =
  'CREATE TABLE readings (id INTEGER PRIMARY KEY, source TEXT NOT NULL, level INTEGER NOT NULL, value REAL)'
export const RECORD_LEVEL =
  'INSERT INTO readings (id, source, level, value) SELECT ?, ?, synchronous, ? FROM pragma_synchronous'

export interface WriterPlacement {
  label: string
  writerWorker: boolean
}

export const WRITER_PLACEMENTS: WriterPlacement[] = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

export interface Workspace {
  path(name: string): string
  registry(options?: Partial<SirannonOptions>): Sirannon
  open(name: string, options?: DatabaseOptions, registryOptions?: Partial<SirannonOptions>): Promise<Database>
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-bulk-load-${label}-`))
  const registries: Sirannon[] = []
  const registry = (options?: Partial<SirannonOptions>): Sirannon => {
    const sirannon = new Sirannon({ driver: betterSqlite3(), ...options })
    registries.push(sirannon)
    return sirannon
  }
  return {
    path: name => join(dir, `${name}.db`),
    registry,
    open: async (name, options, registryOptions) => {
      const db = await registry(registryOptions).open(name, join(dir, `${name}.db`), options)
      await db.execute(READINGS_SCHEMA)
      return db
    },
    dispose: async () => {
      for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export function rowsFrom(firstId: number, count: number, source: string): [number, string, number][] {
  return Array.from({ length: count }, (_, index) => [firstId + index, source, (firstId + index) / 4])
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

export function distinctLevels(path: string, source: string): bigint[] {
  return columnRaw(path, 'SELECT DISTINCT level FROM readings WHERE source = ? ORDER BY level', [source]) as bigint[]
}

export function countRaw(path: string): number {
  return Number(columnRaw(path, 'SELECT count(*) FROM readings')[0])
}

export function walCommitCount(path: string): number {
  const walPath = `${path}-wal`
  if (!existsSync(walPath)) return 0
  const wal = readFileSync(walPath)
  if (wal.length < WAL_HEADER_BYTES) return 0
  const pageSize = wal.readUInt32BE(WAL_PAGE_SIZE_OFFSET)
  const salt = wal.subarray(WAL_SALT_OFFSET, WAL_SALT_OFFSET + WAL_SALT_BYTES)
  let commits = 0
  for (
    let frame = WAL_HEADER_BYTES;
    frame + WAL_FRAME_HEADER_BYTES + pageSize <= wal.length;
    frame += WAL_FRAME_HEADER_BYTES + pageSize
  ) {
    const frameSalt = wal.subarray(frame + WAL_FRAME_SALT_OFFSET, frame + WAL_FRAME_SALT_OFFSET + WAL_SALT_BYTES)
    if (!frameSalt.equals(salt)) break
    if (wal.readUInt32BE(frame + WAL_FRAME_COMMIT_SIZE_OFFSET) !== 0) commits++
  }
  return commits
}

export function journalFormat(path: string): 'wal' | 'rollback' | 'unknown' {
  const header = readFileSync(path)
  const write = header[SQLITE_HEADER_WRITE_VERSION_OFFSET]
  const read = header[SQLITE_HEADER_READ_VERSION_OFFSET]
  if (write === SQLITE_WAL_FORMAT && read === SQLITE_WAL_FORMAT) return 'wal'
  if (write === SQLITE_ROLLBACK_FORMAT && read === SQLITE_ROLLBACK_FORMAT) return 'rollback'
  return 'unknown'
}

export function walBytes(path: string): number | null {
  const wal = `${path}-wal`
  return existsSync(wal) ? statSync(wal).size : null
}

export async function recordLevel(db: Database, id: number, source: string): Promise<void> {
  await db.execute(RECORD_LEVEL, [id, source, 0])
}
