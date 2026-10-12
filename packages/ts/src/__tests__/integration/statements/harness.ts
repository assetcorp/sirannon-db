import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'

const SQLITE_MAGIC = 'SQLite format 3\u0000'
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

export interface DriverCase {
  label: string
  driver: () => SQLiteDriver
}

export const DRIVERS: DriverCase[] = [
  { label: 'better-sqlite3', driver: () => betterSqlite3() },
  { label: 'node:sqlite', driver: () => nodeSqlite() },
]

export interface WriterPlacement {
  label: string
  writerWorker: boolean
}

export const WRITER_PLACEMENTS: WriterPlacement[] = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

export const DRIVER_PLACEMENTS = DRIVERS.flatMap(driverCase =>
  WRITER_PLACEMENTS.map(placement => ({
    label: `${driverCase.label}, ${placement.label}`,
    driver: driverCase.driver,
    writerWorker: placement.writerWorker,
  })),
)

export interface Workspace {
  path(name: string): string
  registry(options?: Partial<SirannonOptions>): Sirannon
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-statements-${label}-`))
  const registries: Sirannon[] = []
  return {
    path: name => join(dir, name),
    registry: options => {
      const sirannon = new Sirannon({ driver: betterSqlite3(), ...options })
      registries.push(sirannon)
      return sirannon
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

export function countRaw(path: string, table: string): number {
  const [row] = readRaw<{ n: bigint }>(path, `SELECT count(*) AS n FROM "${table}"`)
  return Number(row?.n ?? -1n)
}

export function columnRaw(path: string, sql: string, params: unknown[] = []): unknown[] {
  return readRaw<Record<string, unknown>>(path, sql, params).map(row => Object.values(row)[0])
}

export function journalFormat(path: string): 'wal' | 'rollback' {
  const header = readFileSync(path)
  if (header.subarray(0, SQLITE_MAGIC.length).toString('latin1') !== SQLITE_MAGIC) {
    throw new Error(`${path} is not a SQLite database file`)
  }
  const write = header[SQLITE_HEADER_WRITE_VERSION_OFFSET]
  const read = header[SQLITE_HEADER_READ_VERSION_OFFSET]
  if (write === SQLITE_WAL_FORMAT && read === SQLITE_WAL_FORMAT) return 'wal'
  if (write === SQLITE_ROLLBACK_FORMAT && read === SQLITE_ROLLBACK_FORMAT) return 'rollback'
  throw new Error(`${path} carries the file format versions ${write} and ${read}`)
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

export function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => {}
  const promise = new Promise<void>(settle => {
    resolve = settle
  })
  return { promise, resolve }
}
