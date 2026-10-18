import { existsSync, mkdtempSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'

const SQLITE_BUSY = 'SQLITE_BUSY'
const SQLITE_MAGIC = 'SQLite format 3\u0000'
const SQLITE_HEADER_BYTES = 100
const SQLITE_HEADER_WRITE_VERSION_OFFSET = 18
const SQLITE_ROLLBACK_FORMAT = 1
const SQLITE_WAL_FORMAT = 2

export function freshDirectory(label: string): string {
  return mkdtempSync(join(tmpdir(), `sirannon-registry-${label}-`))
}

export function seedOrders(path: string, totals: readonly number[]): void {
  const raw = new BetterSqlite3(path)
  try {
    raw.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
    const insert = raw.prepare('INSERT INTO orders (total) VALUES (?)')
    for (const total of totals) insert.run(total)
  } finally {
    raw.close()
  }
}

export function readTotals(path: string): number[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    return raw
      .prepare('SELECT total FROM orders ORDER BY id')
      .all()
      .map(row => (row as { total: number }).total)
  } finally {
    raw.close()
  }
}

export function journalFormat(path: string): 'wal' | 'rollback' {
  const header = readFileSync(path).subarray(0, SQLITE_HEADER_BYTES)
  if (header.subarray(0, SQLITE_MAGIC.length).toString('latin1') !== SQLITE_MAGIC) {
    throw new Error(`${path} is not a SQLite database file`)
  }
  const writeVersion = header[SQLITE_HEADER_WRITE_VERSION_OFFSET]
  if (writeVersion === SQLITE_WAL_FORMAT) return 'wal'
  if (writeVersion === SQLITE_ROLLBACK_FORMAT) return 'rollback'
  throw new Error(`${path} carries the unknown file format version ${writeVersion}`)
}

export function connectionsHeldOn(path: string): boolean {
  if (!existsSync(path)) return false
  const raw = new BetterSqlite3(path, { fileMustExist: true, timeout: 0 })
  try {
    const before = raw.pragma('journal_mode', { simple: true })
    try {
      raw.pragma('journal_mode = DELETE')
    } catch (err) {
      if ((err as { code?: string }).code === SQLITE_BUSY) return true
      throw err
    }
    raw.pragma(`journal_mode = ${String(before)}`)
    return false
  } finally {
    raw.close()
  }
}
