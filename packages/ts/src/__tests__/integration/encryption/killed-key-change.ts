import { appendFileSync } from 'node:fs'
import { join } from 'node:path'
import { defineDriver } from '../../../core/driver/define.js'
import type { DatabaseFileSystem } from '../../../core/driver/file-system.js'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { directoryDestination } from '../backups/harness.js'

export const KILLED_KEY_CHANGE_ENV = 'SIRANNON_KILLED_KEY_CHANGE'
export const NOT_KILLED = 'the key change ended before the kill point'
export const SEEDED_ENTRIES = 400
export const ENTRIES_AFTER_SNAPSHOT = 25

export const KILL_POINTS = [
  'writes',
  'export',
  'export-written',
  'marker-created',
  'first-round',
  'final-round',
  'replacement-begun',
  'checkpoint',
  'wal-removed',
  'between-delete-and-rename',
  'renamed',
  'marker-flushed',
  'marker-removed',
] as const

export type KillPoint = (typeof KILL_POINTS)[number]

const EXPORT_CALL = /^SELECT (sirannon|sqlcipher)_export\('/

export function secretOf(id: number): string {
  return `ledger-entry-${id.toString().padStart(6, '0')}-confidential`
}

function kill(): never {
  process.kill(process.pid, 'SIGKILL')
  throw new Error('SIGKILL did not stop the process')
}

function watchConnection(conn: SQLiteConnection, path: string, point: KillPoint): SQLiteConnection {
  const exec = conn.exec.bind(conn)
  const prepare = conn.prepare.bind(conn)
  const apply = conn.applyCapturedChanges?.bind(conn)
  conn.exec = async (sql: string) => {
    if (point === 'export-written' && sql.includes('.user_version =')) kill()
    if (point === 'final-round' && sql.includes('DELETE FROM main.sqlite_sequence')) kill()
    await exec(sql)
    if (path.endsWith('-marker') && point === 'replacement-begun' && sql.includes('replacement_begun = 1')) kill()
    if (
      path.endsWith('-marker') &&
      point === 'marker-flushed' &&
      !sql.includes('replacement_begun') &&
      sql.includes('writes + 1')
    )
      kill()
  }
  conn.prepare = async (sql: string) => {
    if (point === 'export' && EXPORT_CALL.test(sql)) kill()
    if (point === 'checkpoint' && sql.includes('wal_checkpoint(TRUNCATE)')) kill()
    return prepare(sql)
  }
  if (apply !== undefined && point === 'first-round') {
    conn.applyCapturedChanges = async () => kill()
  }
  return conn
}

function watchFiles(files: DatabaseFileSystem, dbPath: string, point: KillPoint): DatabaseFileSystem {
  let renamed = false
  return {
    readStart: (path, length) => files.readStart(path, length),
    async remove(path) {
      await files.remove(path)
      if (point === 'wal-removed' && path === `${dbPath}-wal`) kill()
      if (point === 'marker-removed' && renamed && path.endsWith('-marker')) kill()
    },
    async rename(from, to) {
      await files.remove(to)
      if (point === 'between-delete-and-rename') kill()
      await files.rename(from, to)
      renamed = true
      if (point === 'renamed') kill()
    },
  }
}

export function killingDriver(binding: string, dbPath: string, point: KillPoint): SQLiteDriver {
  const base = betterSqlite3({ encryption: true, sqliteBindingPath: binding })
  const files = base.files
  if (files === undefined) throw new Error('The better-sqlite3 driver supplies no file operations')
  return defineDriver({
    capabilities: base.capabilities,
    createBackupEngine: base.createBackupEngine,
    files: watchFiles(files, dbPath, point),
    async open(path, options) {
      if (point === 'marker-created' && path.endsWith('.sirannon-key-change')) kill()
      return watchConnection(await base.open(path, options), path, point)
    },
  })
}

export function ledgerBackups(backupsDir: string) {
  return {
    destination: directoryDestination(join(backupsDir, 'bucket')),
    intervalMs: 0,
    stagingDir: join(backupsDir, 'staging'),
  }
}

async function writeThenDie(
  dbPath: string,
  log: string,
  binding: string,
  point: KillPoint,
  keys: string,
  backupsDir: string | undefined,
): Promise<void> {
  const [oldKey, newKey] = keys.split(':').map(hex => Uint8Array.from(Buffer.from(hex, 'hex')))
  if (oldKey === undefined || newKey === undefined) throw new Error('Pass the old and new keys as hex')
  const sirannon = new Sirannon({ driver: killingDriver(binding, dbPath, point) })
  const db = await sirannon.open('ledger', dbPath, {
    encryption: { key: () => Uint8Array.from(oldKey) },
    ...(backupsDir === undefined ? {} : { backups: ledgerBackups(backupsDir) }),
  })
  await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, secret TEXT NOT NULL)')
  const write = async () => {
    const id = Number((await db.execute('INSERT INTO ledger (secret) VALUES (?)', ['pending'])).lastInsertRowId)
    await db.execute('UPDATE ledger SET secret = ? WHERE id = ?', [secretOf(id), id])
    appendFileSync(log, `${id}\n`)
  }
  for (let n = 0; n < SEEDED_ENTRIES; n++) await write()
  if (backupsDir !== undefined) await db.captureBackupChanges()
  if (point === 'writes') {
    for (let n = 0; ; n++) {
      await write()
      if (n === 50) kill()
    }
  }
  let calls = 0
  let writing = true
  const background = (async () => {
    while (writing) {
      await write()
      await new Promise<void>(resolve => setImmediate(resolve))
    }
  })()
  await db.changeEncryption({
    key: async () => {
      calls++
      if (calls === 2) for (let n = 0; n < ENTRIES_AFTER_SNAPSHOT; n++) await write()
      return Uint8Array.from(newKey)
    },
  })
  writing = false
  await background
  process.stdout.write(`${NOT_KILLED}\n`)
}

const [dbPath, log, binding, point, keys, backupsDir] = process.argv.slice(2)
if (process.env[KILLED_KEY_CHANGE_ENV] === '1' && dbPath && log && binding && point && keys) {
  await writeThenDie(dbPath, log, binding, point as KillPoint, keys, backupsDir)
}
