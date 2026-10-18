import { spawn } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const HISTORY_TABLE = '_sirannon_migrations'
export const USER_VERSION_CEILING = 2_147_483_647

export interface RecordedMigration {
  version: number
  name: string
  checksum: string | null
}

export interface FileState {
  userVersion: number
  history: RecordedMigration[]
  tables: string[]
}

let scratch: string | undefined
const registries: Sirannon[] = []

export function scratchPath(name: string): string {
  scratch ??= mkdtempSync(join(tmpdir(), 'sirannon-migrations-'))
  return join(scratch, name)
}

export function scratchDirectory(name: string): string {
  const path = scratchPath(name)
  mkdirSync(path, { recursive: true })
  return path
}

export function writeFiles(directory: string, files: Record<string, string>): void {
  for (const [name, contents] of Object.entries(files)) writeFileSync(join(directory, name), contents)
}

export function registry(options: Partial<SirannonOptions> = {}): Sirannon {
  const sirannon = new Sirannon({ driver: betterSqlite3(), ...options })
  registries.push(sirannon)
  return sirannon
}

export async function openDatabase(
  name = 'app',
  options?: DatabaseOptions,
  registryOptions: Partial<SirannonOptions> = {},
): Promise<{ db: Database; path: string }> {
  const path = scratchPath(`${name}.db`)
  const db = await registry(registryOptions).open(name, path, options)
  return { db, path }
}

export async function cleanUp(): Promise<void> {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
  if (scratch !== undefined) rmSync(scratch, { recursive: true, force: true })
  scratch = undefined
}

export function withRawConnection<T>(path: string, use: (raw: BetterSqlite3.Database) => T): T {
  const raw = new BetterSqlite3(path)
  try {
    return use(raw)
  } finally {
    raw.close()
  }
}

export function readFileState(path: string): FileState {
  return withRawConnection(path, raw => {
    const userVersion = raw.pragma('user_version', { simple: true }) as number
    const history = historyTableExists(path)
      ? (raw
          .prepare(`SELECT version, name, checksum FROM ${HISTORY_TABLE} ORDER BY version`)
          .all() as RecordedMigration[])
      : []
    const tables = (
      raw
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_sirannon\\_%' ESCAPE '\\' ORDER BY name",
        )
        .all() as { name: string }[]
    ).map(row => row.name)
    return { userVersion, history, tables }
  })
}

export function historyTableExists(path: string): boolean {
  return withRawConnection(
    path,
    raw =>
      raw.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(HISTORY_TABLE) !== undefined,
  )
}

export function recordedVersions(path: string): number[] {
  return readFileState(path).history.map(row => row.version)
}

export function columnsOf(path: string, table: string): string[] {
  return withRawConnection(path, raw =>
    (raw.prepare('SELECT name FROM pragma_table_info(?) ORDER BY cid').all(table) as { name: string }[]).map(
      row => row.name,
    ),
  )
}

export function countRows(path: string, table: string): number {
  return withRawConnection(path, raw => (raw.prepare(`SELECT count(*) AS n FROM "${table}"`).get() as { n: number }).n)
}

export interface MigratorReport {
  outcome: string
  applied?: number[]
  code?: string | null
  message?: string
}

export interface MigratorProcess {
  reports: MigratorReport[]
  ready: Promise<void>
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string }>
}

export function spawnMigrator(mode: string, args: string[]): MigratorProcess {
  const script = join(dirname(fileURLToPath(import.meta.url)), 'migrator-process.ts')
  const child = spawn(process.execPath, ['--import', 'tsx', script, mode, ...args], {
    cwd: process.cwd(),
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const reports: MigratorReport[] = []
  let stdout = ''
  let stderr = ''
  let markReady: () => void = () => {}
  const ready = new Promise<void>(resolve => {
    markReady = resolve
  })
  child.stdout.on('data', chunk => {
    stdout += String(chunk)
    const lines = stdout.split('\n')
    stdout = lines.pop() ?? ''
    for (const line of lines) {
      const report = JSON.parse(line) as MigratorReport
      reports.push(report)
      if (report.outcome === 'ready') markReady()
    }
  })
  child.stderr.on('data', chunk => {
    stderr += String(chunk)
  })
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string }>(resolve => {
    child.on('exit', (code, signal) => resolve({ code, signal, stderr }))
  })
  return { reports, ready, exited }
}

export function integrityOf(path: string): string {
  return withRawConnection(path, raw => raw.pragma('integrity_check', { simple: true }) as string)
}
