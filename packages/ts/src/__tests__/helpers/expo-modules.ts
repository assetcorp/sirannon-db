import { existsSync, readFileSync, renameSync, rmSync, statSync } from 'node:fs'
import { createRequire } from 'node:module'
import type StockDatabase from 'better-sqlite3'

const require = createRequire(import.meta.url)
const NAMED_PREFIXES = [':', '@', '$']

interface Statement {
  readonly reader: boolean
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint }
  all(...params: unknown[]): unknown[]
  get(...params: unknown[]): unknown
}

export const expoMoveSteps: { afterDelete: (() => void) | null } = { afterDelete: null }

function bindArgs(params: unknown): unknown[] {
  if (params === undefined) return []
  if (Array.isArray(params)) return params
  const named: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(params as Record<string, unknown>)) {
    if (!NAMED_PREFIXES.includes(key[0] ?? '')) named[key] = value
  }
  return [named]
}

function handleOf(db: StockDatabase.Database) {
  const statement = (sql: string): Statement => db.prepare(sql) as unknown as Statement
  const handle = {
    async execAsync(sql: string) {
      db.exec(sql)
    },
    async getAllAsync(sql: string, params?: unknown) {
      const stmt = statement(sql)
      if (!stmt.reader) {
        stmt.run(...bindArgs(params))
        return []
      }
      return stmt.all(...bindArgs(params))
    },
    async getFirstAsync(sql: string, params?: unknown) {
      const stmt = statement(sql)
      if (!stmt.reader) {
        stmt.run(...bindArgs(params))
        return null
      }
      return stmt.get(...bindArgs(params)) ?? null
    },
    async runAsync(sql: string, params?: unknown) {
      const info = statement(sql).run(...bindArgs(params))
      return { changes: info.changes, lastInsertRowId: Number(info.lastInsertRowid) }
    },
    async withExclusiveTransactionAsync(task: (tx: unknown) => Promise<void>) {
      db.exec('BEGIN EXCLUSIVE')
      try {
        await task(handle)
        db.exec('COMMIT')
      } catch (err) {
        if (db.inTransaction) db.exec('ROLLBACK')
        throw err
      }
    },
    async closeAsync() {
      db.close()
    },
  }
  return handle
}

export const expoSqlcipherBuild: { binding: string | null } = { binding: null }

export function expoSqliteModule() {
  const BetterSqlite = require('better-sqlite3') as typeof StockDatabase
  return {
    defaultDatabaseDirectory: '/',
    async openDatabaseAsync(name: string, options?: { readOnly?: boolean }) {
      const binding = expoSqlcipherBuild.binding
      if (binding === null) throw new Error('Set expoSqlcipherBuild.binding to a built expo-sqlite SQLCipher first')
      const db = new BetterSqlite(name, { readonly: options?.readOnly === true, nativeBinding: binding })
      db.unsafeMode(true)
      return handleOf(db)
    },
  }
}

function pathOf(uri: string): string {
  if (!uri.startsWith('file://')) throw new Error(`expo-file-system takes file URIs, not ${uri}`)
  return uri.slice('file://'.length).split('/').map(decodeURIComponent).join('/')
}

export function expoFileSystemLegacyModule() {
  return {
    EncodingType: { UTF8: 'utf8', Base64: 'base64' },
    async getInfoAsync(uri: string) {
      const path = pathOf(uri)
      if (!existsSync(path)) return { exists: false, isDirectory: false, uri }
      return { exists: true, isDirectory: false, uri, size: statSync(path).size }
    },
    async readAsStringAsync(uri: string, options: { encoding: string; position: number; length: number }) {
      const bytes = readFileSync(pathOf(uri))
      if (bytes.length === 0) throw new Error('Android reads -1 bytes from an empty file and fails to encode them')
      return bytes.subarray(options.position, options.position + options.length).toString('base64')
    },
    async deleteAsync(uri: string, options?: { idempotent?: boolean }) {
      const path = pathOf(uri)
      if (!existsSync(path) && options?.idempotent !== true) throw new Error(`${path} does not exist`)
      rmSync(path, { force: true })
    },
    async moveAsync(options: { from: string; to: string }) {
      const to = pathOf(options.to)
      rmSync(to, { force: true })
      expoMoveSteps.afterDelete?.()
      renameSync(pathOf(options.from), to)
    },
  }
}
