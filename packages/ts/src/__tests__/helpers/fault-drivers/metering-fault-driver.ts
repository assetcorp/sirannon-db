import { existsSync, writeFileSync } from 'node:fs'
import { isMainThread } from 'node:worker_threads'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const HOLD_MARKER = 'metering_hold_after_sqlite'
const CRASH_MARKER = 'metering_crash_after_sqlite'
const HOLD_POLL_MS = 5
const REACHED_SUFFIX = '.reached'

export interface MeteringFaultDriverConfig {
  sqliteBindingPath: string
}

export function heldAfterSqlite(sql: string, releaseFile: string): string {
  return `${sql} /* ${HOLD_MARKER}:${releaseFile} */`
}

export function crashesAfterSqlite(sql: string): string {
  return `${sql} /* ${CRASH_MARKER} */`
}

export function heldStatementReached(releaseFile: string): boolean {
  return existsSync(`${releaseFile}${REACHED_SUFFIX}`)
}

export function releaseHeldStatement(releaseFile: string): void {
  writeFileSync(releaseFile, '')
}

function pause(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

function refuseMainThread(sql: string): void {
  if (isMainThread) {
    throw new Error(`meteringFaultDriver acts only on the writer worker thread, and '${sql}' reached the main thread`)
  }
}

function marked(sql: string): boolean {
  return sql.includes(HOLD_MARKER) || sql.includes(CRASH_MARKER)
}

function actAfterSqlite(sql: string): void {
  if (sql.includes(CRASH_MARKER)) {
    refuseMainThread(sql)
    process.exit(1)
  }
  const releaseFile = sql.match(new RegExp(`${HOLD_MARKER}:(.+?) \\*/`))?.[1]
  if (releaseFile === undefined) return
  refuseMainThread(sql)
  writeFileSync(`${releaseFile}${REACHED_SUFFIX}`, '')
  while (!existsSync(releaseFile)) pause(HOLD_POLL_MS)
}

function returnAfterSqlite<V>(sql: string, value: V): V {
  actAfterSqlite(sql)
  return value
}

function faultingStatement(sql: string, statement: SQLiteStatement): SQLiteStatement {
  const faulting: SQLiteStatement = {
    all: async <T = unknown>(...params: unknown[]) => returnAfterSqlite(sql, await statement.all<T>(...params)),
    get: async <T = unknown>(...params: unknown[]) => returnAfterSqlite(sql, await statement.get<T>(...params)),
    run: async (...params: unknown[]) => returnAfterSqlite(sql, await statement.run(...params)),
  }
  const allRaw = statement.allRaw?.bind(statement)
  const allCounted = statement.allCounted?.bind(statement)
  const getCounted = statement.getCounted?.bind(statement)
  const allRawCounted = statement.allRawCounted?.bind(statement)
  if (allRaw) {
    faulting.allRaw = async <T = unknown>(...params: unknown[]) => returnAfterSqlite(sql, await allRaw<T>(...params))
  }
  if (allCounted) {
    faulting.allCounted = async <T = unknown>(...params: unknown[]) =>
      returnAfterSqlite(sql, await allCounted<T>(...params))
  }
  if (getCounted) {
    faulting.getCounted = async <T = unknown>(...params: unknown[]) =>
      returnAfterSqlite(sql, await getCounted<T>(...params))
  }
  if (allRawCounted) {
    faulting.allRawCounted = async <T = unknown>(...params: unknown[]) =>
      returnAfterSqlite(sql, await allRawCounted<T>(...params))
  }
  return faulting
}

export function meteringFaultDriver(config: MeteringFaultDriverConfig): SQLiteDriver {
  const base = betterSqlite3({ rowCounts: true, sqliteBindingPath: config.sqliteBindingPath })
  const workerEntry = { specifier: import.meta.url, exportName: 'meteringFaultDriver', config }
  return defineDriver({
    capabilities: base.capabilities,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, { ...hostOptions, countsRows: true })
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const prepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        const statement = await prepare(sql)
        return marked(sql) ? faultingStatement(sql, statement) : statement
      }
      const runBatch = conn.runBatch?.bind(conn)
      if (runBatch) {
        conn.runBatch = async (sql, paramsBatch) => {
          if (!sql.includes(CRASH_MARKER)) return returnAfterSqlite(sql, await runBatch(sql, paramsBatch))
          refuseMainThread(sql)
          await runBatch(sql, paramsBatch.slice(0, Math.ceil(paramsBatch.length / 2)))
          process.exit(1)
        }
      }
      const runBatchSummary = conn.runBatchSummary?.bind(conn)
      if (runBatchSummary) {
        conn.runBatchSummary = async (sql, paramsBatch) =>
          returnAfterSqlite(sql, await runBatchSummary(sql, paramsBatch))
      }
      return conn
    },
  })
}
