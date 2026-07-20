import { betterSqlite3 } from '../../../../drivers/better-sqlite3/index.js'
import { defineDriver } from '../../../driver/define.js'
import type { SQLiteDriver } from '../../../driver/types.js'
import { WriterWorker } from '../../host.js'

const SLEEP_MARKER = 'sirannon_worker_sleep_ms'

export function sleepSql(ms: number): string {
  return `SELECT 1 /* ${SLEEP_MARKER}:${ms} */`
}

function sleepIfMarked(sql: string): void {
  const match = sql.match(new RegExp(`${SLEEP_MARKER}:(\\d+)`))
  if (!match) return
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Number(match[1]))
}

export function sleepingDriver(): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'sleepingDriver' }
  return defineDriver({
    capabilities: base.capabilities,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const originalPrepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        sleepIfMarked(sql)
        return originalPrepare(sql)
      }
      const originalExec = conn.exec.bind(conn)
      conn.exec = async (sql: string) => {
        sleepIfMarked(sql)
        return originalExec(sql)
      }
      return conn
    },
  })
}
