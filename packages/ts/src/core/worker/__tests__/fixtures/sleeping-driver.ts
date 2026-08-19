import { existsSync, writeFileSync } from 'node:fs'
import { betterSqlite3 } from '../../../../drivers/better-sqlite3/index.js'
import { defineDriver } from '../../../driver/define.js'
import type { SQLiteDriver } from '../../../driver/types.js'
import { WriterWorker } from '../../host.js'

const HOLD_MARKER = 'sirannon_worker_hold_until_file'
const HOLD_POLL_MS = 25

export function heldSql(sql: string, releaseFile: string): string {
  return `${sql} /* ${HOLD_MARKER}:${releaseFile} */`
}

export function releaseHeldWrite(releaseFile: string): void {
  writeFileSync(releaseFile, '')
}

function pause(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

function pauseIfMarked(sql: string): void {
  const hold = sql.match(new RegExp(`${HOLD_MARKER}:(.+?) \\*/`))
  const releaseFile = hold?.[1]
  if (!releaseFile) return
  while (!existsSync(releaseFile)) pause(HOLD_POLL_MS)
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
        pauseIfMarked(sql)
        return originalPrepare(sql)
      }
      const originalExec = conn.exec.bind(conn)
      conn.exec = async (sql: string) => {
        pauseIfMarked(sql)
        return originalExec(sql)
      }
      return conn
    },
  })
}
