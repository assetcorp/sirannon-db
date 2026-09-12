import { betterSqlite3 } from '../../../../drivers/better-sqlite3/index.js'
import { defineDriver } from '../../../driver/define.js'
import type { SQLiteDriver } from '../../../driver/types.js'
import { WriterWorker } from '../../host.js'

export const COPY_STALL_MS = 4_000

function pause(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

export function stallingCopyDriver(): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'stallingCopyDriver' }
  return defineDriver({
    ...base,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const copyDatabase = conn.copyDatabase?.bind(conn)
      if (!copyDatabase) return conn
      conn.copyDatabase = request => {
        let stalledOnce = false
        return copyDatabase({
          ...request,
          onStep: step => {
            if (!stalledOnce) {
              stalledOnce = true
              pause(COPY_STALL_MS)
            }
            return request.onStep?.(step) ?? request.pagesPerStep
          },
        })
      }
      return conn
    },
  })
}
