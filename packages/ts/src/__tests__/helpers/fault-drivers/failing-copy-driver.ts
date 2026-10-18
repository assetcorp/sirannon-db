import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const COPY_FAILURE_MESSAGE = 'disk I/O error'
export const PAGE_SIZE_FAILURE_MESSAGE = 'the page size could not be read'

export interface FailingCopyDriverConfig {
  failAtStep?: number
  stallFirstStepMs?: number
  refusePageSize?: boolean
}

function pause(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

export function failingCopyDriver(config: FailingCopyDriverConfig): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'failingCopyDriver', config }
  return defineDriver({
    ...base,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      if (config.refusePageSize) {
        const prepare = conn.prepare.bind(conn)
        conn.prepare = async (sql: string) => {
          if (/^\s*PRAGMA\s+page_size\s*$/i.test(sql)) throw new Error(PAGE_SIZE_FAILURE_MESSAGE)
          return prepare(sql)
        }
      }
      const copyDatabase = conn.copyDatabase?.bind(conn)
      if (!copyDatabase) return conn
      conn.copyDatabase = request => {
        let steps = 0
        return copyDatabase({
          ...request,
          onStep: step => {
            steps++
            if (steps === 1 && config.stallFirstStepMs !== undefined) pause(config.stallFirstStepMs)
            if (config.failAtStep !== undefined && steps >= config.failAtStep && step.remainingPages > 0) {
              throw new Error(COPY_FAILURE_MESSAGE)
            }
            return request.onStep?.(step) ?? request.pagesPerStep
          },
        })
      }
      return conn
    },
  })
}
