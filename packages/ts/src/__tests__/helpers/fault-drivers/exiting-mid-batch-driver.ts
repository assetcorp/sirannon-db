import { isMainThread } from 'node:worker_threads'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const MID_BATCH_EXIT_MARKER = 'sirannon_worker_exits_mid_batch'

export function exitsMidBatch(sql: string): string {
  return `${sql} /* ${MID_BATCH_EXIT_MARKER} */`
}

function exitHalfwayThrough(sql: string): boolean {
  if (!sql.includes(MID_BATCH_EXIT_MARKER)) return false
  if (isMainThread)
    throw new Error('exitingMidBatchDriver ends the writer worker thread and refuses to end the main thread')
  return true
}

export function exitingMidBatchDriver(): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'exitingMidBatchDriver' }
  return defineDriver({
    ...base,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const runBatch = conn.runBatch?.bind(conn)
      const runBatchSummary = conn.runBatchSummary?.bind(conn)
      if (runBatch) {
        conn.runBatch = async (sql, paramsBatch) => {
          if (!exitHalfwayThrough(sql)) return runBatch(sql, paramsBatch)
          await runBatch(sql, paramsBatch.slice(0, Math.ceil(paramsBatch.length / 2)))
          process.exit(1)
        }
      }
      if (runBatchSummary) {
        conn.runBatchSummary = async (sql, paramsBatch) => {
          if (!exitHalfwayThrough(sql)) return runBatchSummary(sql, paramsBatch)
          await runBatchSummary(sql, paramsBatch.slice(0, Math.ceil(paramsBatch.length / 2)))
          process.exit(1)
        }
      }
      return conn
    },
  })
}
