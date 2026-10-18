import { threadId } from 'node:worker_threads'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const EXECUTING_THREAD_ID = 'sirannon_executing_thread_id'

function withExecutingThread(sql: string): string {
  return sql.replaceAll(EXECUTING_THREAD_ID, String(threadId))
}

export function threadRecordingDriver(): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'threadRecordingDriver' }
  return defineDriver({
    ...base,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const prepare = conn.prepare.bind(conn)
      const exec = conn.exec.bind(conn)
      conn.prepare = sql => prepare(withExecutingThread(sql))
      conn.exec = sql => exec(withExecutingThread(sql))
      const runBatch = conn.runBatch?.bind(conn)
      if (runBatch) conn.runBatch = (sql, paramsBatch) => runBatch(withExecutingThread(sql), paramsBatch)
      const runBatchSummary = conn.runBatchSummary?.bind(conn)
      if (runBatchSummary) {
        conn.runBatchSummary = (sql, paramsBatch) => runBatchSummary(withExecutingThread(sql), paramsBatch)
      }
      return conn
    },
  })
}
