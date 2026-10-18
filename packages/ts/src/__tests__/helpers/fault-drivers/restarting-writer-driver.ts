import { isMainThread } from 'node:worker_threads'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const WRITER_THREAD_EXIT_TABLE = 'sirannon_writer_thread_exit'

export function restartingWriterDriver(): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'restartingWriterDriver' }
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
      conn.prepare = async (sql: string) => {
        if (!isMainThread && sql.includes(WRITER_THREAD_EXIT_TABLE)) process.exit(0)
        return prepare(sql)
      }
      return conn
    },
  })
}
