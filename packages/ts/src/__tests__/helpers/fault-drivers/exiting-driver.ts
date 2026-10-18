import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const EXIT_TABLE = 'sirannon_worker_exit_trigger'

export function exitingDriver(): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'exitingDriver' }
  return defineDriver({
    capabilities: base.capabilities,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const original = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        if (sql.includes(EXIT_TABLE)) process.exit(0)
        return original(sql)
      }
      return conn
    },
  })
}
