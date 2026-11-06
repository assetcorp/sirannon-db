import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const ENCRYPTED_EXIT_TABLE = 'sirannon_encrypted_worker_exit_trigger'

export function encryptedExitingDriver(sqliteBindingPath: string): SQLiteDriver {
  const base = betterSqlite3({ encryption: true, sqliteBindingPath })
  const workerEntry = { specifier: import.meta.url, exportName: 'encryptedExitingDriver', config: sqliteBindingPath }
  return defineDriver({
    capabilities: base.capabilities,
    ...(base.files === undefined ? {} : { files: base.files }),
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const original = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        if (sql.includes(ENCRYPTED_EXIT_TABLE)) process.exit(0)
        return original(sql)
      }
      return conn
    },
  })
}
