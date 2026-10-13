import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'

export interface CopylessDriverConfig {
  base: 'better-sqlite3' | 'node:sqlite'
  declaresSteppedCopy: boolean
}

function withoutCopy(conn: SQLiteConnection): SQLiteConnection {
  const { copyDatabase: _copyDatabase, ...rest } = conn
  return rest
}

export function copylessDriver(config: CopylessDriverConfig): SQLiteDriver {
  const base = config.base === 'node:sqlite' ? nodeSqlite() : betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'copylessDriver', config }
  return defineDriver({
    ...base,
    capabilities: { ...base.capabilities, steppedCopy: config.declaresSteppedCopy },
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => withoutCopy(await base.open(path, options)),
  })
}
