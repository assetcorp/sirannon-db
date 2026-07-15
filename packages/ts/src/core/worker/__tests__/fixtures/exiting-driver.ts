import { betterSqlite3 } from '../../../../drivers/better-sqlite3/index.js'
import { defineDriver } from '../../../driver/define.js'
import type { SQLiteDriver } from '../../../driver/types.js'

export const EXIT_TABLE = 'sirannon_worker_exit_trigger'

export function exitingDriver(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: base.capabilities,
    worker: { specifier: import.meta.url, exportName: 'exitingDriver' },
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
