import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { type BetterSqlite3Options, betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export type ExecInterceptor = (sql: string) => void | Promise<void>

export function execInterceptorDriver(beforeExec: ExecInterceptor, options?: BetterSqlite3Options): SQLiteDriver {
  const base = betterSqlite3(options)
  return defineDriver({
    capabilities: base.capabilities,
    open: async (path, openOptions) => {
      const conn = await base.open(path, openOptions)
      const originalExec = conn.exec.bind(conn)
      conn.exec = async (sql: string) => {
        await beforeExec(sql)
        return originalExec(sql)
      }
      return conn
    },
  })
}
