import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const CHANGE_LOG_PRUNE_PREFIX = 'DELETE FROM "_sirannon_changes"'

export function pruneRacingDriver(racingWrite: string): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: base.capabilities,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const originalPrepare = conn.prepare.bind(conn)
      let raced = false
      conn.prepare = async (sql: string): Promise<SQLiteStatement> => {
        const stmt = await originalPrepare(sql)
        if (!sql.startsWith(CHANGE_LOG_PRUNE_PREFIX)) return stmt
        return {
          ...stmt,
          run: async (...params: unknown[]) => {
            if (!raced) {
              raced = true
              await conn.exec(racingWrite)
            }
            return stmt.run(...params)
          },
        }
      }
      return conn
    },
  })
}
