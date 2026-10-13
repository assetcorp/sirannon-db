import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { type BetterSqlite3Options, betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const CHECKPOINT_PRAGMA = /^\s*PRAGMA\s+wal_checkpoint\b/i

export function checkpointObservingDriver(beforeCheckpoint: () => void, options?: BetterSqlite3Options): SQLiteDriver {
  const base = betterSqlite3(options)
  return defineDriver({
    capabilities: base.capabilities,
    ...(base.createWriterContext ? { createWriterContext: base.createWriterContext } : {}),
    open: async (path, openOptions) => {
      const conn = await base.open(path, openOptions)
      const prepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string): Promise<SQLiteStatement> => {
        const statement = await prepare(sql)
        if (!CHECKPOINT_PRAGMA.test(sql)) return statement
        return {
          ...statement,
          get: <T = unknown>(...params: unknown[]) => {
            beforeCheckpoint()
            return statement.get<T>(...params)
          },
        }
      }
      return conn
    },
  })
}
