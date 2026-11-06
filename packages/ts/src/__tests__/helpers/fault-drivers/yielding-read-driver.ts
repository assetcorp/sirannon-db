import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export interface YieldingReadConfig {
  sqliteBindingPath: string
  yieldMs: number
}

function afterTimers(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function yieldingStatement(statement: SQLiteStatement, yieldMs: number): SQLiteStatement {
  const yielding: SQLiteStatement = {
    async all<T = unknown>(...params: unknown[]): Promise<T[]> {
      await afterTimers(yieldMs)
      return statement.all<T>(...params)
    },
    async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
      await afterTimers(yieldMs)
      return statement.get<T>(...params)
    },
    async run(...params: unknown[]) {
      await afterTimers(yieldMs)
      return statement.run(...params)
    },
  }
  const allRaw = statement.allRaw?.bind(statement)
  if (allRaw) {
    yielding.allRaw = async <T = unknown>(...params: unknown[]): Promise<T[]> => {
      await afterTimers(yieldMs)
      return allRaw<T>(...params)
    }
  }
  return yielding
}

export function yieldingReadDriver(config: YieldingReadConfig): SQLiteDriver {
  const base = betterSqlite3({ encryption: true, sqliteBindingPath: config.sqliteBindingPath })
  return defineDriver({
    ...base,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      if (options?.readonly !== true) return conn
      const prepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        await afterTimers(config.yieldMs)
        return yieldingStatement(await prepare(sql), config.yieldMs)
      }
      return conn
    },
  })
}
