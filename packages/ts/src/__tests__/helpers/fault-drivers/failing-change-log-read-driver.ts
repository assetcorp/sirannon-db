import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const CHANGE_LOG_RANGE_READ = /FROM "_sirannon_changes"\s+WHERE table_name IN/

export const CHANGE_LOG_READ_FAILURE = 'disk I/O error'

export interface FailingChangeLogReadDriver {
  driver: SQLiteDriver
  arm(): void
  disarm(): void
  failedReads(): number
}

export function failingChangeLogReadDriver(): FailingChangeLogReadDriver {
  const base = betterSqlite3()
  let armed = false
  let failed = 0

  function guarded<T>(read: () => Promise<T>): Promise<T> {
    if (!armed) return read()
    failed++
    return Promise.reject(new Error(CHANGE_LOG_READ_FAILURE))
  }

  function failing(statement: SQLiteStatement): SQLiteStatement {
    const replacement: SQLiteStatement = {
      all: <T = unknown>(...params: unknown[]) => guarded(() => statement.all<T>(...params)),
      get: <T = unknown>(...params: unknown[]) => statement.get<T>(...params),
      run: (...params: unknown[]) => statement.run(...params),
    }
    const allRaw = statement.allRaw?.bind(statement)
    if (allRaw) replacement.allRaw = <T = unknown>(...params: unknown[]) => guarded(() => allRaw<T>(...params))
    return replacement
  }

  const driver = defineDriver({
    capabilities: base.capabilities,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const prepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        const statement = await prepare(sql)
        return CHANGE_LOG_RANGE_READ.test(sql) ? failing(statement) : statement
      }
      return conn
    },
  })

  return {
    driver,
    arm: () => {
      armed = true
    },
    disarm: () => {
      armed = false
    },
    failedReads: () => failed,
  }
}
