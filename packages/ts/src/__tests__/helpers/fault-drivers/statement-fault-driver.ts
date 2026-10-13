import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const STATEMENT_FAULT_MESSAGE = 'disk I/O error'

const FAIL_EVERY_CALL = -1
const DISARMED = 0

export interface StatementFault {
  failCallWithinEachTransaction(sqlFragment: string, call: number): void
  failEveryCall(sqlFragment: string): void
  disarm(): void
  failures(): number
}

export interface StatementFaultDriver {
  driver: SQLiteDriver
  fault: StatementFault
}

export function statementFaultDriver(): StatementFaultDriver {
  const base = betterSqlite3()
  let target: string | null = null
  let failingCall = DISARMED
  let callsInTransaction = 0
  let failures = 0

  const driver = defineDriver({
    capabilities: base.capabilities,
    ...(base.createWriterContext ? { createWriterContext: base.createWriterContext } : {}),
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const exec = conn.exec.bind(conn)
      const prepare = conn.prepare.bind(conn)
      conn.exec = async (sql: string) => {
        if (/^\s*BEGIN\b/i.test(sql)) callsInTransaction = 0
        return exec(sql)
      }
      conn.prepare = async (sql: string) => {
        const stmt = await prepare(sql)
        if (target === null || !sql.includes(target)) return stmt
        const run = stmt.run.bind(stmt)
        stmt.run = async (...params: unknown[]) => {
          callsInTransaction += 1
          if (failingCall === FAIL_EVERY_CALL || callsInTransaction === failingCall) {
            failures += 1
            throw new Error(STATEMENT_FAULT_MESSAGE)
          }
          return run(...params)
        }
        return stmt
      }
      return conn
    },
  })

  return {
    driver,
    fault: {
      failCallWithinEachTransaction(sqlFragment, call) {
        target = sqlFragment
        failingCall = call
        callsInTransaction = 0
      },
      failEveryCall(sqlFragment) {
        target = sqlFragment
        failingCall = FAIL_EVERY_CALL
        callsInTransaction = 0
      },
      disarm() {
        target = null
        failingCall = DISARMED
        callsInTransaction = 0
      },
      failures: () => failures,
    },
  }
}
