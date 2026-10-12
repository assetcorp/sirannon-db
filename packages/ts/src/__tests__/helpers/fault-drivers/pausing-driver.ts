import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const PAUSE_MARKER = 'sirannon_pause_ms'

export function pausedSql(sql: string, pauseMs: number): string {
  return `${sql} /* ${PAUSE_MARKER}:${pauseMs} */`
}

function pause(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

function pauseRequestedBy(sql: string): number {
  const marked = sql.match(new RegExp(`${PAUSE_MARKER}:(\\d+) \\*/`))
  return marked ? Number(marked[1]) : 0
}

function pausingStatement(statement: SQLiteStatement, pauseMs: number): SQLiteStatement {
  const paused: SQLiteStatement = {
    all<T = unknown>(...params: unknown[]): Promise<T[]> {
      pause(pauseMs)
      return statement.all<T>(...params)
    },
    get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
      pause(pauseMs)
      return statement.get<T>(...params)
    },
    run(...params: unknown[]) {
      pause(pauseMs)
      return statement.run(...params)
    },
  }
  const allRaw = statement.allRaw?.bind(statement)
  if (allRaw) {
    paused.allRaw = <T = unknown>(...params: unknown[]): Promise<T[]> => {
      pause(pauseMs)
      return allRaw<T>(...params)
    }
  }
  return paused
}

export function pausingDriver(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: base.capabilities,
    ...(base.createWriterContext ? { createWriterContext: base.createWriterContext } : {}),
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const prepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        const statement = await prepare(sql)
        const pauseMs = pauseRequestedBy(sql)
        return pauseMs > 0 ? pausingStatement(statement, pauseMs) : statement
      }
      return conn
    },
  })
}
