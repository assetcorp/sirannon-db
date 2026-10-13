import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver, SQLiteStatement } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const MARKED_READ_COMMENT = '/* sirannon_marked_read */'

const CHANGE_LOG_RANGE_READ = /FROM "_sirannon_changes"\s+WHERE table_name IN/

export interface MarkedReadDriver {
  driver: SQLiteDriver
  afterNextMarkedRead(action: () => Promise<void>): void
  failNextMarkedRead(message: string): void
  holdChangeLogRangeReads(release: Promise<void>): void
  heldChangeLogRangeReads(): number
  openConnections(): number
}

export function markedReadDriver(): MarkedReadDriver {
  const base = betterSqlite3()
  let afterRead: (() => Promise<void>) | null = null
  let failure: string | null = null
  let rangeReadRelease: Promise<void> | null = null
  let heldRangeReads = 0
  let opened = 0
  let closed = 0

  async function readMarked<T>(read: () => Promise<T>): Promise<T> {
    const failing = failure
    failure = null
    if (failing !== null) throw new Error(failing)
    const rows = await read()
    const action = afterRead
    afterRead = null
    if (action !== null) await action()
    return rows
  }

  async function readRange<T>(read: () => Promise<T>): Promise<T> {
    const release = rangeReadRelease
    if (release !== null) {
      heldRangeReads++
      try {
        await release
      } finally {
        heldRangeReads--
      }
    }
    return read()
  }

  function wrapped(statement: SQLiteStatement, around: <T>(read: () => Promise<T>) => Promise<T>): SQLiteStatement {
    const replacement: SQLiteStatement = {
      all: <T = unknown>(...params: unknown[]) => around(() => statement.all<T>(...params)),
      get: <T = unknown>(...params: unknown[]) => statement.get<T>(...params),
      run: (...params: unknown[]) => statement.run(...params),
    }
    const allRaw = statement.allRaw?.bind(statement)
    if (allRaw) replacement.allRaw = <T = unknown>(...params: unknown[]) => around(() => allRaw<T>(...params))
    return replacement
  }

  const driver = defineDriver({
    capabilities: base.capabilities,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      opened++
      const prepare = conn.prepare.bind(conn)
      conn.prepare = async (sql: string) => {
        const statement = await prepare(sql)
        if (sql.includes(MARKED_READ_COMMENT)) return wrapped(statement, readMarked)
        if (CHANGE_LOG_RANGE_READ.test(sql)) return wrapped(statement, readRange)
        return statement
      }
      const close = conn.close.bind(conn)
      conn.close = async () => {
        closed++
        await close()
      }
      return conn
    },
  })

  return {
    driver,
    afterNextMarkedRead: action => {
      afterRead = action
    },
    failNextMarkedRead: message => {
      failure = message
    },
    holdChangeLogRangeReads: release => {
      rangeReadRelease = release.finally(() => {
        rangeReadRelease = null
      })
    },
    heldChangeLogRangeReads: () => heldRangeReads,
    openConnections: () => opened - closed,
  }
}
