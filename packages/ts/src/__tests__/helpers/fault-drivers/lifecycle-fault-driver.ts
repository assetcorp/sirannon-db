import { resolve } from 'node:path'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const REFUSED_OPEN_MESSAGE = 'the volume refused to open this database'
export const FAILED_CLOSE_MESSAGE = 'the volume holding this database stopped answering'

export interface LifecycleFaultDriver {
  readonly driver: SQLiteDriver
  refuseOpen(path: string): void
  failClose(path: string): void
  heal(): void
}

export function lifecycleFaultDriver(): LifecycleFaultDriver {
  const base = betterSqlite3()
  let refusedOpen: string | null = null
  let failingClose: string | null = null
  return {
    driver: defineDriver({
      ...base,
      open: async (path, options) => {
        const target = resolve(path)
        if (target === refusedOpen) throw new Error(REFUSED_OPEN_MESSAGE)
        const conn = await base.open(path, options)
        const close = conn.close.bind(conn)
        conn.close = async () => {
          await close()
          if (target === failingClose) throw new Error(FAILED_CLOSE_MESSAGE)
        }
        return conn
      },
    }),
    refuseOpen: path => {
      refusedOpen = resolve(path)
    },
    failClose: path => {
      failingClose = resolve(path)
    },
    heal: () => {
      refusedOpen = null
      failingClose = null
    },
  }
}
