import { basename } from 'node:path'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const FAILING_CLOSE_FILE_MARKER = 'fails-to-close'

export function failingCloseDriver(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: base.capabilities,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      if (!basename(path).includes(FAILING_CLOSE_FILE_MARKER)) return conn
      const close = conn.close.bind(conn)
      conn.close = async () => {
        await close()
        throw new Error('the volume holding this database stopped answering')
      }
      return conn
    },
  })
}
