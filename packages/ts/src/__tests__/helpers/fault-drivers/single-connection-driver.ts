import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export function singleConnectionDriver(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    capabilities: { ...base.capabilities, multipleConnections: false },
    open: (path, options) => base.open(path, options),
  })
}
