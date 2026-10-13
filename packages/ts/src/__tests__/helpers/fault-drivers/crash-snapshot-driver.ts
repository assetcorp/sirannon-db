import { copyFileSync } from 'node:fs'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export interface CrashSnapshotConfig {
  watchedPath: string
  snapshotPath: string
}

export function crashSnapshotDriver(config: CrashSnapshotConfig): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({
    ...base,
    open: async (path, options) => {
      const conn = await base.open(path, options)
      if (path !== config.watchedPath) return conn
      const close = conn.close.bind(conn)
      conn.close = async () => {
        await close()
        copyFileSync(config.watchedPath, config.snapshotPath)
      }
      return conn
    },
  })
}
