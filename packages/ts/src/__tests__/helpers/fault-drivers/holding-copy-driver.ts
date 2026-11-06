import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'

const ONE_PAGE = 1
const HELD = 0

export interface HoldingCopyConfig {
  runtime: 'better-sqlite3' | 'node:sqlite'
  countSql: string
  releaseAtCount: number
  encryptingBindingPath?: string
}

function baseDriver(config: HoldingCopyConfig): SQLiteDriver {
  if (config.runtime === 'node:sqlite') return nodeSqlite()
  if (config.encryptingBindingPath === undefined) return betterSqlite3()
  return betterSqlite3({ encryption: true, sqliteBindingPath: config.encryptingBindingPath })
}

function rowCounter(conn: SQLiteConnection, countSql: string): { latest: () => number; refresh: () => void } {
  let latest = 0
  let counting = false
  return {
    latest: () => latest,
    refresh: () => {
      if (counting) return
      counting = true
      conn
        .prepare(countSql)
        .then(statement => statement.get<{ n: unknown }>())
        .then(row => {
          latest = Number(row?.n ?? 0)
        })
        .catch(() => undefined)
        .finally(() => {
          counting = false
        })
    },
  }
}

export function holdingCopyDriver(config: HoldingCopyConfig): SQLiteDriver {
  const base = baseDriver(config)
  const workerEntry = { specifier: import.meta.url, exportName: 'holdingCopyDriver', config }
  return defineDriver({
    ...base,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const copyDatabase = conn.copyDatabase?.bind(conn)
      if (!copyDatabase) return conn
      conn.copyDatabase = request => {
        const rows = rowCounter(conn, config.countSql)
        let steps = 0
        return copyDatabase({
          ...request,
          pagesPerStep: ONE_PAGE,
          onStep: step => {
            steps++
            const holding = step.remainingPages > 0 && rows.latest() < config.releaseAtCount
            if (!holding) return request.onStep?.(step) ?? request.pagesPerStep
            rows.refresh()
            if (steps > 1) return HELD
            request.onStep?.(step)
            return ONE_PAGE
          },
        })
      }
      return conn
    },
  })
}
