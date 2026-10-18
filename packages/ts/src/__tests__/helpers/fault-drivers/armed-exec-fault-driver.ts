import { existsSync, rmSync, writeFileSync } from 'node:fs'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export interface ArmedExecFaultConfig {
  statement: string
  armFile: string
}

export const ARMED_EXEC_FAULT_MESSAGE = 'disk I/O error'

export function armExecFault(config: ArmedExecFaultConfig): void {
  writeFileSync(config.armFile, '')
}

export function execFaultArmed(config: ArmedExecFaultConfig): boolean {
  return existsSync(config.armFile)
}

function sameStatement(left: string, right: string): boolean {
  return left.trim().toUpperCase() === right.trim().toUpperCase()
}

export function armedExecFaultDriver(config: ArmedExecFaultConfig): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'armedExecFaultDriver', config }
  return defineDriver({
    ...base,
    worker: workerEntry,
    startWriterHost: async (path, options, hostOptions) => {
      const host = await WriterWorker.start(workerEntry, path, options, hostOptions)
      return host.connection
    },
    open: async (path, options) => {
      const conn = await base.open(path, options)
      const exec = conn.exec.bind(conn)
      conn.exec = async (sql: string) => {
        if (sameStatement(sql, config.statement) && existsSync(config.armFile)) {
          rmSync(config.armFile, { force: true })
          throw new Error(ARMED_EXEC_FAULT_MESSAGE)
        }
        return exec(sql)
      }
      return conn
    },
  })
}
