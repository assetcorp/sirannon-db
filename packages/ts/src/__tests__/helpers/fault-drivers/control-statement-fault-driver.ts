import { existsSync, rmSync, writeFileSync } from 'node:fs'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { WriterWorker } from '../../../core/worker/host.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export type ControlStatementFault = 'commit-applies-then-fails' | 'savepoint-refused' | 'begin-refused'

export interface ControlStatementFaultConfig {
  fault: ControlStatementFault
  armFile: string
}

export const INJECTED_FAULT_MESSAGE = 'disk I/O error'

const FAULTED_KEYWORD: Record<ControlStatementFault, RegExp> = {
  'commit-applies-then-fails': /^\s*COMMIT\b/i,
  'savepoint-refused': /^\s*SAVEPOINT\b/i,
  'begin-refused': /^\s*BEGIN\b/i,
}

export function armControlStatementFault(config: ControlStatementFaultConfig): void {
  writeFileSync(config.armFile, '')
}

export function controlStatementFaultArmed(config: ControlStatementFaultConfig): boolean {
  return existsSync(config.armFile)
}

function takeArm(armFile: string): boolean {
  if (!existsSync(armFile)) return false
  rmSync(armFile, { force: true })
  return true
}

export function controlStatementFaultDriver(config: ControlStatementFaultConfig): SQLiteDriver {
  const base = betterSqlite3()
  const workerEntry = { specifier: import.meta.url, exportName: 'controlStatementFaultDriver', config }
  const faulted = FAULTED_KEYWORD[config.fault]
  const failsAfterRunning = config.fault === 'commit-applies-then-fails'
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
        if (!faulted.test(sql)) return exec(sql)
        if (failsAfterRunning) {
          await exec(sql)
          if (takeArm(config.armFile)) throw new Error(INJECTED_FAULT_MESSAGE)
          return
        }
        if (takeArm(config.armFile)) throw new Error(INJECTED_FAULT_MESSAGE)
        return exec(sql)
      }
      return conn
    },
  })
}
