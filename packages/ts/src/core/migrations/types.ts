import type { Transaction } from '../transaction.js'

export interface AppliedMigration {
  version: number
  name: string
  applied_at: number
}

export interface MigrationBaseline {
  through: number
}

export interface Migration {
  version: number
  name: string
  up: string | ((tx: Transaction) => void | Promise<void>)
  down?: string | ((tx: Transaction) => void | Promise<void>)
  baseline?: MigrationBaseline
}

export type MigrationSource = Migration[] | (() => Migration[] | Promise<Migration[]>)

export interface AppliedMigrationEntry {
  version: number
  name: string
}

export interface MigrationResult {
  applied: AppliedMigrationEntry[]
  skipped: number
}

export interface RollbackResult {
  rolledBack: AppliedMigrationEntry[]
}
