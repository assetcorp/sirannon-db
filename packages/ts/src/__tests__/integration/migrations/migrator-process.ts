import { existsSync } from 'node:fs'
import { SirannonError } from '../../../core/errors.js'
import type { Migration } from '../../../core/migrations/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const START_POLL_MS = 2
const ROWS_WRITTEN_BEFORE_KILL = 500

const RACED_SET: Migration[] = [
  {
    version: 1,
    name: 'create_accounts',
    up: 'CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)',
  },
  { version: 2, name: 'add_account_plan', up: "ALTER TABLE accounts ADD COLUMN plan TEXT NOT NULL DEFAULT 'free'" },
  {
    version: 3,
    name: 'seed_operator',
    up: async tx => {
      await tx.execute("INSERT INTO accounts (email, plan) VALUES ('ops@example.com', 'staff')")
      await tx.execute('CREATE INDEX accounts_by_plan ON accounts (plan)')
    },
  },
]

const KILLED_SET: Migration[] = [
  { version: 1, name: 'create_ledger', up: 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)' },
  {
    version: 2,
    name: 'backfill_ledger',
    up: async tx => {
      for (let row = 0; row < ROWS_WRITTEN_BEFORE_KILL; row++) {
        await tx.execute('INSERT INTO ledger (amount) VALUES (?)', [row])
      }
      await tx.execute('CREATE TABLE ledger_totals (total INTEGER NOT NULL)')
      process.stdout.write(`${JSON.stringify({ outcome: 'writing' })}\n`)
      process.kill(process.pid, 'SIGKILL')
    },
  },
]

function report(message: Record<string, unknown>): void {
  process.stdout.write(`${JSON.stringify(message)}\n`)
}

async function waitFor(path: string): Promise<void> {
  while (!existsSync(path)) await new Promise(resolve => setTimeout(resolve, START_POLL_MS))
}

async function race(dbPath: string, startFile: string): Promise<void> {
  report({ outcome: 'ready' })
  await waitFor(startFile)
  const sirannon = new Sirannon({ driver: betterSqlite3() })
  try {
    const db = await sirannon.open('raced', dbPath)
    const result = await db.migrate(RACED_SET)
    report({ outcome: 'migrated', applied: result.applied.map(entry => entry.version) })
  } catch (err) {
    report({
      outcome: 'refused',
      code: err instanceof SirannonError ? err.code : null,
      message: err instanceof Error ? err.message : String(err),
    })
  } finally {
    await sirannon.shutdown()
  }
}

async function killMidMigration(dbPath: string): Promise<void> {
  const sirannon = new Sirannon({ driver: betterSqlite3(), migrations: KILLED_SET })
  await sirannon.open('killed', dbPath)
  report({ outcome: 'survived' })
}

const [mode, dbPath, startFile] = process.argv.slice(2)
if (mode === 'race' && dbPath !== undefined && startFile !== undefined) {
  await race(dbPath, startFile)
} else if (mode === 'kill' && dbPath !== undefined) {
  await killMidMigration(dbPath)
} else {
  report({ outcome: 'unknown-mode', mode })
  process.exitCode = 2
}
