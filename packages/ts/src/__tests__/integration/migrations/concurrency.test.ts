import { writeFileSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { execInterceptorDriver } from '../../helpers/fault-drivers/exec-interceptor-driver.js'
import {
  cleanUp,
  columnsOf,
  countRows,
  integrityOf,
  openDatabase,
  readFileState,
  registry,
  scratchPath,
  spawnMigrator,
  withRawConnection,
} from './harness.js'

afterEach(cleanUp)

const IMPATIENT_BUSY_TIMEOUT_MS = 25
const RACING_PROCESSES = 4
const IN_PROCESS_CALLERS = 8

const createAccounts: Migration = {
  version: 1,
  name: 'create_accounts',
  up: 'CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)',
  down: 'DROP TABLE accounts',
}

const addAccountPlan: Migration = {
  version: 2,
  name: 'add_account_plan',
  up: "ALTER TABLE accounts ADD COLUMN plan TEXT NOT NULL DEFAULT 'free'",
  down: 'ALTER TABLE accounts DROP COLUMN plan',
}

const ACCOUNT_SET = [createAccounts, addAccountPlan]

describe('a migrator that meets a held write lock', () => {
  it('refuses with MIGRATION_CONCURRENT after one retry, then applies once the other connection commits', async () => {
    const { db, path } = await openDatabase('accounts', undefined, {
      driver: betterSqlite3({ busyTimeout: IMPATIENT_BUSY_TIMEOUT_MS }),
    })
    await db.migrate([createAccounts])
    const holder = new BetterSqlite3(path)
    holder.exec('BEGIN IMMEDIATE')
    holder.exec("INSERT INTO accounts (email) VALUES ('holder@example.com')")

    try {
      await expect(db.migrate(ACCOUNT_SET)).rejects.toMatchObject({ code: 'MIGRATION_CONCURRENT' })
      await expect(db.rollback(ACCOUNT_SET, 0)).rejects.toMatchObject({ code: 'MIGRATION_CONCURRENT' })
      const blocked = registry({
        driver: betterSqlite3({ busyTimeout: IMPATIENT_BUSY_TIMEOUT_MS }),
        migrations: ACCOUNT_SET,
      })
      await expect(blocked.open('blocked', path)).rejects.toMatchObject({
        name: 'MigrationError',
        code: 'MIGRATION_CONCURRENT',
      })
      expect(blocked.has('blocked')).toBe(false)
    } finally {
      holder.exec('COMMIT')
      holder.close()
    }

    expect(await db.migrate(ACCOUNT_SET)).toEqual({ applied: [{ version: 2, name: 'add_account_plan' }], skipped: 1 })
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2])
    expect(withRawConnection(path, raw => raw.prepare('SELECT email, plan FROM accounts').all())).toEqual([
      { email: 'holder@example.com', plan: 'free' },
    ])
  })
})

describe('a migrator whose snapshot goes stale part-way', () => {
  it('retries with a fresh snapshot after another connection commits between its read and its write', async () => {
    const path = scratchPath('accounts.db')
    let interleaved = 0
    const driver = execInterceptorDriver(sql => {
      if (sql !== addAccountPlan.up) return
      interleaved++
      if (interleaved === 1) {
        withRawConnection(path, raw => raw.exec("INSERT INTO accounts (email) VALUES ('signup@example.com')"))
      }
    })
    const db = await registry({ driver }).open('accounts', path)
    await db.migrate([createAccounts])

    expect(await db.migrate(ACCOUNT_SET)).toEqual({ applied: [{ version: 2, name: 'add_account_plan' }], skipped: 1 })

    expect(interleaved).toBe(2)
    expect(withRawConnection(path, raw => raw.prepare('SELECT email, plan FROM accounts').all())).toEqual([
      { email: 'signup@example.com', plan: 'free' },
    ])
  })

  it('finds the rows of a second instance that applied the same migration first, and applies nothing twice', async () => {
    const path = scratchPath('accounts.db')
    const rival = await registry().open('rival', path)
    let interleaved = 0
    const driver = execInterceptorDriver(async sql => {
      if (sql !== addAccountPlan.up) return
      interleaved++
      if (interleaved === 1) await rival.migrate(ACCOUNT_SET)
    })
    const db = await registry({ driver }).open('accounts', path)
    await db.migrate([createAccounts])

    expect(await db.migrate(ACCOUNT_SET)).toEqual({ applied: [], skipped: 2 })

    expect(interleaved).toBe(1)
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2])
    expect(columnsOf(path, 'accounts')).toEqual(['id', 'email', 'plan'])
    expect(readFileState(path).userVersion).toBe(2)
  })
})

describe('many migrators at once', () => {
  it('lets exactly one of many concurrent migrate calls on one database apply each migration', async () => {
    const { db, path } = await openDatabase()

    const results = await Promise.all(Array.from({ length: IN_PROCESS_CALLERS }, () => db.migrate(ACCOUNT_SET)))

    expect(results.filter(result => result.applied.length > 0)).toEqual([
      {
        applied: [
          { version: 1, name: 'create_accounts' },
          { version: 2, name: 'add_account_plan' },
        ],
        skipped: 0,
      },
    ])
    expect(results.filter(result => result.applied.length === 0)).toHaveLength(IN_PROCESS_CALLERS - 1)
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2])
  })

  it('keeps one consistent history when several processes migrate one empty file at the same moment', async () => {
    const path = scratchPath('raced.db')
    withRawConnection(path, raw => raw.pragma('journal_mode = WAL'))
    const startFile = scratchPath('start')
    const migrators = Array.from({ length: RACING_PROCESSES }, () => spawnMigrator('race', [path, startFile]))
    await Promise.all(migrators.map(migrator => migrator.ready))

    writeFileSync(startFile, '')
    const exits = await Promise.all(migrators.map(migrator => migrator.exited))

    expect(exits.map(exit => exit.code)).toEqual(Array(RACING_PROCESSES).fill(0))
    const outcomes = migrators.map(migrator => migrator.reports.at(-1))
    for (const outcome of outcomes) {
      expect(
        [
          ['migrated', undefined],
          ['refused', 'MIGRATION_CONCURRENT'],
        ],
        JSON.stringify(outcome),
      ).toContainEqual([outcome?.outcome, outcome?.code])
    }
    const reportedApplied = outcomes.flatMap(outcome => outcome?.applied ?? [])
    expect(outcomes.some(outcome => outcome?.outcome === 'migrated')).toBe(true)
    expect(new Set(reportedApplied).size).toBe(reportedApplied.length)
    expect(reportedApplied.filter(version => ![1, 2, 3].includes(version))).toEqual([])
    expect(readFileState(path)).toMatchObject({ userVersion: 3, tables: ['accounts'] })
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2, 3])
    expect(columnsOf(path, 'accounts')).toEqual(['id', 'email', 'plan'])
    expect(countRows(path, 'accounts')).toBe(1)
    expect(integrityOf(path)).toBe('ok')
  }, 60_000)
})
