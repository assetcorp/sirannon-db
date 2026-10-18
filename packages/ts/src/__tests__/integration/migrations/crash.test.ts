import { afterEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import { execInterceptorDriver } from '../../helpers/fault-drivers/exec-interceptor-driver.js'
import {
  cleanUp,
  countRows,
  integrityOf,
  readFileState,
  registry,
  scratchPath,
  spawnMigrator,
  withRawConnection,
} from './harness.js'

afterEach(cleanUp)

const createLedger: Migration = {
  version: 1,
  name: 'create_ledger',
  up: 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)',
  down: 'DROP TABLE ledger',
}

const backfillLedger: Migration = {
  version: 2,
  name: 'backfill_ledger',
  up: async tx => {
    await tx.execute('INSERT INTO ledger (amount) VALUES (10), (20), (30)')
  },
  down: 'DELETE FROM ledger',
}

function expectConsistent(path: string): void {
  const state = readFileState(path)
  const versions = state.history.map(row => row.version)
  expect([[], [1]]).toContainEqual(versions)
  expect(state.tables).toEqual(versions.length === 0 ? [] : ['ledger'])
  expect(state.userVersion).toBe(versions.at(-1) ?? 0)
  if (versions.length > 0) expect(countRows(path, 'ledger')).toBe(0)
  expect(integrityOf(path)).toBe('ok')
}

describe('a migration that stops part-way', () => {
  it('leaves no trace of a migration whose process dies by SIGKILL mid-write, and applies the full set on the next open', async () => {
    const path = scratchPath('ledger.db')
    const killed = spawnMigrator('kill', [path])

    const exit = await killed.exited

    expect(exit.signal, exit.stderr).toBe('SIGKILL')
    expect(killed.reports.map(report => report.outcome)).toEqual(['writing'])
    expectConsistent(path)

    const reopened = await registry({ migrations: [createLedger, backfillLedger] }).open('ledger', path)

    expect(await reopened.appliedMigrations()).toHaveLength(2)
    expect(readFileState(path)).toMatchObject({ userVersion: 2, tables: ['ledger'] })
    expect(withRawConnection(path, raw => raw.prepare('SELECT amount FROM ledger ORDER BY id').pluck().all())).toEqual([
      10, 20, 30,
    ])
  }, 30_000)

  it('leaves the file unchanged when SQLite cannot commit a migration or a rollback, and succeeds on the next attempt', async () => {
    const path = scratchPath('ledger.db')
    let failCommits = false
    const driver = execInterceptorDriver(sql => {
      if (failCommits && sql === 'COMMIT') {
        failCommits = false
        throw Object.assign(new Error('database or disk is full'), { code: 'SQLITE_FULL' })
      }
    })
    const failing = registry({ driver, migrations: [createLedger, backfillLedger] })

    failCommits = true
    await expect(failing.open('ledger', path)).rejects.toMatchObject({
      code: 'DATABASE_OPEN_FAILED',
      message: expect.stringContaining('database or disk is full'),
    })
    expect(failing.has('ledger')).toBe(false)
    expectConsistent(path)

    const db = await failing.open('ledger', path)
    expect(countRows(path, 'ledger')).toBe(3)

    failCommits = true
    await expect(db.rollback([createLedger, backfillLedger], 0)).rejects.toThrow('database or disk is full')
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2])
    expect(countRows(path, 'ledger')).toBe(3)

    expect((await db.rollback([createLedger, backfillLedger], 0)).rolledBack.map(entry => entry.version)).toEqual([
      2, 1,
    ])
    expect(readFileState(path)).toEqual({ userVersion: 0, history: [], tables: [] })
  })
})
