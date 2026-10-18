import { afterEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { MigrationError } from '../../../core/errors.js'
import type { Migration } from '../../../core/migrations/types.js'
import {
  cleanUp,
  historyTableExists,
  openDatabase,
  readFileState,
  USER_VERSION_CEILING,
  withRawConnection,
} from './harness.js'

afterEach(cleanUp)

const createInvoices: Migration = {
  version: 1,
  name: 'create_invoices',
  up: 'CREATE TABLE invoices (id INTEGER PRIMARY KEY)',
  down: 'DROP TABLE invoices',
}

function hostile(fields: Record<string, unknown>): Migration {
  return { version: 2, name: 'add_totals', up: 'CREATE TABLE totals (id INTEGER PRIMARY KEY)', ...fields } as Migration
}

async function expectRefusedUntouched(
  db: Database,
  path: string,
  set: Migration[],
  code = 'MIGRATION_VALIDATION_ERROR',
): Promise<void> {
  const outcome = db.migrate(set)
  await expect(
    outcome,
    JSON.stringify(set, (_, value) => (typeof value === 'bigint' ? `${value}n` : value)),
  ).rejects.toBeInstanceOf(MigrationError)
  await expect(outcome).rejects.toMatchObject({ code })
  expect(historyTableExists(path)).toBe(false)
  expect(readFileState(path)).toEqual({ userVersion: 0, history: [], tables: [] })
}

describe('migration versions', () => {
  it('refuses every version outside 1 to 2,147,483,647 before it writes anything', async () => {
    const { db, path } = await openDatabase()
    const versions: unknown[] = [
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      USER_VERSION_CEILING + 1,
      Number.MAX_SAFE_INTEGER + 1,
      '3',
      3n,
      null,
      undefined,
    ]

    for (const version of versions) {
      await expectRefusedUntouched(db, path, [createInvoices, hostile({ version })])
    }
  })

  it('accepts versions 1 and 2,147,483,647 and mirrors the highest into user_version', async () => {
    const { db, path } = await openDatabase()
    const ceiling = hostile({ version: USER_VERSION_CEILING, down: 'DROP TABLE totals' })

    expect((await db.migrate([ceiling, createInvoices])).applied.map(entry => entry.version)).toEqual([
      1,
      USER_VERSION_CEILING,
    ])

    expect(readFileState(path)).toMatchObject({ userVersion: USER_VERSION_CEILING, tables: ['invoices', 'totals'] })
    expect(await db.rollback([ceiling, createInvoices])).toEqual({
      rolledBack: [{ version: USER_VERSION_CEILING, name: 'add_totals' }],
    })
    expect(readFileState(path).userVersion).toBe(1)
  })
})

describe('migration names and bodies', () => {
  it('refuses every name outside letters, digits, and underscores, including one that is no string', async () => {
    const { db, path } = await openDatabase()
    const names: unknown[] = [
      '',
      'add-totals',
      'add totals',
      'totals;drop',
      "o'brien",
      'café',
      'line\nbreak',
      7,
      null,
      undefined,
      {},
    ]

    for (const name of names) {
      await expectRefusedUntouched(db, path, [createInvoices, hostile({ name })])
    }

    await db.migrate([hostile({ name: 'Add_Totals_2026' })])
    expect(readFileState(path).history.map(row => row.name)).toEqual(['Add_Totals_2026'])
  })

  it('refuses an up or a down that is empty, blank, or neither SQL text nor a function', async () => {
    const { db, path } = await openDatabase()
    const blanks: unknown[] = ['', '   ', '\n\t\r\n']
    const malformed: unknown[] = [null, 42, {}, ['CREATE TABLE totals (id)'], true]

    for (const up of [...blanks, ...malformed, undefined]) {
      await expectRefusedUntouched(db, path, [createInvoices, hostile({ up })])
    }
    for (const down of [...blanks, ...malformed]) {
      await expectRefusedUntouched(db, path, [createInvoices, hostile({ down })])
    }
  })

  it('refuses an entry that is no migration object, and a set with two migrations under one version', async () => {
    const { db, path } = await openDatabase()

    for (const entry of [null, undefined, 'CREATE TABLE totals (id)', 2]) {
      await expectRefusedUntouched(db, path, [createInvoices, entry as unknown as Migration])
    }
    await expectRefusedUntouched(
      db,
      path,
      [createInvoices, hostile({ version: 1, name: 'create_receipts' })],
      'MIGRATION_DUPLICATE_VERSION',
    )
    await expectRefusedUntouched(db, path, [createInvoices, { ...createInvoices }], 'MIGRATION_DUPLICATE_VERSION')
  })
})

describe('rollback targets', () => {
  it('refuses a target that is negative, fractional, or no number, and a malformed set, before it reverts anything', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([createInvoices])
    const before = readFileState(path)

    for (const target of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1, '0', null]) {
      await expect(db.rollback([createInvoices], target as number)).rejects.toMatchObject({
        code: 'MIGRATION_VALIDATION_ERROR',
      })
    }
    await expect(db.rollback([createInvoices, hostile({ name: 'bad-name' })], 0)).rejects.toMatchObject({
      code: 'MIGRATION_VALIDATION_ERROR',
    })

    expect(readFileState(path)).toEqual(before)
    expect(withRawConnection(path, raw => raw.prepare('SELECT count(*) AS n FROM invoices').get())).toEqual({ n: 0 })
  })
})
