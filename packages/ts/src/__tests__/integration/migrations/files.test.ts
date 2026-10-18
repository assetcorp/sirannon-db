import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { MigrationError } from '../../../core/errors.js'
import { migrationsFromFiles } from '../../../core/migrations/from-files.js'
import { loadMigrations, scanDirectory } from '../../../utils/file-migrations/index.js'
import {
  cleanUp,
  columnsOf,
  openDatabase,
  readFileState,
  recordedVersions,
  scratchDirectory,
  USER_VERSION_CEILING,
  writeFiles,
} from './harness.js'

afterEach(cleanUp)

const CREATE_INVOICES = 'CREATE TABLE invoices (id INTEGER PRIMARY KEY)'
const ADD_TOTAL = 'ALTER TABLE invoices ADD COLUMN total INTEGER'
const ADD_REFUNDED = 'ALTER TABLE invoices ADD COLUMN refunded INTEGER'

function expectRefused(build: () => unknown, label: string, code: string, message?: RegExp): void {
  let refusal: unknown
  try {
    build()
  } catch (err) {
    refusal = err
  }
  expect(refusal, label).toBeInstanceOf(MigrationError)
  expect(refusal, label).toMatchObject({ code, ...(message ? { message: expect.stringMatching(message) } : {}) })
}

function expectLoadRefused(directory: string, code: string): void {
  expectRefused(() => loadMigrations(directory), directory, code)
}

function expectMapRefused(files: Record<string, unknown>, code: string, message?: RegExp): void {
  expectRefused(() => migrationsFromFiles(files), JSON.stringify(files), code, message)
}

describe('a directory of migration files', () => {
  it('loads the files in numeric version order and ignores every file of another form', async () => {
    const directory = scratchDirectory('migrations')
    writeFiles(directory, {
      '10_add_refunded.up.sql': ADD_REFUNDED,
      '2_add_total.up.sql': `\uFEFF${ADD_TOTAL}\r\n`,
      '001_create_invoices.up.sql': `\n${CREATE_INVOICES}\n`,
      '001_create_invoices.down.sql': 'DROP TABLE invoices',
      'README.md': '# migrations',
      'notes.sql': 'DROP TABLE invoices',
      '3_draft.UP.sql': 'DROP TABLE invoices',
      'x_draft.up.sql': 'DROP TABLE invoices',
      '4-draft.up.sql': 'DROP TABLE invoices',
      '5_draft.up.sql.bak': 'DROP TABLE invoices',
      '6_draft.sideways.sql': 'DROP TABLE invoices',
    })
    mkdirSync(join(directory, '7_nested.up.sql'))

    const loaded = loadMigrations(directory)

    expect(loaded.map(migration => [migration.version, migration.name, migration.up, migration.down])).toEqual([
      [1, 'create_invoices', CREATE_INVOICES, undefined],
      [2, 'add_total', ADD_TOTAL, undefined],
      [10, 'add_refunded', ADD_REFUNDED, undefined],
    ])
    expect(scanDirectory(directory)).toEqual([
      {
        version: 1,
        name: 'create_invoices',
        upPath: join(directory, '001_create_invoices.up.sql'),
        downPath: join(directory, '001_create_invoices.down.sql'),
      },
      { version: 2, name: 'add_total', upPath: join(directory, '2_add_total.up.sql'), downPath: null },
      { version: 10, name: 'add_refunded', upPath: join(directory, '10_add_refunded.up.sql'), downPath: null },
    ])
    const { db, path } = await openDatabase()
    expect((await db.migrate(loaded)).applied.map(entry => entry.version)).toEqual([1, 2, 10])
    expect(columnsOf(path, 'invoices')).toEqual(['id', 'total', 'refunded'])
    expect(readFileState(path).userVersion).toBe(10)
  })

  it('refuses a directory path with a traversal segment or a control character, and one that names no directory', () => {
    const directory = scratchDirectory('migrations')
    writeFiles(directory, { '1_create_invoices.up.sql': CREATE_INVOICES, 'plain.txt': 'not a directory' })

    for (const unsafe of [
      '../migrations',
      'migrations/../../etc',
      `${directory}/../migrations`,
      'C:\\app\\migrations\\..\\..\\secrets',
      `${directory}\0`,
      `${directory}\n`,
      `${directory}\u001b`,
    ]) {
      expectLoadRefused(unsafe, 'MIGRATION_VALIDATION_ERROR')
    }
    expectLoadRefused(join(directory, 'missing'), 'MIGRATION_ERROR')
    expectLoadRefused(join(directory, 'plain.txt'), 'MIGRATION_ERROR')
  })

  it('refuses two files under one version, a version without an up file, an empty up file, and a version out of range', async () => {
    const cases: { files: Record<string, string>; code: string }[] = [
      {
        files: { '1_create_invoices.up.sql': CREATE_INVOICES, '1_create_orders.up.sql': CREATE_INVOICES },
        code: 'MIGRATION_DUPLICATE_VERSION',
      },
      {
        files: { '1_create_invoices.up.sql': CREATE_INVOICES, '001_create_invoices.up.sql': 'DROP TABLE invoices' },
        code: 'MIGRATION_DUPLICATE_VERSION',
      },
      {
        files: { '1_create_invoices.down.sql': 'DROP TABLE invoices', '01_create_invoices.down.sql': 'SELECT 1' },
        code: 'MIGRATION_DUPLICATE_VERSION',
      },
      { files: { '1_create_invoices.down.sql': 'DROP TABLE invoices' }, code: 'MIGRATION_VALIDATION_ERROR' },
      { files: { '1_create_invoices.up.sql': ' \r\n\t' }, code: 'MIGRATION_VALIDATION_ERROR' },
      { files: { '0_create_invoices.up.sql': CREATE_INVOICES }, code: 'MIGRATION_VALIDATION_ERROR' },
      { files: { '000_create_invoices.up.sql': CREATE_INVOICES }, code: 'MIGRATION_VALIDATION_ERROR' },
      { files: { '99999999999999999999_create_invoices.up.sql': CREATE_INVOICES }, code: 'MIGRATION_VALIDATION_ERROR' },
    ]
    for (const [index, { files, code }] of cases.entries()) {
      const directory = scratchDirectory(`case-${index}`)
      writeFiles(directory, files)
      expectLoadRefused(directory, code)
    }

    const edge = scratchDirectory('ceiling')
    writeFiles(edge, { [`${USER_VERSION_CEILING}_create_invoices.up.sql`]: CREATE_INVOICES })
    const past = scratchDirectory('past-ceiling')
    writeFiles(past, { [`${USER_VERSION_CEILING + 1}_create_invoices.up.sql`]: CREATE_INVOICES })
    const { db, path } = await openDatabase()
    await expect(db.migrate(loadMigrations(past))).rejects.toMatchObject({ code: 'MIGRATION_VALIDATION_ERROR' })
    await db.migrate(loadMigrations(edge))
    expect(readFileState(path).userVersion).toBe(USER_VERSION_CEILING)
  })

  it('marks the baseline that the options name, for a directory and for a bundled map alike', async () => {
    const directory = scratchDirectory('squashed')
    const files = {
      '1_create_invoices.up.sql': CREATE_INVOICES,
      '2_add_total.up.sql': ADD_TOTAL,
      '3_squashed.up.sql': `${CREATE_INVOICES};\n${ADD_TOTAL}`,
      '4_add_refunded.up.sql': ADD_REFUNDED,
    }
    writeFiles(directory, files)
    const baseline = { version: 3, through: 2 }
    const sources = [loadMigrations(directory, { baseline }), migrationsFromFiles(files, { baseline })]

    for (const [index, set] of sources.entries()) {
      const fresh = await openDatabase(`fresh-${index}`)
      const existing = await openDatabase(`existing-${index}`)
      await existing.db.migrate(set.slice(0, 2).map(migration => ({ ...migration })))

      expect((await fresh.db.migrate(set)).applied.map(entry => entry.version)).toEqual([3, 4])
      expect((await existing.db.migrate(set)).applied.map(entry => entry.version)).toEqual([4])
      expect(columnsOf(fresh.path, 'invoices')).toEqual(['id', 'total', 'refunded'])
      expect(recordedVersions(existing.path)).toEqual([1, 2, 4])
    }

    const missing = { baseline: { version: 9, through: 2 } }
    expectRefused(() => loadMigrations(directory, missing), 'directory', 'MIGRATION_VALIDATION_ERROR')
    expectRefused(() => migrationsFromFiles(files, missing), 'map', 'MIGRATION_VALIDATION_ERROR')
  })
})

describe('rollback from migration files', () => {
  it('reverts with each .down.sql file, which it reads at rollback time', async () => {
    const directory = scratchDirectory('migrations')
    writeFiles(directory, {
      '1_create_invoices.up.sql': CREATE_INVOICES,
      '1_create_invoices.down.sql': 'DROP TABLE invoices',
      '2_create_ledgers.up.sql':
        'CREATE TABLE ledgers (id INTEGER PRIMARY KEY);\nCREATE TABLE ledger_lines (id INTEGER PRIMARY KEY)',
      '2_create_ledgers.down.sql': 'DROP TABLE ledger_lines;\nDROP TABLE ledgers;',
      '3_add_total.up.sql': ADD_TOTAL,
      '3_add_total.down.sql': 'ALTER TABLE invoices DROP COLUMN total',
    })
    const set = loadMigrations(directory)
    const { db, path } = await openDatabase()
    await db.migrate(set)

    expect((await db.rollback(set)).rolledBack.map(entry => entry.version)).toEqual([3])
    expect(columnsOf(path, 'invoices')).toEqual(['id'])
    expect((await db.rollback(set, 1)).rolledBack.map(entry => entry.version)).toEqual([2])
    expect(readFileState(path)).toMatchObject({ userVersion: 1, tables: ['invoices'] })

    writeFiles(directory, { '1_create_invoices.down.sql': 'DROP TABLE invoices; CREATE TABLE rollback_marker (id)' })
    expect((await db.rollback(set, 0)).rolledBack.map(entry => entry.version)).toEqual([1])
    expect(readFileState(path)).toEqual({ userVersion: 0, history: [], tables: ['rollback_marker'] })

    expect((await db.migrate(set)).applied.map(entry => entry.version)).toEqual([1, 2, 3])
  })

  it('refuses a version whose down file is missing, blank, or a directory when the rollback reads it, and reverts nothing', async () => {
    const cases: { label: string; tamper: (downPath: string) => void; code: string }[] = [
      { label: 'absent', tamper: downPath => rmSync(downPath), code: 'MIGRATION_NO_DOWN' },
      { label: 'emptied', tamper: downPath => writeFileSync(downPath, ' \n '), code: 'MIGRATION_VALIDATION_ERROR' },
      {
        label: 'directory',
        tamper: downPath => {
          rmSync(downPath)
          mkdirSync(downPath)
        },
        code: 'MIGRATION_ROLLBACK_ERROR',
      },
    ]

    for (const { label, tamper, code } of cases) {
      const directory = scratchDirectory(label)
      writeFiles(directory, {
        '1_create_invoices.up.sql': CREATE_INVOICES,
        '1_create_invoices.down.sql': 'DROP TABLE invoices',
        '2_add_total.up.sql': ADD_TOTAL,
        '2_add_total.down.sql': 'ALTER TABLE invoices DROP COLUMN total',
      })
      const set = loadMigrations(directory)
      const { db, path } = await openDatabase(label)
      await db.migrate(set)
      const before = readFileState(path)
      tamper(join(directory, '2_add_total.down.sql'))

      await expect(db.rollback(set, 0), label).rejects.toMatchObject({ code, version: 2 })
      expect(readFileState(path)).toEqual(before)
      expect(columnsOf(path, 'invoices')).toEqual(['id', 'total'])
    }

    const neverHadDown = scratchDirectory('never')
    writeFiles(neverHadDown, {
      '1_create_invoices.up.sql': CREATE_INVOICES,
      '2_add_total.up.sql': ADD_TOTAL,
      '2_add_total.down.sql': 'ALTER TABLE invoices DROP COLUMN total',
    })
    const set = loadMigrations(neverHadDown)
    const { db, path } = await openDatabase('never')
    await db.migrate(set)
    await expect(db.rollback(set, 0)).rejects.toMatchObject({ code: 'MIGRATION_NO_DOWN', version: 1 })
    expect(recordedVersions(path)).toEqual([1, 2])
    expect((await db.rollback(set)).rolledBack.map(entry => entry.version)).toEqual([2])
  })
})

describe('migrations built from a bundled map of files', () => {
  it('parses only the final path segment, trims the SQL, sorts by version, and applies the result', async () => {
    const set = migrationsFromFiles({
      './src/db/migrations/002_add_total.up.sql': `\n  ${ADD_TOTAL}\n`,
      'C:\\app\\migrations\\001_create_invoices.up.sql': CREATE_INVOICES,
      '/var/bundle/001_create_invoices.down.sql': 'DROP TABLE invoices',
      '10_add_refunded.up.sql': ADD_REFUNDED,
    })

    expect(set).toEqual([
      { version: 1, name: 'create_invoices', up: CREATE_INVOICES, down: 'DROP TABLE invoices' },
      { version: 2, name: 'add_total', up: ADD_TOTAL },
      { version: 10, name: 'add_refunded', up: ADD_REFUNDED },
    ])
    const { db, path } = await openDatabase()
    await db.migrate(set)
    expect(columnsOf(path, 'invoices')).toEqual(['id', 'total', 'refunded'])
    expect(migrationsFromFiles({})).toEqual([])
  })

  it('refuses a malformed map, and tells a bundler user to import the raw text', () => {
    const rawGuidance = /query: '\?raw'/
    for (const value of [{ default: CREATE_INVOICES }, undefined, null, 42, [CREATE_INVOICES]]) {
      expectMapRefused({ '001_create_invoices.up.sql': value }, 'MIGRATION_VALIDATION_ERROR', rawGuidance)
    }
    for (const key of [
      './migrations/create-invoices.up.sql',
      'migrations/001_create_invoices.up.sql/',
      '001_create_invoices.sql',
      '001_create invoices.up.sql',
      '0_create_invoices.up.sql',
      '-1_create_invoices.up.sql',
    ]) {
      expectMapRefused({ [key]: CREATE_INVOICES }, 'MIGRATION_VALIDATION_ERROR')
    }
    expectMapRefused({ '001_create_invoices.up.sql': ' \n\t' }, 'MIGRATION_VALIDATION_ERROR')
    expectMapRefused(
      { '001_create_invoices.down.sql': 'DROP TABLE invoices' },
      'MIGRATION_VALIDATION_ERROR',
      /missing an \.up\.sql file/,
    )
    expectMapRefused(
      { '001_create_invoices.up.sql': CREATE_INVOICES, '001_create_orders.up.sql': CREATE_INVOICES },
      'MIGRATION_DUPLICATE_VERSION',
    )
    expectMapRefused(
      { './a/001_create_invoices.up.sql': CREATE_INVOICES, './b/1_create_invoices.up.sql': CREATE_INVOICES },
      'MIGRATION_DUPLICATE_VERSION',
    )
  })
})
