import { afterEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import {
  cleanUp,
  columnsOf,
  countRows,
  openDatabase,
  readFileState,
  registry,
  scratchPath,
  withRawConnection,
} from './harness.js'

afterEach(cleanUp)

const INVOICES_SCRIPT = 'CREATE TABLE invoices (id INTEGER PRIMARY KEY);\nCREATE INDEX invoices_by_id ON invoices (id)'
const INVOICES_SCRIPT_FNV1A = '4a6dafe615790dc8'
const CAFES_SQL = 'CREATE TABLE cafés (name TEXT NOT NULL)'
const CAFES_SQL_FNV1A = 'ad368aa57460f747'
const SEED_CAFES_SQL = "INSERT INTO cafés (name) VALUES ('crème brûlée')"
const SEED_CAFES_SQL_FNV1A = 'c2ec046a06753ba7'
const REFUNDS_SQL = 'CREATE TABLE refunds (id INTEGER PRIMARY KEY, invoice_id INTEGER REFERENCES invoices (id))'
const REFUNDS_SQL_FNV1A = 'f830460d7bfb707e'

const LEGACY_HISTORY_TABLE = `CREATE TABLE _sirannon_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
)`

function invoices(up: string = INVOICES_SCRIPT): Migration {
  return { version: 1, name: 'create_invoices', up }
}

const seedInvoices: Migration = {
  version: 2,
  name: 'seed_invoices',
  up: async tx => {
    await tx.execute('INSERT INTO invoices (id) VALUES (1), (2)')
  },
}

const refunds: Migration = { version: 3, name: 'create_refunds', up: REFUNDS_SQL }

describe('the checksum of each applied migration', () => {
  it('stores the 64-bit FNV-1a hash of each normalised SQL up, and no hash for a function up', async () => {
    const { db, path } = await openDatabase()
    const set: Migration[] = [
      invoices(`\r\n  ${INVOICES_SCRIPT.replace(/\n/g, '\r\n')}\t\n `),
      seedInvoices,
      { version: 3, name: 'create_cafes', up: `\n\n${CAFES_SQL}` },
      { version: 4, name: 'seed_cafes', up: SEED_CAFES_SQL },
    ]

    await db.migrate(set)

    const recorded = readFileState(path).history
    expect(recorded).toEqual([
      { version: 1, name: 'create_invoices', checksum: INVOICES_SCRIPT_FNV1A },
      { version: 2, name: 'seed_invoices', checksum: null },
      { version: 3, name: 'create_cafes', checksum: CAFES_SQL_FNV1A },
      { version: 4, name: 'seed_cafes', checksum: SEED_CAFES_SQL_FNV1A },
    ])
    expect(await db.appliedMigrations()).toEqual(recorded)

    const lone = await openDatabase('lone-carriage-returns')
    await lone.db.migrate([invoices(INVOICES_SCRIPT.replace(/\n/g, '\r'))])
    expect(readFileState(lone.path).history[0].checksum).toBe(INVOICES_SCRIPT_FNV1A)
  })

  it('refuses an up whose text changed after the database applied it, and accepts whitespace and line-ending edits', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([invoices()])

    for (const edited of [
      INVOICES_SCRIPT.replace('INTEGER PRIMARY KEY', 'INTEGER  PRIMARY KEY'),
      INVOICES_SCRIPT.toLowerCase(),
      INVOICES_SCRIPT.replace(';\n', ';\n\n'),
      `${INVOICES_SCRIPT};`,
    ]) {
      await expect(db.migrate([invoices(edited), refunds])).rejects.toMatchObject({
        code: 'MIGRATION_CHECKSUM_MISMATCH',
        version: 1,
      })
      expect(readFileState(path).tables).toEqual(['invoices'])
    }

    for (const reformatted of [
      INVOICES_SCRIPT.replace(/\n/g, '\r\n'),
      INVOICES_SCRIPT.replace(/\n/g, '\r'),
      `\n\t  ${INVOICES_SCRIPT}  \r\n`,
    ]) {
      expect(await db.migrate([invoices(reformatted)])).toEqual({ applied: [], skipped: 1 })
    }
    expect((await db.migrate([invoices(), refunds])).applied.map(entry => entry.version)).toEqual([3])
    expect(readFileState(path).history.map(row => row.checksum)).toEqual([INVOICES_SCRIPT_FNV1A, REFUNDS_SQL_FNV1A])
  })

  it('verifies only the SQL migrations that the set still holds, and skips function migrations and rows that the set leaves out', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([invoices(), seedInvoices])
    const rewrittenSeed: Migration = {
      ...seedInvoices,
      up: async tx => {
        await tx.execute('INSERT INTO invoices (id) VALUES (99)')
      },
    }

    expect(await db.migrate([invoices(), rewrittenSeed])).toEqual({ applied: [], skipped: 2 })
    expect((await db.migrate([refunds])).applied.map(entry => entry.version)).toEqual([3])

    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2, 3])
    expect(countRows(path, 'invoices')).toBe(2)
  })
})

describe('migrate on a history from a release before checksums', () => {
  it('adds the checksum column with every row kept, then backfills and enforces each SQL hash', async () => {
    const path = scratchPath('legacy.db')
    withRawConnection(path, raw => {
      raw.exec(LEGACY_HISTORY_TABLE)
      raw.exec(INVOICES_SCRIPT)
      raw.exec('INSERT INTO invoices (id) VALUES (1), (2)')
      const record = raw.prepare('INSERT INTO _sirannon_migrations (version, name, applied_at) VALUES (?, ?, ?)')
      record.run(1, 'create_invoices', 1_700_000_000.25)
      record.run(2, 'seed_invoices', 1_700_000_060.5)
    })
    const db = await registry().open('legacy', path)

    expect((await db.migrate([invoices(), seedInvoices, refunds])).applied.map(entry => entry.version)).toEqual([3])

    expect(columnsOf(path, '_sirannon_migrations')).toEqual(['version', 'name', 'applied_at', 'checksum'])
    expect(
      withRawConnection(path, raw =>
        raw.prepare('SELECT version, name, applied_at, checksum FROM _sirannon_migrations WHERE version < 3').all(),
      ),
    ).toEqual([
      { version: 1, name: 'create_invoices', applied_at: 1_700_000_000.25, checksum: INVOICES_SCRIPT_FNV1A },
      { version: 2, name: 'seed_invoices', applied_at: 1_700_000_060.5, checksum: null },
    ])
    expect(readFileState(path)).toMatchObject({ userVersion: 3, tables: ['invoices', 'refunds'] })
    expect(countRows(path, 'invoices')).toBe(2)

    await expect(db.migrate([invoices(`${INVOICES_SCRIPT};`)])).rejects.toMatchObject({
      code: 'MIGRATION_CHECKSUM_MISMATCH',
      version: 1,
    })
  })

  it('backfills a null checksum and user_version when nothing is pending', async () => {
    const path = scratchPath('null-checksums.db')
    withRawConnection(path, raw => {
      raw.exec(`${LEGACY_HISTORY_TABLE.slice(0, -2)},\n  checksum TEXT\n)`)
      raw.exec(INVOICES_SCRIPT)
      raw.prepare('INSERT INTO _sirannon_migrations (version, name) VALUES (?, ?)').run(1, 'create_invoices')
    })
    const db = await registry().open('legacy', path)

    expect(await db.migrate([invoices()])).toEqual({ applied: [], skipped: 1 })

    expect(readFileState(path)).toMatchObject({
      userVersion: 1,
      history: [{ version: 1, name: 'create_invoices', checksum: INVOICES_SCRIPT_FNV1A }],
    })
  })
})
