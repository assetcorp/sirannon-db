import { afterEach, describe, expect, it } from 'vitest'
import { MigrationError } from '../../../core/errors.js'
import type { Migration } from '../../../core/migrations/types.js'
import {
  cleanUp,
  columnsOf,
  openDatabase,
  readFileState,
  recordedVersions,
  registry,
  scratchPath,
  withRawConnection,
} from './harness.js'

afterEach(cleanUp)

const createCustomers: Migration = {
  version: 1,
  name: 'create_customers',
  up: 'CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)',
  down: 'DROP TABLE customers',
}

const addCustomerTier: Migration = {
  version: 2,
  name: 'add_customer_tier',
  up: async tx => {
    await tx.execute("ALTER TABLE customers ADD COLUMN tier TEXT NOT NULL DEFAULT 'free'")
    await tx.execute('CREATE INDEX customers_by_tier ON customers (tier)')
  },
  down: async tx => {
    await tx.execute('DROP INDEX customers_by_tier')
    await tx.execute('ALTER TABLE customers DROP COLUMN tier')
  },
}

const seedCustomers: Migration = {
  version: 3,
  name: 'seed_customers',
  up: async tx => {
    await tx.execute("INSERT INTO customers (email, tier) VALUES ('ada@example.com', 'pro')")
    await tx.execute("INSERT INTO customers (email) VALUES ('grace@example.com')")
  },
  down: "DELETE FROM customers WHERE email IN ('ada@example.com', 'grace@example.com')",
}

const CUSTOMER_SET = [seedCustomers, createCustomers, addCustomerTier]

function customersIn(path: string): { email: string; tier: string }[] {
  return withRawConnection(
    path,
    raw => raw.prepare('SELECT email, tier FROM customers ORDER BY id').all() as { email: string; tier: string }[],
  )
}

describe.each([
  { label: 'on the calling thread', writerWorker: false },
  { label: 'on the writer worker', writerWorker: true },
])('migrations applied $label', ({ writerWorker }) => {
  it('applies SQL and function migrations in version order and records each one once', async () => {
    const { db, path } = await openDatabase('shop', { writerWorker })

    const first = await db.migrate(CUSTOMER_SET)

    expect(first).toEqual({
      applied: [
        { version: 1, name: 'create_customers' },
        { version: 2, name: 'add_customer_tier' },
        { version: 3, name: 'seed_customers' },
      ],
      skipped: 0,
    })
    expect(customersIn(path)).toEqual([
      { email: 'ada@example.com', tier: 'pro' },
      { email: 'grace@example.com', tier: 'free' },
    ])
    const state = readFileState(path)
    expect(state.history.map(row => [row.version, row.name])).toEqual([
      [1, 'create_customers'],
      [2, 'add_customer_tier'],
      [3, 'seed_customers'],
    ])
    expect(state.history[1].checksum).toBeNull()
    expect(state.history[2].checksum).toBeNull()
    expect(state.userVersion).toBe(3)

    expect(await db.migrate(CUSTOMER_SET)).toEqual({ applied: [], skipped: 3 })
    expect(customersIn(path)).toHaveLength(2)
    expect(await db.appliedMigrations()).toEqual(state.history)
  })

  it('reverts the newest migration by default, every version above a target, and everything at target 0', async () => {
    const { db, path } = await openDatabase('shop', { writerWorker })
    await db.migrate(CUSTOMER_SET)

    expect(await db.rollback(CUSTOMER_SET)).toEqual({ rolledBack: [{ version: 3, name: 'seed_customers' }] })
    expect(customersIn(path)).toEqual([])
    expect(readFileState(path)).toMatchObject({ userVersion: 2, tables: ['customers'] })

    expect(await db.rollback(CUSTOMER_SET, 2_147_483_648)).toEqual({ rolledBack: [] })
    expect(await db.rollback(CUSTOMER_SET, 2)).toEqual({ rolledBack: [] })

    expect(await db.rollback(CUSTOMER_SET, 1)).toEqual({ rolledBack: [{ version: 2, name: 'add_customer_tier' }] })
    expect(columnsOf(path, 'customers')).toEqual(['id', 'email'])
    expect(readFileState(path)).toMatchObject({ userVersion: 1 })

    await db.migrate(CUSTOMER_SET)
    expect(await db.rollback(CUSTOMER_SET, 0)).toEqual({
      rolledBack: [
        { version: 3, name: 'seed_customers' },
        { version: 2, name: 'add_customer_tier' },
        { version: 1, name: 'create_customers' },
      ],
    })
    expect(readFileState(path)).toEqual({ userVersion: 0, history: [], tables: [] })
    expect(await db.rollback(CUSTOMER_SET, 0)).toEqual({ rolledBack: [] })

    expect((await db.migrate(CUSTOMER_SET)).applied.map(entry => entry.version)).toEqual([1, 2, 3])
    expect(customersIn(path)).toHaveLength(2)
  })
})

describe('rollback without a down migration', () => {
  it('refuses with MIGRATION_NO_DOWN before it reverts anything, and reverts a newer version that has a down', async () => {
    const { db, path } = await openDatabase()
    const noDownForCustomers = { ...createCustomers, down: undefined }
    const set = [noDownForCustomers, addCustomerTier]
    await db.migrate(set)
    const before = readFileState(path)

    await expect(db.rollback(set, 0)).rejects.toMatchObject({ code: 'MIGRATION_NO_DOWN', version: 1 })
    await expect(db.rollback([addCustomerTier], 0)).rejects.toMatchObject({ code: 'MIGRATION_NO_DOWN', version: 1 })
    await expect(db.rollback([noDownForCustomers])).rejects.toMatchObject({ code: 'MIGRATION_NO_DOWN', version: 2 })
    expect(readFileState(path)).toEqual(before)
    expect(columnsOf(path, 'customers')).toEqual(['id', 'email', 'tier'])

    expect(await db.rollback(set)).toEqual({ rolledBack: [{ version: 2, name: 'add_customer_tier' }] })
    expect(recordedVersions(path)).toEqual([1])
  })
})

const failingUps: { label: string; up: Migration['up']; code: string; message: RegExp }[] = [
  {
    label: 'a statement that SQLite refuses part-way through a script',
    up: 'CREATE TABLE invoices (id INTEGER PRIMARY KEY); INSERT INTO invoices (id) VALUES (1); INSERT INTO missing_table VALUES (1)',
    code: 'MIGRATION_ERROR',
    message: /no such table: missing_table/,
  },
  {
    label: 'a function that throws an Error after it writes',
    up: async tx => {
      await tx.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY)')
      throw new Error('ledger service unavailable')
    },
    code: 'MIGRATION_ERROR',
    message: /ledger service unavailable/,
  },
  {
    label: 'a function that throws a string',
    up: () => {
      throw 'plain string failure'
    },
    code: 'MIGRATION_ERROR',
    message: /plain string failure/,
  },
  {
    label: 'a function that rejects with a number',
    up: () => Promise.reject(42),
    code: 'MIGRATION_ERROR',
    message: /42/,
  },
  {
    label: 'a function whose statement SQLite refuses',
    up: async tx => {
      await tx.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY)')
      await tx.execute('INSERT INTO invoices (id, missing_column) VALUES (1, 2)')
    },
    code: 'MIGRATION_ERROR',
    message: /missing_column/,
  },
  {
    label: 'a function that throws its own MigrationError',
    up: () => {
      throw new MigrationError('tenant refuses this change', 2, 'TENANT_REFUSED')
    },
    code: 'TENANT_REFUSED',
    message: /^tenant refuses this change$/,
  },
]

describe('a migration that fails part-way', () => {
  it.each(failingUps)('reports $label with the failing version and keeps the file consistent', async ({
    up,
    code,
    message,
  }) => {
    const { db, path } = await openDatabase()
    const failing: Migration = { version: 2, name: 'create_invoices', up }

    const outcome = db.migrate([createCustomers, failing])

    await expect(outcome).rejects.toBeInstanceOf(MigrationError)
    await expect(outcome).rejects.toMatchObject({ code, version: 2, message: expect.stringMatching(message) })
    const state = readFileState(path)
    const versions = state.history.map(row => row.version)
    expect([[], [1]]).toContainEqual(versions)
    expect(state.tables).toEqual(versions.length === 0 ? [] : ['customers'])
    expect(state.userVersion).toBe(versions.at(-1) ?? 0)

    const repaired: Migration = {
      version: 2,
      name: 'create_invoices',
      up: 'CREATE TABLE invoices (id INTEGER PRIMARY KEY)',
    }
    expect((await db.migrate([createCustomers, repaired])).applied.map(entry => entry.version)).toEqual(
      versions.length === 0 ? [1, 2] : [2],
    )
    expect(readFileState(path)).toMatchObject({ userVersion: 2, tables: ['customers', 'invoices'] })
  })

  it('reports a failing down with MIGRATION_ROLLBACK_ERROR and leaves every version applied', async () => {
    const { db, path } = await openDatabase()
    const original = new MigrationError('archive is still referenced', 2, 'ARCHIVE_IN_USE')
    const downs: { down: Migration['down']; code: string; message: RegExp }[] = [
      {
        down: 'DROP TABLE customers; DROP TABLE missing_table',
        code: 'MIGRATION_ROLLBACK_ERROR',
        message: /missing_table/,
      },
      {
        down: () => {
          throw 'down string failure'
        },
        code: 'MIGRATION_ROLLBACK_ERROR',
        message: /down string failure/,
      },
      {
        down: () => {
          throw original
        },
        code: 'ARCHIVE_IN_USE',
        message: /^archive is still referenced$/,
      },
    ]
    const archive: Migration = {
      version: 2,
      name: 'create_archive',
      up: 'CREATE TABLE archive (id INTEGER PRIMARY KEY)',
    }
    await db.migrate([createCustomers, archive])
    const before = readFileState(path)

    for (const { down, code, message } of downs) {
      const outcome = db.rollback([createCustomers, { ...archive, down }], 0)
      await expect(outcome).rejects.toMatchObject({ code, version: 2, message: expect.stringMatching(message) })
      expect(readFileState(path)).toEqual(before)
    }
    await expect(
      db.rollback([
        createCustomers,
        {
          ...archive,
          down: () => {
            throw original
          },
        },
      ]),
    ).rejects.toBe(original)
  })
})

describe('a database that refuses schema changes', () => {
  it('refuses migrate and rollback on a read-only or closed database and leaves the file untouched', async () => {
    const path = scratchPath('reports.db')
    const writer = await registry().open('writer', path)
    await writer.migrate([createCustomers])
    await writer.close()
    const before = readFileState(path)

    const reader = await registry().open('reader', path, { readOnly: true })
    await expect(reader.migrate([createCustomers, addCustomerTier])).rejects.toMatchObject({ code: 'READ_ONLY' })
    await expect(reader.rollback([createCustomers], 0)).rejects.toMatchObject({ code: 'READ_ONLY' })
    expect(await reader.appliedMigrations()).toEqual(before.history)
    await reader.close()

    await expect(reader.migrate([createCustomers, addCustomerTier])).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    await expect(reader.rollback([createCustomers], 0)).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    expect(readFileState(path)).toEqual(before)
  })
})
