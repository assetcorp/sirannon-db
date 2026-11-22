import { afterEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { MigrationError } from '../../../core/errors.js'
import type { Migration } from '../../../core/migrations/types.js'
import { cleanUp, openDatabase, recordedVersions, withRawConnection } from './harness.js'

afterEach(cleanUp)

const SCHEMA: Migration = {
  version: 1,
  name: 'create_customers_and_orders',
  up: `
CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE);
CREATE TABLE invoices (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL);
CREATE TABLE refunds (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id) ON DELETE RESTRICT);
INSERT INTO customers VALUES (1, 'first', '10'), (2, 'second', '20');
INSERT INTO orders VALUES (10, 1), (11, 2);
INSERT INTO invoices VALUES (20, 1), (21, 2);
INSERT INTO refunds VALUES (30, 1);
`,
}

const COPY_CUSTOMERS = `
CREATE TABLE new_customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER);
INSERT INTO new_customers SELECT id, name, CAST(score AS INTEGER) FROM customers;
DROP TABLE customers;
ALTER TABLE new_customers RENAME TO customers;
`

const PRISMA_SHAPED_COPY = `-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_customers" (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER);
INSERT INTO "new_customers" ("id", "name", "score") SELECT "id", "name", CAST("score" AS INTEGER) FROM "customers";
DROP TABLE "customers";
ALTER TABLE "new_customers" RENAME TO "customers";
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
`

const PRAGMA_AFTER_OTHER_STATEMENTS = `
CREATE INDEX orders_by_customer ON orders (customer_id);
UPDATE customers SET name = upper(name);
pragma main.foreign_keys = 'off';
${COPY_CUSTOMERS}
`

const PLACEMENTS = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

interface ChildRows {
  orders: number
  invoicesWithCustomer: number
  refunds: number
}

function childRows(path: string): ChildRows {
  return withRawConnection(path, raw => ({
    orders: Number((raw.prepare('SELECT count(*) AS n FROM orders').get() as { n: number }).n),
    invoicesWithCustomer: Number(
      (raw.prepare('SELECT count(*) AS n FROM invoices WHERE customer_id IS NOT NULL').get() as { n: number }).n,
    ),
    refunds: Number((raw.prepare('SELECT count(*) AS n FROM refunds').get() as { n: number }).n),
  }))
}

function scoreType(path: string): string {
  return withRawConnection(
    path,
    raw =>
      (raw.prepare("SELECT type FROM pragma_table_info('customers') WHERE name = 'score'").get() as { type: string })
        .type,
  )
}

async function expectForeignKeysEnforced(db: Database): Promise<void> {
  await expect(db.execute('INSERT INTO orders VALUES (99, 404)')).rejects.toThrow(/FOREIGN KEY constraint failed/)
}

describe.each(PLACEMENTS)('a migration that copies a table that foreign keys refer to, with $label', placement => {
  it('keeps every child row when the SQL is in the shape that Prisma writes', async () => {
    const { db, path } = await openDatabase('prisma-shape', { writerWorker: placement.writerWorker })
    await db.migrate([SCHEMA, { version: 2, name: 'score_as_integer', up: PRISMA_SHAPED_COPY }])

    expect(childRows(path)).toEqual({ orders: 2, invoicesWithCustomer: 2, refunds: 1 })
    expect(scoreType(path)).toBe('INTEGER')
    expect(recordedVersions(path)).toEqual([1, 2])
    await expectForeignKeysEnforced(db)
  })

  it('keeps every child row when the pragma comes after other statements', async () => {
    const { db, path } = await openDatabase('pragma-later', { writerWorker: placement.writerWorker })
    await db.migrate([SCHEMA, { version: 2, name: 'score_as_integer', up: PRAGMA_AFTER_OTHER_STATEMENTS }])

    expect(childRows(path)).toEqual({ orders: 2, invoicesWithCustomer: 2, refunds: 1 })
    expect(withRawConnection(path, raw => raw.prepare('SELECT name FROM customers ORDER BY id').all())).toEqual([
      { name: 'FIRST' },
      { name: 'SECOND' },
    ])
    await expectForeignKeysEnforced(db)
  })

  it('rolls back with MIGRATION_ERROR and names the broken rows when the copy loses a parent row', async () => {
    const { db, path } = await openDatabase('broken-reference', { writerWorker: placement.writerWorker })
    await db.migrate([SCHEMA])
    const losesCustomerTwo = `PRAGMA foreign_keys = OFF;
CREATE TABLE new_customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER);
INSERT INTO new_customers SELECT id, name, CAST(score AS INTEGER) FROM customers WHERE id = 1;
DROP TABLE customers;
ALTER TABLE new_customers RENAME TO customers;`

    const failure = await db
      .migrate([SCHEMA, { version: 2, name: 'lose_customer_two', up: losesCustomerTwo }])
      .catch((err: unknown) => err)

    expect(failure).toBeInstanceOf(MigrationError)
    expect(failure).toMatchObject({ code: 'MIGRATION_ERROR', version: 2 })
    expect((failure as Error).message).toMatch(/orders row 11 refers to a missing row of customers/)
    expect(recordedVersions(path)).toEqual([1])
    expect(scoreType(path)).toBe('TEXT')
    expect(childRows(path)).toEqual({ orders: 2, invoicesWithCustomer: 2, refunds: 1 })
    await expectForeignKeysEnforced(db)
  })
})

describe('a migration that turns foreign keys off', () => {
  it('runs a function migration with withoutForeignKeys and keeps every child row', async () => {
    const { db, path } = await openDatabase('function-migration')
    await db.migrate([
      SCHEMA,
      {
        version: 2,
        name: 'score_as_integer',
        withoutForeignKeys: true,
        up: async tx => {
          for (const statement of COPY_CUSTOMERS.split(';').filter(sql => sql.trim().length > 0)) {
            await tx.execute(statement)
          }
        },
      },
    ])

    expect(childRows(path)).toEqual({ orders: 2, invoicesWithCustomer: 2, refunds: 1 })
    await expectForeignKeysEnforced(db)
  })

  it('keeps every child row when a rollback copies the table back', async () => {
    const { db, path } = await openDatabase('rollback-copy')
    const copyBack = `PRAGMA foreign_keys = OFF;
CREATE TABLE old_customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score TEXT);
INSERT INTO old_customers SELECT id, name, CAST(score AS TEXT) FROM customers;
DROP TABLE customers;
ALTER TABLE old_customers RENAME TO customers;`
    const migrations: Migration[] = [
      SCHEMA,
      { version: 2, name: 'score_as_integer', up: PRISMA_SHAPED_COPY, down: copyBack },
    ]
    await db.migrate(migrations)
    await db.rollback(migrations)

    expect(recordedVersions(path)).toEqual([1])
    expect(scoreType(path)).toBe('TEXT')
    expect(childRows(path)).toEqual({ orders: 2, invoicesWithCustomer: 2, refunds: 1 })
    await expectForeignKeysEnforced(db)
  })

  it('fails with MIGRATION_ERROR when a deferred foreign key breaks at commit, and records nothing', async () => {
    const { db, path } = await openDatabase('deferred-at-commit')
    await db.migrate([SCHEMA])
    const orphan = `PRAGMA defer_foreign_keys = ON;
INSERT INTO orders VALUES (12, 404);`

    const failure = await db
      .migrate([SCHEMA, { version: 2, name: 'insert_orphan_order', up: orphan }])
      .catch((err: unknown) => err)

    expect(failure).toBeInstanceOf(MigrationError)
    expect(failure).toMatchObject({ code: 'MIGRATION_ERROR', version: 2 })
    expect(recordedVersions(path)).toEqual([1])
    expect(childRows(path).orders).toBe(2)
    await expectForeignKeysEnforced(db)
  })
})
