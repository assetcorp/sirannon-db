import { afterEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import { cleanUp, openDatabase, recordedVersions, registry, withRawConnection } from './harness.js'

afterEach(cleanUp)

const SCHEMA: Migration = {
  version: 1,
  name: 'create_orders',
  up: `CREATE TABLE customers (id INTEGER PRIMARY KEY);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE);
INSERT INTO customers VALUES (1);
INSERT INTO orders VALUES (1, 1);`,
}

const COPY = `CREATE TABLE replacement (id INTEGER PRIMARY KEY);
INSERT INTO replacement SELECT * FROM customers;
DROP TABLE customers;
ALTER TABLE replacement RENAME TO customers;`

function orders(path: string): unknown[] {
  return withRawConnection(path, raw => raw.prepare('SELECT * FROM orders ORDER BY id').all())
}

function signal(): { promise: Promise<void>; resolve: () => void } {
  let resolve = () => {}
  const promise = new Promise<void>(done => {
    resolve = done
  })
  return { promise, resolve }
}

describe('foreign key migration boundaries', () => {
  it.each([
    "PRAGMA 'foreign_keys' = OFF;",
    '\ufeffPRAGMA foreign_keys = OFF;',
    'PrAgMa /* setting */ main . foreign_keys ( "FaLsE" );',
    'PRAGMA foreign_keys=ON; PRAGMA foreign_keys=0;',
    "PRAGMA foreign_keys('no');",
  ])('preserves child rows when copying with %s', async marker => {
    const { db, path } = await openDatabase()
    await db.migrate([SCHEMA, { version: 2, name: 'copy_customers', up: `${marker}\n${COPY}` }])
    expect(orders(path)).toEqual([{ id: 1, customer_id: 1 }])
    await expect(db.execute('INSERT INTO orders VALUES (2, 404)')).rejects.toThrow(/FOREIGN KEY/)
  })

  it('keeps cascades enabled for markers in literals and comments, and for a pragma read', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([
      SCHEMA,
      {
        version: 2,
        name: 'delete_customer',
        up: `SELECT '; PRAGMA foreign_keys=OFF;';
/* PRAGMA foreign_keys=OFF; */
-- PRAGMA foreign_keys=OFF;
PRAGMA foreign_keys;
CREATE TRIGGER customer_added AFTER INSERT ON customers BEGIN
  SELECT '; PRAGMA foreign_keys=OFF;';
  SELECT CASE WHEN NEW.id=2 THEN 'PRAGMA foreign_keys=OFF' ELSE '' END;
END;
DELETE FROM customers;`,
      },
    ])
    expect(orders(path)).toEqual([])
    expect(recordedVersions(path)).toEqual([1, 2])
  })

  it.each([false, true])('restores enforcement before a concurrent write with writerWorker=%s', async writerWorker => {
    const { db, path } = await openDatabase('concurrent', { writerWorker })
    await db.migrate([SCHEMA])
    const entered = signal()
    const release = signal()
    const migration = db.migrate([
      SCHEMA,
      {
        version: 2,
        name: 'copy_customers',
        withoutForeignKeys: true,
        up: async tx => {
          await db.execute('INSERT INTO customers VALUES (2)')
          await tx.execute('DELETE FROM customers WHERE id=2')
          entered.resolve()
          await release.promise
        },
      },
    ])
    await entered.promise
    const write = db.execute('INSERT INTO orders VALUES (2, 404)').catch((error: unknown) => error)
    release.resolve()
    await migration
    expect(await write).toBeInstanceOf(Error)
    expect(orders(path)).toEqual([{ id: 1, customer_id: 1 }])
    expect(recordedVersions(path)).toEqual([1, 2])
  })

  it('rolls back a partial failure and enables foreign keys for the next migration after a restart', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([SCHEMA])
    await expect(
      db.migrate([
        SCHEMA,
        { version: 2, name: 'broken_copy', up: `PRAGMA foreign_keys=OFF; ${COPY} SELECT * FROM missing_table;` },
      ]),
    ).rejects.toMatchObject({ code: 'MIGRATION_ERROR', version: 2 })
    expect(recordedVersions(path)).toEqual([1])
    expect(orders(path)).toEqual([{ id: 1, customer_id: 1 }])
    await expect(db.execute('INSERT INTO orders VALUES (2, 404)')).rejects.toThrow(/FOREIGN KEY/)
    await db.close()
    const reopened = await registry().open('reopened', path)
    await reopened.migrate([
      SCHEMA,
      { version: 2, name: 'copy_customers', up: `PRAGMA foreign_keys=OFF; ${COPY}` },
      { version: 3, name: 'delete_customer', up: 'DELETE FROM customers' },
    ])
    expect(orders(path)).toEqual([])
    expect(recordedVersions(path)).toEqual([1, 2, 3])
  })

  it.each([
    false,
    true,
  ])('restores foreign keys after a mixed rollback with callerDisabled=%s', async callerDisabled => {
    const { db, path } = await openDatabase()
    const migrations: Migration[] = [
      SCHEMA,
      {
        version: 2,
        name: 'copy_customers',
        up: `PRAGMA foreign_keys=OFF; ${COPY}`,
        down: `PRAGMA foreign_keys=OFF; ${COPY}`,
      },
      {
        version: 3,
        name: 'add_customer',
        up: 'INSERT INTO customers VALUES (2)',
        down: 'DELETE FROM customers WHERE id=2',
      },
    ]
    if (callerDisabled) await db.execute('PRAGMA foreign_keys=OFF')
    await db.migrate(migrations)
    await db.rollback(migrations, 1)
    if (callerDisabled) {
      await db.execute('INSERT INTO orders VALUES (2, 404)')
      expect(orders(path)).toEqual([
        { id: 1, customer_id: 1 },
        { id: 2, customer_id: 404 },
      ])
    } else {
      await expect(db.execute('INSERT INTO orders VALUES (2, 404)')).rejects.toThrow(/FOREIGN KEY/)
      expect(orders(path)).toEqual([{ id: 1, customer_id: 1 }])
    }
    expect(recordedVersions(path)).toEqual([1])
  })
})
