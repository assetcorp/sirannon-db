import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Database } from '../../database.js'
import { MigrationError, SirannonError } from '../../errors.js'
import { createTenantResolver } from '../../lifecycle/tenant.js'
import type { Migration } from '../../migrations/types.js'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-reg-mig-'))
})

afterEach(() => {
  vi.restoreAllMocks()
  rmSync(tempDir, { recursive: true, force: true })
})

const createUsersTable: Migration = {
  version: 1,
  name: 'create_users',
  up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)',
}

const addUsersNameColumn: Migration = {
  version: 2,
  name: 'add_users_name',
  up: 'ALTER TABLE users ADD COLUMN name TEXT',
}

const failingMigration: Migration = {
  version: 3,
  name: 'broken_index',
  up: 'CREATE INDEX idx_missing ON no_such_table (col)',
}

describe('registry migrations', () => {
  it('applies the migration set on a direct open before the database serves queries', async () => {
    const sir = new Sirannon({ driver: testDriver, migrations: [createUsersTable, addUsersNameColumn] })
    const db = await sir.open('main', join(tempDir, 'main.db'))

    await db.execute("INSERT INTO users (email, name) VALUES ('ada@example.com', 'Ada')")
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toEqual([{ name: 'Ada' }])
    await sir.shutdown()
  })

  it('applies the migration set to a tenant opened lazily through resolve', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      migrations: [createUsersTable],
      lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: tempDir }) } },
    })

    const db = await sir.resolve('tenantA')
    expect(db).toBeDefined()
    const rows = await db?.query('SELECT id FROM users')
    expect(rows).toEqual([])
    await sir.shutdown()
  })

  it('fails the open with a MigrationError and leaves the database unregistered when a migration fails', async () => {
    const sir = new Sirannon({ driver: testDriver, migrations: [createUsersTable, failingMigration] })

    const attempt = sir.open('main', join(tempDir, 'main.db'))
    await expect(attempt).rejects.toThrow(MigrationError)
    await expect(attempt).rejects.toMatchObject({ code: 'MIGRATION_ERROR', version: 3 })
    expect(sir.has('main')).toBe(false)
    expect(sir.get('main')).toBeUndefined()
    await sir.shutdown()
  })

  it('wraps a non-Sirannon failure of the migration step in a typed open error', async () => {
    vi.spyOn(Database.prototype, 'migrate').mockRejectedValueOnce(new Error('disk I/O error'))
    const sir = new Sirannon({ driver: testDriver, migrations: [createUsersTable] })

    const attempt = sir.open('main', join(tempDir, 'main.db'))
    await expect(attempt).rejects.toThrow(SirannonError)
    await expect(attempt).rejects.toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    expect(sir.has('main')).toBe(false)
    await sir.shutdown()
  })

  it('allows a later open of the same id to retry after a failed migration run', async () => {
    const sirBroken = new Sirannon({ driver: testDriver, migrations: [createUsersTable, failingMigration] })
    await expect(sirBroken.open('main', join(tempDir, 'main.db'))).rejects.toThrow(MigrationError)
    await sirBroken.shutdown()

    const sirFixed = new Sirannon({ driver: testDriver, migrations: [createUsersTable, addUsersNameColumn] })
    const db = await sirFixed.open('main', join(tempDir, 'main.db'))
    const rows = await db.query('SELECT id, email, name FROM users')
    expect(rows).toEqual([])
    await sirFixed.shutdown()
  })

  it('returns undefined from resolve when a failing migration aborts the auto-open', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      migrations: [failingMigration],
      lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: tempDir }) } },
    })

    await expect(sir.resolve('tenantA')).rejects.toThrow(MigrationError)
    expect(sir.has('tenantA')).toBe(false)
    await sir.shutdown()
  })

  it('shares one open between concurrent resolve calls for the same cold tenant', async () => {
    let resolverCalls = 0
    const resolver = (id: string) => {
      resolverCalls++
      return { path: join(tempDir, `${id}.db`) }
    }
    const sir = new Sirannon({
      driver: testDriver,
      migrations: [createUsersTable],
      lifecycle: { autoOpen: { resolver } },
    })

    const [a, b, c] = await Promise.all([sir.resolve('tenantA'), sir.resolve('tenantA'), sir.resolve('tenantA')])
    expect(a).toBeDefined()
    expect(b).toBe(a)
    expect(c).toBe(a)
    expect(resolverCalls).toBe(1)
    await sir.shutdown()
  })

  it('rejects every concurrent resolve call when the shared open fails its migrations', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      migrations: [failingMigration],
      lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: tempDir }) } },
    })

    const results = await Promise.allSettled([sir.resolve('tenantA'), sir.resolve('tenantA')])
    for (const result of results) {
      expect(result.status).toBe('rejected')
    }
    expect(sir.has('tenantA')).toBe(false)
    await sir.shutdown()
  })

  it('can resolve the same tenant again after a failed shared open', async () => {
    const sirBroken = new Sirannon({
      driver: testDriver,
      migrations: [failingMigration],
      lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: tempDir }) } },
    })
    await expect(sirBroken.resolve('tenantA')).rejects.toThrow(MigrationError)
    await sirBroken.shutdown()

    const sirFixed = new Sirannon({
      driver: testDriver,
      migrations: [createUsersTable],
      lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: tempDir }) } },
    })
    const db = await sirFixed.resolve('tenantA')
    expect(db).toBeDefined()
    await sirFixed.shutdown()
  })

  it('skips the migration set for a read-only database', async () => {
    const seed = new Sirannon({ driver: testDriver })
    const seeded = await seed.open('main', join(tempDir, 'main.db'))
    await seeded.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)')
    await seed.shutdown()

    const sir = new Sirannon({ driver: testDriver, migrations: [createUsersTable, addUsersNameColumn] })
    const db = await sir.open('main', join(tempDir, 'main.db'), { readOnly: true })
    const columns = await db.query<{ name: string }>("SELECT name FROM pragma_table_info('users')")
    expect(columns.map(column => column.name)).toEqual(['id', 'email'])
    await sir.shutdown()
  })

  it('accepts a source function and invokes it once across many opens', async () => {
    let sourceCalls = 0
    const sir = new Sirannon({
      driver: testDriver,
      migrations: () => {
        sourceCalls++
        return Promise.resolve([createUsersTable])
      },
    })

    const first = await sir.open('a', join(tempDir, 'a.db'))
    const second = await sir.open('b', join(tempDir, 'b.db'))
    expect(await first.query('SELECT id FROM users')).toEqual([])
    expect(await second.query('SELECT id FROM users')).toEqual([])
    expect(sourceCalls).toBe(1)
    await sir.shutdown()
  })

  it('fails the open and retries the source function on the next open after it throws', async () => {
    let sourceCalls = 0
    const sir = new Sirannon({
      driver: testDriver,
      migrations: () => {
        sourceCalls++
        if (sourceCalls === 1) throw new Error('bundle not ready')
        return [createUsersTable]
      },
    })

    const attempt = sir.open('main', join(tempDir, 'main.db'))
    await expect(attempt).rejects.toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    expect(sir.has('main')).toBe(false)

    const db = await sir.open('main', join(tempDir, 'main.db'))
    expect(await db.query('SELECT id FROM users')).toEqual([])
    expect(sourceCalls).toBe(2)
    await sir.shutdown()
  })

  it('fails the open with MIGRATION_SOURCE_INVALID when the source returns a non-array', async () => {
    const sir = new Sirannon({
      driver: testDriver,
      migrations: () => ({}) as unknown as [],
    })

    const attempt = sir.open('main', join(tempDir, 'main.db'))
    await expect(attempt).rejects.toThrow(SirannonError)
    await expect(attempt).rejects.toMatchObject({ code: 'MIGRATION_SOURCE_INVALID' })
    expect(sir.has('main')).toBe(false)
    await sir.shutdown()
  })

  it('leaves behaviour unchanged when no migration set is declared', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'main.db'))
    const tables = await db.query<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table'")
    expect(tables.some(table => table.name === '_sirannon_migrations')).toBe(false)
    await sir.shutdown()
  })
})
