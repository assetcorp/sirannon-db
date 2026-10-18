import { readdirSync } from 'node:fs'
import { afterEach, describe, expect, it } from 'vitest'
import { createTenantResolver } from '../../../core/lifecycle/tenant.js'
import type { Migration } from '../../../core/migrations/types.js'
import {
  cleanUp,
  columnsOf,
  countRows,
  historyTableExists,
  readFileState,
  recordedVersions,
  registry,
  scratchDirectory,
  scratchPath,
} from './harness.js'

afterEach(cleanUp)

const CONCURRENT_RESOLVES = 5
const CONCURRENT_OPENS = 10

const createUsers: Migration = {
  version: 1,
  name: 'create_users',
  up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)',
}

const addUserName: Migration = {
  version: 2,
  name: 'add_user_name',
  up: async tx => {
    await tx.execute('ALTER TABLE users ADD COLUMN name TEXT')
  },
}

const createSessions: Migration = {
  version: 3,
  name: 'create_sessions',
  up: 'CREATE TABLE sessions (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users (id))',
}

const brokenIndex: Migration = {
  version: 3,
  name: 'broken_index',
  up: 'CREATE INDEX sessions_by_user ON missing_sessions (user_id)',
}

function expectConsistentAfterFailure(path: string): void {
  const state = readFileState(path)
  const versions = state.history.map(row => row.version)
  expect([[], [1, 2]]).toContainEqual(versions)
  expect(state.tables).toEqual(versions.length === 0 ? [] : ['users'])
  expect(state.userVersion).toBe(versions.at(-1) ?? 0)
}

describe('registry migrations on a direct open', () => {
  it('applies the pending set before any caller can reach the database, and only the new versions on a later open', async () => {
    const path = scratchPath('main.db')
    const tablesSeenAtOpen: string[][] = []
    const first = registry({
      migrations: [createUsers, addUserName],
      hooks: { onDatabaseOpen: ({ path: opened }) => tablesSeenAtOpen.push(readFileState(opened).tables) },
    })

    const opening = first.open('main', path)
    expect(first.has('main')).toBe(false)
    expect(first.get('main')).toBeUndefined()
    const db = await opening

    expect(tablesSeenAtOpen).toEqual([['users']])
    await db.execute("INSERT INTO users (email, name) VALUES ('ada@example.com', 'Ada')")
    await first.shutdown()

    const second = registry({ migrations: [createUsers, addUserName, createSessions] })
    await second.open('main', path)
    expect(readFileState(path)).toMatchObject({ userVersion: 3, tables: ['sessions', 'users'] })
    expect(recordedVersions(path)).toEqual([1, 2, 3])
    expect(countRows(path, 'users')).toBe(1)
    await second.shutdown()

    const before = readFileState(path)
    const edited = registry({ migrations: [{ ...createUsers, up: `${createUsers.up};` }] })
    await expect(edited.open('main', path)).rejects.toMatchObject({
      name: 'MigrationError',
      code: 'MIGRATION_CHECKSUM_MISMATCH',
      version: 1,
    })
    expect(edited.has('main')).toBe(false)
    expect(readFileState(path)).toEqual(before)
  })

  it('leaves the database unregistered when a migration fails, rethrows that MigrationError, and lets a later open retry', async () => {
    const path = scratchPath('main.db')
    const broken = registry({ migrations: [createUsers, addUserName, brokenIndex] })

    for (let attempt = 0; attempt < 2; attempt++) {
      const opening = broken.open('main', path)
      await expect(opening).rejects.toMatchObject({
        name: 'MigrationError',
        code: 'MIGRATION_ERROR',
        version: 3,
        message: expect.stringContaining('missing_sessions'),
      })
      expect(broken.has('main')).toBe(false)
      expect(broken.get('main')).toBeUndefined()
      expect(broken.databases().size).toBe(0)
      expectConsistentAfterFailure(path)
    }

    const repaired = registry({ migrations: [createUsers, addUserName, createSessions] })
    await repaired.open('main', path)
    expect(readFileState(path)).toMatchObject({ userVersion: 3, tables: ['sessions', 'users'] })
  })

  it('skips the set for a read-only open without calling the source function', async () => {
    const path = scratchPath('main.db')
    await registry({ migrations: [createUsers] }).open('seed', path)
    const before = readFileState(path)
    let sourceCalls = 0
    const readers = registry({
      migrations: () => {
        sourceCalls++
        return [createUsers, addUserName]
      },
    })

    const db = await readers.open('main', path, { readOnly: true })

    expect(await db.query('SELECT id, email FROM users')).toEqual([])
    expect(sourceCalls).toBe(0)
    expect(columnsOf(path, 'users')).toEqual(['id', 'email'])
    expect(readFileState(path)).toEqual(before)
  })

  it('creates no history table when the registry declares no set or an empty one', async () => {
    const sets = [undefined, [], () => [], async () => []]

    for (const [index, migrations] of sets.entries()) {
      const path = scratchPath(`empty-${index}.db`)
      await registry(migrations === undefined ? {} : { migrations }).open(`empty-${index}`, path)
      expect(historyTableExists(path)).toBe(false)
    }
  })

  it('applies SQL and function migrations through the writer worker', async () => {
    const path = scratchPath('main.db')
    const db = await registry({ migrations: [createUsers, addUserName], writerWorker: true }).open('main', path)

    await db.execute("INSERT INTO users (email, name) VALUES ('ada@example.com', 'Ada')")

    expect(columnsOf(path, 'users')).toEqual(['id', 'email', 'name'])
    expect(readFileState(path).userVersion).toBe(2)
  })
})

describe('a registry migration source function', () => {
  it('receives one call from the registry across many concurrent opens', async () => {
    let sourceCalls = 0
    const sirannon = registry({
      migrations: async () => {
        sourceCalls++
        await new Promise(resolve => setTimeout(resolve, 5))
        return [createUsers, addUserName]
      },
    })
    const ids = Array.from({ length: CONCURRENT_OPENS }, (_, index) => `tenant-${index}`)

    await Promise.all(ids.map(id => sirannon.open(id, scratchPath(`${id}.db`))))

    expect(sourceCalls).toBe(1)
    for (const id of ids) expect(columnsOf(scratchPath(`${id}.db`), 'users')).toEqual(['id', 'email', 'name'])
  })

  it('fails the open and leaves the database unregistered when it throws or rejects, so that the next open calls it again', async () => {
    const path = scratchPath('main.db')
    let sourceCalls = 0
    const sirannon = registry({
      migrations: () => {
        sourceCalls++
        if (sourceCalls === 1) throw new Error('migration bundle not ready')
        if (sourceCalls === 2) return Promise.reject(new Error('migration bundle still loading'))
        return [createUsers]
      },
    })

    await expect(sirannon.open('main', path)).rejects.toThrow('migration bundle not ready')
    expect(sirannon.has('main')).toBe(false)
    await expect(sirannon.open('main', path)).rejects.toThrow('migration bundle still loading')
    expect(sirannon.has('main')).toBe(false)
    expect(historyTableExists(path)).toBe(false)

    await sirannon.open('main', path)
    expect(sourceCalls).toBe(3)
    expect(recordedVersions(path)).toEqual([1])
  })

  it('fails the open with MIGRATION_SOURCE_INVALID when it returns no array, and rethrows a validation error in the set', async () => {
    const results: unknown[] = [{}, null, undefined, 'CREATE TABLE users (id)', 42, Promise.resolve({ 1: createUsers })]

    for (const [index, result] of results.entries()) {
      const path = scratchPath(`invalid-${index}.db`)
      const sirannon = registry({ migrations: () => result as Migration[] })
      await expect(sirannon.open('main', path)).rejects.toMatchObject({
        name: 'MigrationError',
        code: 'MIGRATION_SOURCE_INVALID',
        version: 0,
      })
      expect(sirannon.has('main')).toBe(false)
      expect(historyTableExists(path)).toBe(false)
    }

    const path = scratchPath('malformed.db')
    const malformed = registry({ migrations: () => [createUsers, { ...addUserName, name: 'add-user-name' }] })
    await expect(malformed.open('main', path)).rejects.toMatchObject({ code: 'MIGRATION_VALIDATION_ERROR', version: 2 })
    expect(historyTableExists(path)).toBe(false)
  })
})

describe('registry migrations on a tenant opened through the resolver', () => {
  it('migrates a tenant on first use and opens no file for an id that the tenant resolver refuses', async () => {
    const basePath = scratchDirectory('tenants')
    const sirannon = registry({
      migrations: [createUsers, addUserName],
      lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath }) } },
    })

    for (const refused of ['Acme', 'ACME', 'acme/../../escape', '_acme', '-acme', '', 'a'.repeat(256)]) {
      expect(await sirannon.resolve(refused), refused).toBeUndefined()
    }
    expect(readdirSync(basePath)).toEqual([])

    const tenant = await sirannon.resolve('acme')
    expect(await tenant?.query('SELECT id, email, name FROM users')).toEqual([])
    expect(readdirSync(basePath).filter(name => name.endsWith('.db'))).toEqual(['acme.db'])
    expect(recordedVersions(`${basePath}/acme.db`)).toEqual([1, 2])
  })

  it('shares one open and one migration pass among concurrent resolves of a cold tenant, and one error when it fails', async () => {
    const basePath = scratchDirectory('tenants')
    let resolverCalls = 0
    let sourceCalls = 0
    const resolve = createTenantResolver({ basePath })
    const sirannon = registry({
      migrations: () => {
        sourceCalls++
        return [createUsers, addUserName]
      },
      lifecycle: {
        autoOpen: {
          resolver: id => {
            resolverCalls++
            return resolve(id)
          },
        },
      },
    })

    const opened = await Promise.all(Array.from({ length: CONCURRENT_RESOLVES }, () => sirannon.resolve('acme')))

    expect(opened[0]).toBeDefined()
    for (const db of opened) expect(db).toBe(opened[0])
    expect([resolverCalls, sourceCalls]).toEqual([1, 1])

    const broken = registry({
      migrations: [createUsers, addUserName, brokenIndex],
      lifecycle: { autoOpen: { resolver: resolve } },
    })
    const settled = await Promise.allSettled(
      Array.from({ length: CONCURRENT_RESOLVES }, () => broken.resolve('globex')),
    )
    const reasons = settled.map(outcome => (outcome.status === 'rejected' ? outcome.reason : outcome.status))
    expect(reasons[0]).toMatchObject({ name: 'MigrationError', code: 'MIGRATION_ERROR', version: 3 })
    for (const reason of reasons) expect(reason).toBe(reasons[0])
    expect(broken.has('globex')).toBe(false)
    expectConsistentAfterFailure(`${basePath}/globex.db`)
  })
})
