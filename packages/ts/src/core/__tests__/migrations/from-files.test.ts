import { describe, expect, it } from 'vitest'
import { MigrationError } from '../../errors.js'
import { migrationsFromFiles } from '../../migrations/from-files.js'

describe('migrationsFromFiles', () => {
  it('builds a sorted migration set from a bundler-style map with path prefixes', () => {
    const migrations = migrationsFromFiles({
      './migrations/002_add_orders.up.sql': 'CREATE TABLE orders (id INTEGER PRIMARY KEY)',
      './migrations/001_create_users.up.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
      './migrations/001_create_users.down.sql': 'DROP TABLE users',
    })

    expect(migrations).toEqual([
      {
        version: 1,
        name: 'create_users',
        up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
        down: 'DROP TABLE users',
      },
      {
        version: 2,
        name: 'add_orders',
        up: 'CREATE TABLE orders (id INTEGER PRIMARY KEY)',
      },
    ])
  })

  it('parses keys with Windows path separators', () => {
    const migrations = migrationsFromFiles({
      'migrations\\001_create_users.up.sql': 'CREATE TABLE users (id INTEGER PRIMARY KEY)',
    })
    expect(migrations).toHaveLength(1)
    expect(migrations[0].name).toBe('create_users')
  })

  it('trims surrounding whitespace from SQL contents', () => {
    const migrations = migrationsFromFiles({
      '001_create_users.up.sql': '\n  CREATE TABLE users (id INTEGER PRIMARY KEY)\n',
    })
    expect(migrations[0].up).toBe('CREATE TABLE users (id INTEGER PRIMARY KEY)')
  })

  it('returns an empty set for an empty map', () => {
    expect(migrationsFromFiles({})).toEqual([])
  })

  it('rejects a key whose final segment does not match the naming convention', () => {
    expect(() => migrationsFromFiles({ './migrations/create-users.up.sql': 'CREATE TABLE users (id)' })).toThrow(
      MigrationError,
    )
  })

  it('rejects a non-string value with guidance about raw imports', () => {
    const attempt = () => migrationsFromFiles({ '001_create_users.up.sql': { default: 'CREATE TABLE users (id)' } })
    expect(attempt).toThrow(MigrationError)
    expect(attempt).toThrow(/query: '\?raw'/)
  })

  it('rejects empty SQL contents', () => {
    expect(() => migrationsFromFiles({ '001_create_users.up.sql': '   ' })).toThrow(MigrationError)
  })

  it('rejects a version whose only file is a down migration', () => {
    expect(() => migrationsFromFiles({ '001_create_users.down.sql': 'DROP TABLE users' })).toThrow(
      /missing an \.up\.sql file/,
    )
  })

  it('rejects two names sharing one version', () => {
    const attempt = () =>
      migrationsFromFiles({
        '001_create_users.up.sql': 'CREATE TABLE users (id)',
        '001_create_orders.up.sql': 'CREATE TABLE orders (id)',
      })
    expect(attempt).toThrow(MigrationError)
    try {
      attempt()
    } catch (err) {
      expect((err as MigrationError).code).toBe('MIGRATION_DUPLICATE_VERSION')
    }
  })

  it('rejects two files claiming the same version and direction', () => {
    expect(() =>
      migrationsFromFiles({
        './a/001_create_users.up.sql': 'CREATE TABLE users (id)',
        './b/001_create_users.up.sql': 'CREATE TABLE users (id)',
      }),
    ).toThrow(MigrationError)
  })

  it('rejects version zero', () => {
    expect(() => migrationsFromFiles({ '0_create_users.up.sql': 'CREATE TABLE users (id)' })).toThrow(MigrationError)
  })
})
