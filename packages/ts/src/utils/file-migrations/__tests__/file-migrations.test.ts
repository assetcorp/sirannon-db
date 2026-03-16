import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { MigrationError } from '../../../core/errors.js'
import { loadMigrations, readDownMigrations, readUpMigrations, scanDirectory } from '../index.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-filemig-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('scanDirectory', () => {
  it('scans migration files in correct order', () => {
    writeFileSync(join(tempDir, '2_add_age.up.sql'), 'ALTER TABLE users ADD COLUMN age INTEGER')
    writeFileSync(join(tempDir, '1_create_users.up.sql'), 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
    writeFileSync(join(tempDir, '1_create_users.down.sql'), 'DROP TABLE users')

    const results = scanDirectory(tempDir)
    expect(results).toHaveLength(2)
    expect(results[0].version).toBe(1)
    expect(results[0].name).toBe('create_users')
    expect(results[0].downPath).not.toBeNull()
    expect(results[1].version).toBe(2)
    expect(results[1].name).toBe('add_age')
    expect(results[1].downPath).toBeNull()
  })

  it('throws on null bytes in path', () => {
    expect(() => scanDirectory('/tmp/foo\0bar')).toThrow(MigrationError)
  })

  it('throws on path traversal', () => {
    expect(() => scanDirectory('../../../etc')).toThrow(MigrationError)
  })

  it('throws on non-existent directory', () => {
    expect(() => scanDirectory(join(tempDir, 'nope'))).toThrow(MigrationError)
  })

  it('throws on non-directory path', () => {
    const filePath = join(tempDir, 'not-a-dir.txt')
    writeFileSync(filePath, 'text')
    expect(() => scanDirectory(filePath)).toThrow(MigrationError)
  })

  it('throws on missing up file', () => {
    writeFileSync(join(tempDir, '1_create_users.down.sql'), 'DROP TABLE users')
    expect(() => scanDirectory(tempDir)).toThrow(MigrationError)
  })

  it('throws on duplicate version with different names', () => {
    writeFileSync(join(tempDir, '1_create_users.up.sql'), 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
    writeFileSync(join(tempDir, '1_init_db.up.sql'), 'CREATE TABLE init (id INTEGER PRIMARY KEY)')
    expect(() => scanDirectory(tempDir)).toThrow(MigrationError)
  })
})

describe('readUpMigrations', () => {
  it('reads migration SQL from files', () => {
    writeFileSync(join(tempDir, '1_create_users.up.sql'), 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
    const scanned = scanDirectory(tempDir)
    const migrations = readUpMigrations(scanned)
    expect(migrations).toHaveLength(1)
    expect(migrations[0].up).toBe('CREATE TABLE users (id INTEGER PRIMARY KEY)')
  })

  it('throws on empty migration file', () => {
    writeFileSync(join(tempDir, '1_empty.up.sql'), '  ')
    const scanned = scanDirectory(tempDir)
    expect(() => readUpMigrations(scanned)).toThrow(MigrationError)
  })
})

describe('readDownMigrations', () => {
  it('reads down migration SQL', () => {
    writeFileSync(join(tempDir, '1_create_users.up.sql'), 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
    writeFileSync(join(tempDir, '1_create_users.down.sql'), 'DROP TABLE users')
    const scanned = scanDirectory(tempDir)
    const downMigrations = readDownMigrations(scanned, [1])
    expect(downMigrations).toHaveLength(1)
    expect(downMigrations[0].down).toBe('DROP TABLE users')
  })

  it('throws when down file is missing', () => {
    writeFileSync(join(tempDir, '1_create_users.up.sql'), 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
    const scanned = scanDirectory(tempDir)
    expect(() => readDownMigrations(scanned, [1])).toThrow(MigrationError)
  })
})

describe('loadMigrations', () => {
  it('loads migration objects from a directory', () => {
    writeFileSync(join(tempDir, '1_create_users.up.sql'), 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    writeFileSync(join(tempDir, '2_add_email.up.sql'), 'ALTER TABLE users ADD COLUMN email TEXT')

    const migrations = loadMigrations(tempDir)
    expect(migrations).toHaveLength(2)
    expect(migrations[0].version).toBe(1)
    expect(migrations[1].version).toBe(2)
    expect(typeof migrations[0].up).toBe('string')
  })
})
