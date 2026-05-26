import { describe, expect, it } from 'vitest'
import { extractDroppedTable } from '../../engine/constants.js'

describe('extractDroppedTable', () => {
  it('returns the table name for a bare DROP TABLE statement', () => {
    expect(extractDroppedTable('DROP TABLE foo')).toBe('foo')
  })

  it('handles leading whitespace', () => {
    expect(extractDroppedTable('   \tDROP TABLE foo')).toBe('foo')
  })

  it('accepts IF EXISTS with mixed case', () => {
    expect(extractDroppedTable('drop table if exists foo')).toBe('foo')
    expect(extractDroppedTable('DROP TABLE IF EXISTS foo')).toBe('foo')
    expect(extractDroppedTable('Drop Table If Exists Foo')).toBe('Foo')
  })

  it('strips surrounding double quotes from the identifier', () => {
    expect(extractDroppedTable('DROP TABLE "foo"')).toBe('foo')
    expect(extractDroppedTable('DROP TABLE IF EXISTS "users"')).toBe('users')
  })

  it('tolerates a trailing semicolon', () => {
    expect(extractDroppedTable('DROP TABLE foo;')).toBe('foo')
    expect(extractDroppedTable('DROP TABLE foo ;')).toBe('foo')
  })

  it('returns null for non-DROP-TABLE DDL', () => {
    expect(extractDroppedTable('CREATE TABLE foo (id INTEGER PRIMARY KEY)')).toBeNull()
    expect(extractDroppedTable('ALTER TABLE foo ADD COLUMN bar TEXT')).toBeNull()
    expect(extractDroppedTable('DROP INDEX idx_foo')).toBeNull()
    expect(extractDroppedTable('CREATE INDEX idx_foo ON foo (bar)')).toBeNull()
  })

  it('returns null for DROP TABLE with multiple statements separated by semicolons', () => {
    expect(extractDroppedTable('DROP TABLE foo; DROP TABLE bar')).toBeNull()
  })

  it('returns null when the identifier is not a safe identifier', () => {
    expect(extractDroppedTable('DROP TABLE 123abc')).toBeNull()
    expect(extractDroppedTable('DROP TABLE foo-bar')).toBeNull()
    expect(extractDroppedTable('DROP TABLE schema.foo')).toBeNull()
  })

  it('returns null for empty or non-DDL input', () => {
    expect(extractDroppedTable('')).toBeNull()
    expect(extractDroppedTable('SELECT 1')).toBeNull()
    expect(extractDroppedTable('INSERT INTO foo VALUES (1)')).toBeNull()
  })

  it('does not match DROP TABLE inside a longer composite SQL', () => {
    expect(extractDroppedTable('SELECT * FROM foo WHERE x = DROP TABLE bar')).toBeNull()
  })
})
