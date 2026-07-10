import { describe, expect, it } from 'vitest'
import { isReservedIdentifier, reservedSqlError } from '../internal-tables.js'

describe('reservedSqlError', () => {
  it('allows ordinary user SQL', () => {
    expect(reservedSqlError('SELECT * FROM users WHERE id = ?')).toBeNull()
    expect(reservedSqlError('INSERT INTO orders (total) VALUES (10)')).toBeNull()
    expect(reservedSqlError('SELECT * FROM my_sqlite_backup')).toBeNull()
    expect(reservedSqlError('SELECT * FROM orders_sirannon_archive')).toBeNull()
  })

  it('blocks references to the internal change log in any case', () => {
    expect(reservedSqlError('SELECT * FROM _sirannon_changes')).not.toBeNull()
    expect(reservedSqlError('select * from _SIRANNON_CHANGES')).not.toBeNull()
    expect(reservedSqlError('DELETE FROM _sirannon_applied_changes')).not.toBeNull()
  })

  it('blocks the reserved sqlite_ namespace', () => {
    expect(reservedSqlError('SELECT * FROM sqlite_master')).not.toBeNull()
    expect(reservedSqlError('SELECT * FROM sqlite_schema')).not.toBeNull()
    expect(reservedSqlError('UPDATE sqlite_sequence SET seq = 0')).not.toBeNull()
  })

  it('sees through every identifier quoting form', () => {
    expect(reservedSqlError('SELECT * FROM "_sirannon_changes"')).not.toBeNull()
    expect(reservedSqlError('SELECT * FROM [_sirannon_changes]')).not.toBeNull()
    expect(reservedSqlError('SELECT * FROM `_sirannon_changes`')).not.toBeNull()
  })

  it('does not trip on the prefix inside string literals or comments', () => {
    expect(reservedSqlError("SELECT * FROM users WHERE name = '_sirannon_changes'")).toBeNull()
    expect(reservedSqlError('SELECT 1 /* _sirannon_changes */')).toBeNull()
    expect(reservedSqlError('SELECT 1 -- sqlite_master\n')).toBeNull()
  })

  it('blocks ATTACH and DETACH as the leading keyword of any statement', () => {
    expect(reservedSqlError("ATTACH DATABASE 'other.db' AS other")).not.toBeNull()
    expect(reservedSqlError('DETACH DATABASE other')).not.toBeNull()
    expect(reservedSqlError("  \n /* c */ ATTACH DATABASE 'x' AS y")).not.toBeNull()
    expect(reservedSqlError("SELECT 1; ATTACH DATABASE 'x' AS y")).not.toBeNull()
  })

  it('does not treat a non-leading attach token as the statement keyword', () => {
    expect(reservedSqlError('SELECT attach FROM attachments')).toBeNull()
  })

  it('blocks a reserved reference in a trailing statement', () => {
    expect(reservedSqlError('SELECT 1; DROP TABLE _sirannon_changes')).not.toBeNull()
  })
})

describe('isReservedIdentifier', () => {
  it('recognises reserved prefixes case-insensitively', () => {
    expect(isReservedIdentifier('_sirannon_changes')).toBe(true)
    expect(isReservedIdentifier('_SIRANNON_meta')).toBe(true)
    expect(isReservedIdentifier('sqlite_master')).toBe(true)
    expect(isReservedIdentifier('SQLITE_SEQUENCE')).toBe(true)
  })

  it('leaves ordinary identifiers alone', () => {
    expect(isReservedIdentifier('users')).toBe(false)
    expect(isReservedIdentifier('my_sqlite')).toBe(false)
    expect(isReservedIdentifier('orders_sirannon')).toBe(false)
  })
})
