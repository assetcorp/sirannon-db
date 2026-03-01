import { describe, expect, it } from 'vitest'
import { toExecuteResponse } from '../protocol.js'

describe('toExecuteResponse', () => {
  it('passes through numeric lastInsertRowId', () => {
    const result = toExecuteResponse({ changes: 3, lastInsertRowId: 42 })
    expect(result).toEqual({ changes: 3, lastInsertRowId: 42 })
  })

  it('converts bigint lastInsertRowId to string', () => {
    const result = toExecuteResponse({
      changes: 1,
      lastInsertRowId: 9007199254740993n,
    })
    expect(result.lastInsertRowId).toBe('9007199254740993')
    expect(typeof result.lastInsertRowId).toBe('string')
  })

  it('preserves zero values', () => {
    const result = toExecuteResponse({ changes: 0, lastInsertRowId: 0 })
    expect(result).toEqual({ changes: 0, lastInsertRowId: 0 })
  })

  it('converts bigint zero to string', () => {
    const result = toExecuteResponse({ changes: 0, lastInsertRowId: 0n })
    expect(result.lastInsertRowId).toBe('0')
  })
})
