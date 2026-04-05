import { describe, expect, it } from 'vitest'
import { ReplicationError } from '../errors.js'
import { generateNodeId, validateNodeId } from '../node-id.js'

describe('node-id', () => {
  it('generates a 32-character hex string', () => {
    const id = generateNodeId()
    expect(id).toMatch(/^[0-9a-f]{32}$/)
  })

  it('generates 1000 unique IDs', () => {
    const ids = new Set<string>()
    for (let i = 0; i < 1000; i++) {
      ids.add(generateNodeId())
    }
    expect(ids.size).toBe(1000)
  })

  it('validates a correct node ID without throwing', () => {
    expect(() => validateNodeId('abcdef0123456789abcdef0123456789')).not.toThrow()
  })

  it('rejects an ID that is too short', () => {
    expect(() => validateNodeId('abcdef')).toThrow(ReplicationError)
  })

  it('rejects an ID with uppercase letters', () => {
    expect(() => validateNodeId('ABCDEF0123456789ABCDEF0123456789')).toThrow(ReplicationError)
  })

  it('rejects an ID with non-hex characters', () => {
    expect(() => validateNodeId('zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')).toThrow(ReplicationError)
  })

  it('rejects an empty string', () => {
    expect(() => validateNodeId('')).toThrow(ReplicationError)
  })
})
