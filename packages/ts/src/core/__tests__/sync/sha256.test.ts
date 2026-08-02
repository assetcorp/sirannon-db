import { createHash } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { sha256Hex } from '../../sync/sha256.js'

function nodeSha256(input: string): string {
  return createHash('sha256').update(input).digest('hex')
}

describe('sha256Hex', () => {
  it('matches the reference vectors', () => {
    expect(sha256Hex('')).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
    expect(sha256Hex('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
  })

  it('matches node:crypto across padding boundaries and multibyte input', () => {
    const inputs = [
      'a'.repeat(55),
      'a'.repeat(56),
      'a'.repeat(63),
      'a'.repeat(64),
      'a'.repeat(65),
      'a'.repeat(1000),
      'héllo wörld — 東京 🚀',
      JSON.stringify({ table: 'notes', rowId: '10', data: { body: 'ü' } }),
    ]
    for (const input of inputs) {
      expect(sha256Hex(input)).toBe(nodeSha256(input))
    }
  })
})
