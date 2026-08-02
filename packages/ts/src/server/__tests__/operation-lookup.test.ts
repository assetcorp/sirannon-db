import { describe, expect, it } from 'vitest'
import { resolveArguments } from '../operation-lookup.js'

interface Identity {
  tenantId: string
}

describe('resolveArguments', () => {
  it('reports a declared argument named after an inherited member as missing', () => {
    for (const name of ['constructor', 'toString', 'valueOf', 'hasOwnProperty']) {
      const resolution = resolveArguments<Identity>({ args: [name] }, {}, undefined)
      expect(resolution.ok, name).toBe(false)
      if (resolution.ok) continue
      expect(resolution.refusal.code, name).toBe('MISSING_ARGUMENT')
    }
  })

  it('takes a supplied argument named after an inherited member', () => {
    const resolution = resolveArguments<Identity>({ args: ['constructor'] }, { constructor: 'acme' }, undefined)
    expect(resolution.ok).toBe(true)
    if (!resolution.ok) return
    expect(resolution.value).toEqual({ constructor: 'acme' })
  })

  it('reports an inherited member the caller supplies as undeclared rather than identity-filled', () => {
    const resolution = resolveArguments<Identity>(
      { args: ['status'], fromIdentity: { tenant: 'tenantId' } },
      { status: 'open', toString: 'x' },
      { tenantId: 'acme' },
    )

    expect(resolution.ok).toBe(false)
    if (resolution.ok) return
    expect(resolution.refusal.message).toMatch(/not declared by this operation/)
  })

  it('still refuses an argument the identity fills', () => {
    const resolution = resolveArguments<Identity>(
      { args: [], fromIdentity: { tenant: 'tenantId' } },
      { tenant: 'other' },
      { tenantId: 'acme' },
    )

    expect(resolution.ok).toBe(false)
    if (resolution.ok) return
    expect(resolution.refusal.code).toBe('ARGUMENT_NOT_ALLOWED')
    expect(resolution.refusal.message).toMatch(/authenticated identity/)
  })
})
