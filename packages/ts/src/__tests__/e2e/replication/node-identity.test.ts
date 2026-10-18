import { describe, expect, it } from 'vitest'
import { ReplicationError, validateNodeId } from '../../../replication/index.js'
import { ORDERS_SCHEMA, PRIMARY, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

const SPEC_NODE_ID = /^[0-9a-f]{32}$/

describe('node identity', () => {
  it('gives each primary started without a node ID its own generated ID of 32 lowercase hex characters', async () => {
    const { scene } = handle
    const first = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], config: { nodeId: undefined } })
    const second = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], config: { nodeId: undefined } })

    const firstId = first.engine.status().nodeId
    const secondId = second.engine.status().nodeId

    expect(firstId).toMatch(SPEC_NODE_ID)
    expect(secondId).toMatch(SPEC_NODE_ID)
    expect(firstId).not.toBe(secondId)
  })

  it('accepts a node ID of exactly 32 lowercase hex characters', () => {
    expect(() => validateNodeId('0123456789abcdef0123456789abcdef')).not.toThrow()
  })

  it.each([
    ['31 characters', '0123456789abcdef0123456789abcde'],
    ['33 characters', '0123456789abcdef0123456789abcdef0'],
    ['uppercase hex', '0123456789ABCDEF0123456789ABCDEF'],
    ['a character outside hex', '0123456789abcdef0123456789abcdeg'],
    ['a trailing newline', '0123456789abcdef0123456789abcdef\n'],
    ['a leading space', ' 0123456789abcdef0123456789abcde'],
    ['nothing', ''],
  ])('refuses a node ID with %s', (_label, candidate) => {
    expect(() => validateNodeId(candidate)).toThrow(ReplicationError)
  })
})
