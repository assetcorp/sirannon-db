import { describe, expect, it } from 'vitest'
import { preferredBackupNode } from '../../backup/preferred-node.js'

const GROUP = { primaryNodeId: 'node-a', nodeIds: ['node-c', 'node-a', 'node-b'] }

describe('the node a replication group backs up from', () => {
  it('leaves the primary serving writes and picks a replica instead', () => {
    expect(preferredBackupNode(GROUP, 'replica')).toBe('node-b')
  })

  it('picks the same replica on every node, whatever order the group listed them in', () => {
    const reversed = { primaryNodeId: 'node-a', nodeIds: ['node-b', 'node-c', 'node-a'] }

    expect(preferredBackupNode(reversed, 'replica')).toBe(preferredBackupNode(GROUP, 'replica'))
  })

  it('falls back to the primary where the group has no other node', () => {
    expect(preferredBackupNode({ primaryNodeId: 'only', nodeIds: ['only'] }, 'replica')).toBe('only')
  })

  it('names nobody where every node of the group is out of service', () => {
    expect(preferredBackupNode({ primaryNodeId: 'node-a', nodeIds: [] }, 'replica')).toBeNull()
  })

  it('names the primary where the operator pins the backups to it', () => {
    expect(preferredBackupNode(GROUP, 'primary')).toBe('node-a')
  })

  it('names nobody where the operator pins the backups to a primary the group currently lacks', () => {
    expect(preferredBackupNode({ primaryNodeId: null, nodeIds: ['node-b'] }, 'primary')).toBeNull()
  })

  it('names nobody where the primary the operator pinned them to is out of service', () => {
    expect(preferredBackupNode({ primaryNodeId: 'node-a', nodeIds: ['node-b'] }, 'primary')).toBeNull()
  })

  it('names the node the operator pinned them to', () => {
    expect(preferredBackupNode(GROUP, { nodeId: 'node-c' })).toBe('node-c')
  })
})
