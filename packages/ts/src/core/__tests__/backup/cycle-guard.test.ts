import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { decideBackupTurn, logGrownPastLimit, previousRunStillActive } from '../../backup/cycle-guard.js'
import type { BackupGroupSource } from '../../backup/preferred-node.js'
import { tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()

const GROUP = { primaryNodeId: 'node-a', nodeIds: ['node-c', 'node-a', 'node-b'] }

function source(overrides?: Partial<BackupGroupSource>): BackupGroupSource {
  return {
    nodeId: 'node-b',
    readMembership: async () => GROUP,
    ...overrides,
  }
}

describe('the question a turn asks before it copies anything', () => {
  it('answers yes on a database that belongs to no group', async () => {
    expect(await decideBackupTurn(undefined, 'replica')).toEqual({ runs: true })
  })

  it('answers yes on the node the group names', async () => {
    expect((await decideBackupTurn(source(), 'replica')).runs).toBe(true)
  })

  it('names the node whose turn it was where this node stands down', async () => {
    const decision = await decideBackupTurn(source({ nodeId: 'node-c' }), 'replica')

    expect(decision.runs).toBe(false)
    expect(decision.skip?.reason).toBe('not-preferred')
    expect(decision.skip?.preferredNodeId).toBe('node-b')
    expect(decision.skip?.message).toContain("Node 'node-b' takes this replication group's backups")
  })

  it('stands down where the group names no backup node at all', async () => {
    const empty = source({ nodeId: 'node-a', readMembership: async () => ({ primaryNodeId: null, nodeIds: [] }) })
    const decision = await decideBackupTurn(empty, 'replica')

    expect(decision.skip?.reason).toBe('not-preferred')
    expect(decision.skip?.preferredNodeId).toBeUndefined()
    expect(decision.skip?.message).toContain('names no node to back it up')
  })

  it('reports the reason it could not read the membership, and takes no backup', async () => {
    const unreachable = source({
      readMembership: () => Promise.reject(new Error('etcd deadline exceeded')),
    })
    const decision = await decideBackupTurn(unreachable, 'replica')

    expect(decision.runs).toBe(false)
    expect(decision.skip?.reason).toBe('group-unavailable')
    expect(decision.skip?.message).toContain('etcd deadline exceeded')
    expect(decision.skip?.nodeId).toBe('node-b')
  })

  it('answers a pinned node without reading the membership, so a coordinator outage stops nothing', async () => {
    const readMembership = vi.fn(() => Promise.reject(new Error('etcd is down')))
    const decision = await decideBackupTurn(source({ readMembership }), { nodeId: 'node-b' })

    expect(decision.runs).toBe(true)
    expect(readMembership).not.toHaveBeenCalled()
  })

  it('describes the turn it drops behind one that is still running', () => {
    expect(previousRunStillActive().reason).toBe('previous-run-active')
  })
})

describe('the log a turn captured nothing from', () => {
  it('measures nothing on a database that has written no log yet', async () => {
    expect(await logGrownPastLimit(join(temp.path, 'source.db-wal'), 4096, 'source')).toBeNull()
  })

  it('reports a filesystem that refuses to measure the log, rather than passing it as small enough', async () => {
    const file = join(temp.path, 'source.db')
    writeFileSync(file, 'not a directory')

    await expect(logGrownPastLimit(join(file, 'source.db-wal'), 4096, 'source')).rejects.toThrow()
  })

  it('measures nothing where the operator set no limit', async () => {
    expect(await logGrownPastLimit(join(temp.path, 'source.db-wal'), undefined, 'source')).toBeNull()
  })
})
