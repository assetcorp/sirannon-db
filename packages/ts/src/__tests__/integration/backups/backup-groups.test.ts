import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { BackupNodePreference } from '../../../core/backup/preferred-node.js'
import {
  addOrders,
  type CycleNode,
  changeRecords,
  controlledGroup,
  createOrders,
  expectRestoresTo,
  onlyChainId,
  openCycleNode,
} from './cycle-harness.js'
import {
  CHAIN_LIST_NAME,
  codeOf,
  createWorkspace,
  directoryDestination,
  filesIn,
  gate,
  integrityOf,
  orderLabels,
  replayStoredChain,
  storedChain,
  storedHeads,
  type Workspace,
  waitFor,
  walBytes,
} from './harness.js'

const NODES = ['node-a', 'node-b', 'node-c']

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('backup-groups')
})

afterEach(async () => {
  await workspace.dispose()
})

function stateFileIn(node: CycleNode): boolean {
  return existsSync(join(node.stagingDir, 'cycle.json'))
}

function stagedCaptures(node: CycleNode): string[] {
  return filesIn(node.stagingDir).filter(name => name.endsWith('.wal'))
}

describe('backups across the nodes of one replication group', () => {
  it('lets only the node the group names write, whatever order each node lists the group in, while the rest skip and empty their logs', async () => {
    const bucket = workspace.path('bucket')
    const listings = [
      ['node-c', 'node-a', 'node-b'],
      ['node-b', 'node-c', 'node-a'],
      ['node-a', 'node-b', 'node-c'],
    ]
    const preferred = [...NODES].sort().find(nodeId => nodeId !== 'node-a')
    const nodes: CycleNode[] = []
    for (const [at, nodeId] of NODES.entries()) {
      nodes.push(
        await openCycleNode(workspace, nodeId, {
          destination: directoryDestination(bucket),
          backups: {
            replicationGroup: controlledGroup(nodeId, { primaryNodeId: 'node-a', nodeIds: listings[at] ?? [] }),
          },
        }),
      )
    }
    for (const node of nodes) {
      await createOrders(node.db)
      await addOrders(node.db, [`written-on-${node.db.id}`])
      await node.db.captureBackupChanges()
    }

    const destination = directoryDestination(bucket)
    expect(preferred).toBe('node-b')
    expect(storedHeads(destination)).toHaveLength(1)
    for (const node of nodes) {
      const takesTheTurn = node.db.id === preferred
      expect(node.skips.length > 0, node.db.id).toBe(!takesTheTurn)
      expect(node.runs.length > 0, node.db.id).toBe(takesTheTurn)
      expect(walBytes(node.path), node.db.id).toBe(0)
      if (takesTheTurn) continue
      expect(node.skips.at(-1), node.db.id).toMatchObject({
        reason: 'not-preferred',
        nodeId: node.db.id,
        preferredNodeId: preferred,
      })
      expect(stateFileIn(node), node.db.id).toBe(false)
    }
    await expectRestoresTo(workspace, destination, ['written-on-node-b'], 'group')
  })

  it.each<[string, BackupNodePreference, string[], string | null]>([
    ['the primary when asked', 'primary', ['node-a', 'node-b'], 'node-a'],
    ['the primary in a group with no other eligible node', 'replica', ['node-a'], 'node-a'],
    ['no node while the primary the operator asked for is out of service', 'primary', ['node-b'], null],
    ['no node while every node is out of service', 'replica', [], null],
  ])('puts the backups on %s', async (_label, preferredNode, nodeIds, expected) => {
    const bucket = workspace.path('bucket')
    const nodes: CycleNode[] = []
    for (const nodeId of ['node-a', 'node-b']) {
      const node = await openCycleNode(workspace, nodeId, {
        destination: directoryDestination(bucket),
        backups: { preferredNode, replicationGroup: controlledGroup(nodeId, { primaryNodeId: 'node-a', nodeIds }) },
      })
      await node.db.captureBackupChanges()
      nodes.push(node)
    }

    expect(nodes.filter(node => node.runs.length > 0).map(node => node.db.id)).toEqual(expected ? [expected] : [])
    expect(storedHeads(directoryDestination(bucket))).toHaveLength(expected ? 1 : 0)
    if (expected === null) {
      for (const node of nodes) {
        expect(node.skips.at(-1)?.message).toContain('names no node to back it up')
        expect(node.skips.at(-1)?.preferredNodeId).toBeUndefined()
      }
    }
  })

  it('holds the frames of a node cut off from its group, reports why and how much log it holds, and sends them later', async () => {
    const group = controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
    const node = await openCycleNode(workspace, 'node-b', { backups: { replicationGroup: group } })
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-outage'])
    await node.db.captureBackupChanges()
    await addOrders(node.db, ['during-the-outage'])
    group.failure = new Error('etcd deadline exceeded')

    const skipped = await node.db.captureBackupChanges()

    const held = walBytes(node.path)
    expect(skipped).toBeUndefined()
    expect(held).toBeGreaterThan(0)
    expect(node.skips.at(-1)).toMatchObject({ reason: 'group-unavailable', nodeId: 'node-b', uncapturedLogBytes: held })
    expect(node.skips.at(-1)?.message).toContain('etcd deadline exceeded')
    expect(node.errors).toEqual([])

    group.failure = null
    const resumed = await node.db.captureBackupChanges()

    expect(resumed?.kind).toBe('change')
    expect(storedHeads(node.destination)).toHaveLength(1)
    await expectRestoresTo(workspace, node.destination, ['before-the-outage', 'during-the-outage'], 'outage')
  })

  it('warns once, as the cycle starts, when a group shares a destination that cannot claim a place in the list', async () => {
    for (const claims of [false, true]) {
      const node = await openCycleNode(workspace, `claims-${claims}`, {
        destination: workspace.destination(`bucket-${claims}`, { claims }),
        backups: {
          replicationGroup: controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] }),
        },
      })
      await createOrders(node.db)
      await node.db.captureBackupChanges()
      await node.db.captureBackupChanges()

      const warnings = node.errors.filter(error => error.message.includes('writePieceIfAbsent'))
      expect(warnings.map(codeOf), `claims: ${claims}`).toEqual(claims ? [] : ['BACKUP_DESTINATION_ERROR'])
      expect(storedHeads(node.destination), `claims: ${claims}`).toHaveLength(1)
    }
  })

  it('stands down when a failover moves the backups away, and starts a fresh chain once they come back', async () => {
    const group = controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
    const node = await openCycleNode(workspace, 'node-b', { backups: { replicationGroup: group } })
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-failover'])
    await node.db.captureBackupChanges()
    const first = onlyChainId(node.destination)

    group.membership = { primaryNodeId: 'node-b', nodeIds: ['node-a', 'node-b'] }
    await addOrders(node.db, ['while-node-b-is-primary'])
    await node.db.captureBackupChanges()
    expect(stateFileIn(node)).toBe(false)
    expect(walBytes(node.path)).toBe(0)

    group.membership = { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] }
    const back = await node.db.captureBackupChanges()

    const heads = storedHeads(node.destination)
    expect(back?.kind).toBe('full')
    expect(heads.map(head => head.chainId)[0]).toBe(first)
    expect(heads).toHaveLength(2)
    expect(heads[1]?.previousChainId).toBeUndefined()
    await expectRestoresTo(workspace, node.destination, ['before-the-failover', 'while-node-b-is-primary'], 'failover')
  })

  it('sends its staged capture before it stands down, keeps it while the destination refuses, and offers it once a turn', async () => {
    const group = controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
    const node = await openCycleNode(workspace, 'node-b', {
      backups: { replicationGroup: group, maxUncapturedLogBytes: 1_000_000_000 },
    })
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    const chainId = onlyChainId(node.destination)
    await addOrders(node.db, ['staged-before-standing-down'])
    let offers = 0
    node.destination.faults.holdWrite = name => {
      if (name.endsWith('.wal')) offers++
      return undefined
    }
    node.destination.faults.refuseWrite = name => name.endsWith('.wal')
    await node.db.captureBackupChanges().catch(() => undefined)
    group.membership = { primaryNodeId: 'node-a', nodeIds: ['node-a'] }
    offers = 0

    await node.db.captureBackupChanges()

    expect(offers).toBe(1)
    expect(stagedCaptures(node)).toHaveLength(1)
    expect(stateFileIn(node)).toBe(true)
    expect(node.errors.at(-1)?.message).toContain('the bucket refused piece 0')

    node.destination.faults.refuseWrite = undefined
    await node.db.captureBackupChanges()

    expect(stagedCaptures(node)).toEqual([])
    expect(stateFileIn(node)).toBe(false)
    expect(walBytes(node.path)).toBe(0)
    expect(changeRecords(storedChain(node.destination, chainId)).map(record => record.sequence)).toEqual([1, 2])
    await expectRestoresTo(workspace, node.destination, ['staged-before-standing-down'], 'stood-down')
  })

  it('asks its group nothing where the operator pinned the backups to one node, so an outage stops nothing', async () => {
    const bucket = workspace.path('bucket')
    const groups = ['node-a', 'node-b'].map(nodeId => {
      const group = controlledGroup(nodeId, { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
      group.failure = new Error('etcd is down')
      return group
    })
    const nodes: CycleNode[] = []
    for (const group of groups) {
      const node = await openCycleNode(workspace, group.nodeId, {
        destination: directoryDestination(bucket),
        backups: { replicationGroup: group, preferredNode: { nodeId: 'node-a' } },
      })
      await createOrders(node.db)
      await addOrders(node.db, [`pinned-${group.nodeId}`])
      await node.db.captureBackupChanges()
      nodes.push(node)
    }

    expect(groups.map(group => group.reads)).toEqual([0, 0])
    expect(nodes.map(node => node.runs.length > 0)).toEqual([true, false])
    expect(nodes[1]?.skips.at(-1)).toMatchObject({ reason: 'not-preferred', preferredNodeId: 'node-a' })
    await expectRestoresTo(workspace, directoryDestination(bucket), ['pinned-node-a'], 'pinned')
  })

  it('keeps a held log at maxUncapturedLogBytes, empties one a byte past it with BACKUP_CHAIN_BROKEN, and starts afresh', async () => {
    const cutOff = async (name: string, maxUncapturedLogBytes?: number) => {
      const group = controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
      const node = await openCycleNode(workspace, name, {
        destination: workspace.destination(`${name}-bucket`),
        backups: { replicationGroup: group, ...(maxUncapturedLogBytes === undefined ? {} : { maxUncapturedLogBytes }) },
      })
      await createOrders(node.db)
      await addOrders(node.db, ['captured'])
      await node.db.captureBackupChanges()
      group.failure = new Error('etcd deadline exceeded')
      await addOrders(node.db, ['held-while-cut-off'])
      await node.db.captureBackupChanges()
      return { node, group }
    }

    const unbounded = await cutOff('unbounded')
    const held = unbounded.node.skips.at(-1)?.uncapturedLogBytes ?? 0
    const atTheLimit = await cutOff('at-the-limit', held)
    const pastTheLimit = await cutOff('past-the-limit', held - 1)

    expect(held).toBeGreaterThan(0)
    expect(walBytes(unbounded.node.path)).toBe(held)
    expect(atTheLimit.node.errors).toEqual([])
    expect(walBytes(atTheLimit.node.path)).toBe(held)
    expect(stateFileIn(atTheLimit.node)).toBe(true)
    expect(pastTheLimit.node.errors.map(codeOf)).toEqual(['BACKUP_CHAIN_BROKEN'])
    expect(pastTheLimit.node.errors[0]?.message).toContain('in no backup')
    expect(pastTheLimit.node.errors[0]?.message).toContain(`maxUncapturedLogBytes of ${held - 1}`)
    expect(pastTheLimit.node.db.backupStatus().lastError?.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(walBytes(pastTheLimit.node.path)).toBe(0)
    expect(stateFileIn(pastTheLimit.node)).toBe(false)

    pastTheLimit.group.failure = null
    const fresh = await pastTheLimit.node.db.captureBackupChanges()

    expect(fresh?.kind).toBe('full')
    expect(storedHeads(pastTheLimit.node.destination)).toHaveLength(2)
    await expectRestoresTo(workspace, pastTheLimit.node.destination, ['captured', 'held-while-cut-off'], 'limit')
  })

  it('holds a log that captures normally, however small the limit the operator sets', async () => {
    const node = await openCycleNode(workspace, 'tiny-limit', { backups: { maxUncapturedLogBytes: 0 } })
    await createOrders(node.db)
    await addOrders(node.db, ['captured-normally'])

    const report = await node.db.captureBackupChanges()

    expect(report?.kind).toBe('change')
    expect(node.errors).toEqual([])
    expect(storedHeads(node.destination)).toHaveLength(1)
    await expectRestoresTo(workspace, node.destination, ['captured-normally'], 'tiny-limit')
  })

  it('skips a turn that falls due while the previous one is still running, and says why', async () => {
    const held = gate()
    const node = await openCycleNode(workspace, 'overlapping', { backups: { intervalMs: 20 } })
    await createOrders(node.db)
    await node.db.captureBackupChanges()
    node.destination.faults.holdWrite = name => (name.endsWith('.wal') ? held.promise : undefined)
    await addOrders(node.db, ['held-in-transfer'])

    const running = node.db.captureBackupChanges()
    try {
      await waitFor(
        () => node.skips.some(skip => skip.reason === 'previous-run-active'),
        'a turn skipped behind a running one',
      )
    } finally {
      held.open()
    }
    await running

    const skip = node.skips.find(candidate => candidate.reason === 'previous-run-active')
    expect(skip?.message).toContain('still running its previous turn')
    expect(skip?.uncapturedLogBytes).toEqual(expect.any(Number))
    expect(node.errors).toEqual([])
    await expectRestoresTo(workspace, node.destination, ['held-in-transfer'], 'overlapping')
  })
})

describe('the list of chains at a destination', () => {
  it('adds a new chain after the highest place an operator left in the list', async () => {
    const destination = workspace.destination()
    destination.replace(
      CHAIN_LIST_NAME,
      5,
      Buffer.from(JSON.stringify({ chainId: 'kept-by-the-operator', startedAt: 1 })),
    )

    const node = await openCycleNode(workspace, 'appended', { destination })
    await node.db.captureBackupChanges()

    expect(destination.indices(CHAIN_LIST_NAME)).toEqual([5, 6])
    expect(storedHeads(destination).map(head => head.chainId)).toEqual(['kept-by-the-operator', node.runs[0]?.chainId])
    expect(node.errors).toEqual([])
  })

  it('gives up with BACKUP_DESTINATION_ERROR, naming the chain, when the destination reports every place taken', async () => {
    const destination = workspace.destination()
    destination.faults.claimTaken = name => name === CHAIN_LIST_NAME

    const node = await openCycleNode(workspace, 'crowded-out', { destination })
    await waitFor(() => node.errors.length > 0, 'the chain to give up on a place in the list')

    expect(node.errors.map(codeOf)).toEqual(['BACKUP_DESTINATION_ERROR'])
    expect(node.errors[0]?.message).toContain('Another chain took every one of the 8 places')
    const orphanedLogs = destination.names().filter(name => name.startsWith(`${CHAIN_LIST_NAME}.`))
    expect(orphanedLogs).toHaveLength(1)
    expect(node.errors[0]?.message).toContain(`chain '${orphanedLogs[0]?.slice(CHAIN_LIST_NAME.length + 1)}'`)
    expect(storedHeads(destination)).toEqual([])
    expect(node.runs).toEqual([])
  })
})

describe('two nodes that start a chain on one destination at the same moment', () => {
  async function racingNodes(claims: boolean): Promise<[CycleNode, CycleNode]> {
    const bucket = workspace.path(`bucket-${claims}`)
    const both = gate()
    let arrivals = 0
    const listedTogether = (name: string) => {
      if (name !== CHAIN_LIST_NAME || arrivals >= 2) return undefined
      arrivals++
      if (arrivals === 2) both.open()
      return both.promise
    }
    const firstReadBack = gate()
    const first = directoryDestination(bucket, { claims })
    const second = directoryDestination(bucket, { claims })
    first.faults.holdList = listedTogether
    second.faults.holdList = listedTogether
    if (!claims) {
      first.faults.afterRead = name => {
        if (name === CHAIN_LIST_NAME) firstReadBack.open()
      }
      second.faults.holdWrite = name => (name === CHAIN_LIST_NAME ? firstReadBack.promise : undefined)
    }
    const [one, two] = await Promise.all([
      openCycleNode(workspace, `racer-one-${claims}`, { destination: first }),
      openCycleNode(workspace, `racer-two-${claims}`, { destination: second }),
    ])
    await waitFor(() => one.runs.length > 0 && two.runs.length > 0, 'both full copies to join the list')
    for (const node of [one, two]) {
      await createOrders(node.db)
      await addOrders(node.db, [`from-${node.db.id}`])
    }
    return [one, two]
  }

  it('keeps both chains in the list when the destination can claim a place', async () => {
    const [one, two] = await racingNodes(true)
    await one.db.captureBackupChanges()
    await two.db.captureBackupChanges()

    const heads = storedHeads(one.destination)
    expect(heads).toHaveLength(2)
    expect([...one.errors, ...two.errors]).toEqual([])
    for (const node of [one, two]) {
      const chainId = node.runs[0]?.chainId ?? ''
      expect(heads.map(head => head.chainId)).toContain(chainId)
      const rebuilt = workspace.path(`${node.db.id}-rebuilt.db`)
      replayStoredChain(node.destination, storedChain(node.destination, chainId), rebuilt)
      expect(integrityOf(rebuilt)).toBe('ok')
      expect(orderLabels(rebuilt)).toEqual([`from-${node.db.id}`])
    }
  })

  it('loses one chain from the list without a claim, and the losing node reports it and starts a fresh chain', async () => {
    const [one, two] = await racingNodes(false)
    const lost = one.runs[0]?.chainId ?? ''

    await one.db.captureBackupChanges()
    await two.db.captureBackupChanges()

    const heads = storedHeads(one.destination).map(head => head.chainId)
    expect(heads).not.toContain(lost)
    expect(one.errors.map(codeOf)).toEqual(['BACKUP_CHAIN_BROKEN'])
    expect(one.errors[0]?.message).toContain(lost)
    expect(two.errors).toEqual([])
    expect(heads).toEqual(expect.arrayContaining([two.runs[0]?.chainId, one.runs.at(-1)?.chainId]))
    const fresh = workspace.path('fresh-rebuilt.db')
    replayStoredChain(one.destination, storedChain(one.destination, one.runs.at(-1)?.chainId ?? ''), fresh)
    expect(orderLabels(fresh)).toEqual([`from-${one.db.id}`])
  })
})
