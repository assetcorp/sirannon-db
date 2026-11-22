import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  addOrders,
  controlledGroup,
  createOrders,
  expectRestoresTo,
  holdReadTransaction,
  openCycleNode,
} from './cycle-harness.js'
import { createWorkspace, orderLabels, PLACEMENTS, storedHeads, type Workspace, walBytes } from './harness.js'

const EMPTIED = { busy: false, framesInLog: 0, framesCheckpointed: 0, emptied: true }
const QUICK_BUSY_TIMEOUT_MS = 20

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('cycle-checkpoint')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(PLACEMENTS)('checkpoint on a database with backups and $label', ({ driver, writerWorker }) => {
  it('captures every write before it empties the log, on the first call, on later calls, and with nothing new to capture', async () => {
    const node = await openCycleNode(workspace, 'ledger', { driver: driver(), writerWorker })
    await createOrders(node.db)
    await addOrders(node.db, ['first', 'second'])

    expect(await node.db.checkpoint()).toEqual(EMPTIED)
    expect(walBytes(node.path)).toBe(0)
    const runsAfterFirst = node.runs.length
    expect(node.runs.at(-1)?.kind).toBe('change')

    await addOrders(node.db, ['third'])
    expect(await node.db.checkpoint()).toEqual(EMPTIED)
    expect(walBytes(node.path)).toBe(0)
    expect(node.runs.length).toBe(runsAfterFirst + 1)
    expect(node.runs.at(-1)?.kind).toBe('change')

    expect(await node.db.checkpoint()).toEqual(EMPTIED)
    expect(node.runs.length).toBe(runsAfterFirst + 1)

    expect(node.errors).toEqual([])
    expect(storedHeads(node.destination)).toHaveLength(1)
    await node.db.close()
    expect(orderLabels(node.path)).toEqual(['first', 'second', 'third'])
    await expectRestoresTo(workspace, node.destination, ['first', 'second', 'third'], 'ledger')
  })
})

describe('checkpoint on a database with backups that cannot finish the checkpoint', () => {
  it('reports busy while a reader holds the log, keeps the captured frames, and loses no write from the backups', async () => {
    const node = await openCycleNode(workspace, 'held', {
      driver: betterSqlite3({ busyTimeout: QUICK_BUSY_TIMEOUT_MS }),
    })
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-reader'])
    await node.db.checkpoint()
    const reader = holdReadTransaction(node.path)
    try {
      await addOrders(node.db, ['while-the-reader-holds'])

      const held = await node.db.checkpoint()

      expect(held.busy).toBe(true)
      expect(held.emptied).toBe(false)
      expect(held.framesInLog).toBeGreaterThan(0)
      expect(walBytes(node.path)).toBeGreaterThan(0)
    } finally {
      reader.release()
    }

    await addOrders(node.db, ['after-the-reader'])
    expect(await node.db.checkpoint()).toEqual(EMPTIED)
    expect(walBytes(node.path)).toBe(0)
    expect(node.errors).toEqual([])
    await node.db.close()
    await expectRestoresTo(
      workspace,
      node.destination,
      ['before-the-reader', 'while-the-reader-holds', 'after-the-reader'],
      'held',
    )
  })

  it('fails with BACKUP_ERROR and leaves the log in place while the replication group is out of reach', async () => {
    const group = controlledGroup('node-b', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
    const node = await openCycleNode(workspace, 'node-b', { backups: { replicationGroup: group } })
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-outage'])
    await node.db.checkpoint()
    await addOrders(node.db, ['during-the-outage'])
    group.failure = new Error('etcd deadline exceeded')
    const held = walBytes(node.path)

    await expect(node.db.checkpoint()).rejects.toMatchObject({
      code: 'BACKUP_ERROR',
      message: expect.stringContaining('etcd deadline exceeded'),
    })
    expect(walBytes(node.path)).toBe(held)

    group.failure = null
    expect(await node.db.checkpoint()).toEqual(EMPTIED)
    await node.db.close()
    await expectRestoresTo(workspace, node.destination, ['before-the-outage', 'during-the-outage'], 'node-b')
  })

  it('empties the log on a node whose group takes its backups from another node', async () => {
    const group = controlledGroup('node-a', { primaryNodeId: 'node-a', nodeIds: ['node-a', 'node-b'] })
    const node = await openCycleNode(workspace, 'node-a', { backups: { replicationGroup: group } })
    await createOrders(node.db)
    await addOrders(node.db, ['written-on-the-primary'])

    expect(await node.db.checkpoint()).toEqual(EMPTIED)
    expect(walBytes(node.path)).toBe(0)
    expect(node.runs).toEqual([])
    expect(node.skips.length).toBeGreaterThan(0)
    await node.db.close()
    expect(orderLabels(node.path)).toEqual(['written-on-the-primary'])
  })
})
