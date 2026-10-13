import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import {
  addOrders,
  changeRecords,
  createOrders,
  expectRestoresTo,
  holdReadTransaction,
  onlyChainId,
  openCycleNode,
} from './cycle-harness.js'
import {
  CHAIN_LIST_NAME,
  createWorkspace,
  framesIn,
  INSERT_ORDER,
  logHeaderOf,
  ORDER_PADDING,
  orderLabels,
  orderRows,
  PLACEMENTS,
  pause,
  storedChain,
  storedHeads,
  type Workspace,
  waitFor,
  walBytes,
} from './harness.js'

const BUSY_TIMEOUT_MS = 20
const SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES = 1000

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('cycle-chain')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(PLACEMENTS)('the backup cycle with $label', ({ label, driver, writerWorker }) => {
  const build = label.startsWith('node:sqlite') ? 'node:sqlite' : 'better-sqlite3'
  const quickBusyDriver = () =>
    build === 'node:sqlite'
      ? nodeSqlite({ busyTimeout: BUSY_TIMEOUT_MS })
      : betterSqlite3({ busyTimeout: BUSY_TIMEOUT_MS })

  it('starts a chain with a full copy, adds one change piece per turn, and rebuilds to every row', async () => {
    const node = await openCycleNode(workspace, 'ledger', { driver: driver(), writerWorker })
    await createOrders(node.db)
    await addOrders(node.db, ['first', 'second'])
    const firstTurn = await node.db.captureBackupChanges()
    await addOrders(node.db, ['third'])
    const secondTurn = await node.db.captureBackupChanges()
    const idleTurn = await node.db.captureBackupChanges()
    await addOrders(node.db, ['written-before-close'])
    await node.db.close()

    const chainId = onlyChainId(node.destination)
    const records = storedChain(node.destination, chainId)
    const changes = changeRecords(records)
    expect(records[0]).toMatchObject({ kind: 'full', chainId })
    expect(changes.map(record => record.sequence)).toEqual([1, 2, 3])
    expect(idleTurn).toBeUndefined()
    expect([firstTurn?.kind, secondTurn?.kind]).toEqual(['change', 'change'])
    expect(firstTurn?.destinationName).toBe(changes[0]?.name)
    for (const change of changes) {
      const header = logHeaderOf(node.destination.stored(change.name, 0))
      expect(change.position?.firstFrame).toBe(1)
      expect([header.salt1, header.salt2]).toEqual([change.position?.salt1, change.position?.salt2])
      expect(change.frameCount).toBe(framesIn(change.bytesWritten, header.pageSize))
    }
    expect(node.runs.map(run => run.kind)).toEqual(['full', 'change', 'change', 'change'])
    const fullCopyLog = node.runs[0]?.logPosition
    const [firstChange, secondChange] = changes
    expect([fullCopyLog?.salt1, fullCopyLog?.salt2, fullCopyLog?.logSequence]).toEqual([
      firstChange?.position?.salt1,
      firstChange?.position?.salt2,
      firstChange?.position?.logSequence,
    ])
    expect(secondChange?.position?.logSequence).toBe((firstChange?.position?.logSequence ?? 0) + 1)
    expect(secondChange?.position?.salt1).not.toBe(firstChange?.position?.salt1)
    expect(node.errors).toEqual([])
    await expectRestoresTo(workspace, node.destination, ['first', 'second', 'third', 'written-before-close'], 'ledger')
  })

  it('holds every frame a bulk load writes until a turn captures it, then empties the log', async () => {
    const node = await openCycleNode(workspace, 'owned', { driver: driver(), writerWorker })
    await node.db.captureBackupChanges()
    await createOrders(node.db)
    await node.db.bulkLoad(INSERT_ORDER, orderRows('bulk', 20_000, ORDER_PADDING))
    const held = walBytes(node.path)
    const plain = await workspace.registry(driver()).open('plain', workspace.path('plain.db'), { writerWorker })
    await createOrders(plain)
    await plain.bulkLoad(INSERT_ORDER, orderRows('bulk', 20_000, ORDER_PADDING))

    const report = await node.db.captureBackupChanges()

    expect(framesIn(held, 4096)).toBeGreaterThan(SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES)
    expect(walBytes(workspace.path('plain.db'))).toBe(0)
    expect(report?.pageCount).toBe(framesIn(held, 4096))
    expect(walBytes(node.path)).toBe(0)
    await node.db.close()
    await expectRestoresTo(
      workspace,
      node.destination,
      orderRows('bulk', 20_000).map(([value]) => value),
      'owned',
    )
  })

  it('takes the same chain up again after a clean restart', async () => {
    const stagingDir = workspace.path('restart-staging')
    const destination = workspace.destination()
    const first = await openCycleNode(workspace, 'restarted', {
      driver: driver(),
      writerWorker,
      stagingDir,
      destination,
    })
    await createOrders(first.db)
    await addOrders(first.db, ['before-restart'])
    await first.db.captureBackupChanges()
    await addOrders(first.db, ['captured-at-close'])
    await first.db.close()

    const second = await openCycleNode(workspace, 'restarted', {
      driver: driver(),
      writerWorker,
      stagingDir,
      destination,
    })
    await addOrders(second.db, ['after-restart'])
    await second.db.captureBackupChanges()
    await second.db.close()

    const chainId = onlyChainId(destination)
    expect(changeRecords(storedChain(destination, chainId)).map(record => record.sequence)).toEqual([1, 2, 3])
    expect(second.runs.map(run => run.kind)).not.toContain('full')
    expect([...first.errors, ...second.errors]).toEqual([])
    await expectRestoresTo(
      workspace,
      destination,
      ['before-restart', 'captured-at-close', 'after-restart'],
      'restarted',
    )
  })

  it('reads on from the middle of a log that a reader held open, and starts the next log at frame one', async () => {
    const node = await openCycleNode(workspace, 'held', { driver: quickBusyDriver(), writerWorker })
    await createOrders(node.db)
    await addOrders(node.db, ['before-the-reader'])
    await node.db.captureBackupChanges()
    await addOrders(node.db, ['under-the-reader-1'])
    const reader = holdReadTransaction(node.path, build)
    const held = await node.db.captureBackupChanges()
    await addOrders(node.db, ['under-the-reader-2'])
    const continued = await node.db.captureBackupChanges()
    reader.release()
    await addOrders(node.db, ['after-the-reader'])
    const released = await node.db.captureBackupChanges()
    await addOrders(node.db, ['fresh-log'])
    const fresh = await node.db.captureBackupChanges()

    const records = changeRecords(storedChain(node.destination, onlyChainId(node.destination)))
    const [, heldRecord, continuedRecord, releasedRecord, freshRecord] = records
    expect(held?.position?.firstFrame).toBe(1)
    expect(walBytes(node.path)).toBe(0)
    expect(continuedRecord?.position?.firstFrame).toBe((heldRecord?.position?.lastFrame ?? 0) + 1)
    expect(continuedRecord?.position?.salt1).toBe(heldRecord?.position?.salt1)
    expect(releasedRecord?.position?.firstFrame).toBe((continuedRecord?.position?.lastFrame ?? 0) + 1)
    expect(freshRecord?.position?.firstFrame).toBe(1)
    expect(freshRecord?.position?.salt1).not.toBe(releasedRecord?.position?.salt1)
    expect([continued?.kind, released?.kind, fresh?.kind]).toEqual(['change', 'change', 'change'])
    expect(node.errors).toEqual([])
    await expectRestoresTo(
      workspace,
      node.destination,
      ['before-the-reader', 'under-the-reader-1', 'under-the-reader-2', 'after-the-reader', 'fresh-log'],
      'held',
    )
    const onePieceAtATime = workspace.path('held-one-at-a-time.db')
    const report = await restoreBackup({
      destination: node.destination,
      driver: betterSqlite3(),
      destPath: onePieceAtATime,
      batchSize: 1,
    })
    expect(report.batchCount).toBe(records.length)
    expect(orderLabels(onePieceAtATime)).toEqual(orderLabels(workspace.path('held-restored.db')))
  })

  it('starts a fresh chain with a full copy once the chain passes its full-copy interval', async () => {
    const node = await openCycleNode(workspace, 'rolled', {
      driver: driver(),
      writerWorker,
      backups: { fullCopyIntervalMs: 200 },
    })
    await createOrders(node.db)
    await addOrders(node.db, ['in-the-first-chain'])
    await node.db.captureBackupChanges()
    await pause(250)
    await addOrders(node.db, ['in-the-second-chain'])
    const rolled = await node.db.captureBackupChanges()

    const heads = storedHeads(node.destination)
    expect(heads).toHaveLength(2)
    expect(rolled?.kind).toBe('full')
    expect(heads[1]?.previousChainId).toBe(heads[0]?.chainId)
    expect(storedChain(node.destination, heads[1]?.chainId ?? '')[0]?.kind).toBe('full')
    await expectRestoresTo(workspace, node.destination, ['in-the-first-chain', 'in-the-second-chain'], 'rolled')
  })

  it('takes a turn on its own interval with no call from the caller', async () => {
    const node = await openCycleNode(workspace, 'timed', {
      driver: driver(),
      writerWorker,
      backups: { intervalMs: 40 },
    })
    await createOrders(node.db)
    await addOrders(node.db, ['captured-by-the-timer'])

    await waitFor(() => node.runs.some(run => run.kind === 'change'), 'a change piece taken on the interval')

    expect(storedHeads(node.destination, CHAIN_LIST_NAME)).toHaveLength(1)
    await expectRestoresTo(workspace, node.destination, ['captured-by-the-timer'], 'timed')
  })
})
