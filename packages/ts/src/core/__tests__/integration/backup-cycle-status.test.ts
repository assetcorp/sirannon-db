import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { BackupCycleOptions } from '../../backup/cycle-options.js'
import type { BackupCycleStatus } from '../../backup/cycle-status.js'
import type { BackupRunReport } from '../../backup/report.js'
import { Database } from '../../database.js'
import { type MemoryDestination, memoryDestination } from '../backup/memory-destination.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string
const openDatabases: Database[] = []

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cycle-status-'))
})

afterEach(async () => {
  for (const db of openDatabases.splice(0)) {
    await db.close().catch(() => {})
  }
  rmSync(tempDir, { recursive: true, force: true })
})

interface Opened {
  db: Database
  destination: MemoryDestination
}

const WAIT_LIMIT_MS = 5_000
const PIECE_LATENCY_MS = 2

function slowDestination(destination: MemoryDestination): MemoryDestination {
  const store = destination.writePiece.bind(destination)
  destination.writePiece = async (name, index, bytes) => {
    await new Promise(resolve => setTimeout(resolve, PIECE_LATENCY_MS))
    await store(name, index, bytes)
  }
  return destination
}

async function waitFor(reached: () => boolean): Promise<void> {
  const giveUpAt = Date.now() + WAIT_LIMIT_MS
  while (Date.now() < giveUpAt) {
    if (reached()) return
    await new Promise(resolve => setTimeout(resolve, 1))
  }
  throw new Error(`Nothing reached that state inside ${WAIT_LIMIT_MS}ms`)
}

async function openWithCycle(name: string, overrides?: Partial<BackupCycleOptions>): Promise<Opened> {
  const destination = memoryDestination()
  const db = await Database.create(name, join(tempDir, `${name}.db`), testDriver, {
    backups: {
      destination,
      intervalMs: 0,
      stagingDir: join(tempDir, `${name}-staging`),
      onError: () => {},
      ...overrides,
    },
  })
  openDatabases.push(db)
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
  return { db, destination }
}

describe('what a database reports about the backup cycle it runs', () => {
  it('reports the turn it has finished, and reports nothing under way once that turn is done', async () => {
    const { db } = await openWithCycle('finished')

    await db.captureBackupChanges()
    const status = db.backupStatus()

    expect(status.running).toBe(false)
    expect(status.lastRun?.kind).toBe('change')
    expect(status.chainId).toBe(status.lastRun?.chainId)
    expect(status.progress).toBeUndefined()
    expect(status.lastError).toBeUndefined()
  })

  it('reports nothing at all before the first turn has finished anything', async () => {
    const destination = memoryDestination()
    const db = await Database.create('untouched', join(tempDir, 'untouched.db'), testDriver, {
      backups: { destination, intervalMs: 0, stagingDir: join(tempDir, 'untouched-staging'), onError: () => {} },
    })
    openDatabases.push(db)

    const status = db.backupStatus()

    expect(status.lastRun).toBeUndefined()
    expect(status.lastError).toBeUndefined()
    expect(status.lastSkip).toBeUndefined()
  })

  it('reports the progress of the turn under way while that turn is still copying', async () => {
    const seen: BackupCycleStatus[] = []
    const runs: BackupRunReport[] = []
    const { db } = await openWithCycle('in-flight', {
      onRun: report => runs.push(report),
      onProgress: () => seen.push(db.backupStatus()),
    })

    await db.captureBackupChanges()

    const during = seen.find(status => status.progress !== undefined)
    expect(during?.running).toBe(true)
    expect(during?.progress?.runId).toBe(runs[0]?.runId)
    expect(during?.progress?.phase).toBe('copy')
  })

  it('reports the failure a turn raised, under the code that turn failed with', async () => {
    const destination = memoryDestination()
    destination.refusePiece(0)
    const { db } = await openWithCycle('refused', { destination })

    await db.captureBackupChanges().catch(() => {})
    const status = db.backupStatus()

    expect(status.lastError?.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(status.lastError?.message).toContain('refusing piece 0')
    expect(status.running).toBe(false)
  })

  it('reports the failure of a turn its caller took as a rejection, with no error callback involved', async () => {
    const { db, destination } = await openWithCycle('rejected')
    await db.captureBackupChanges()
    destination.refuseName('sirannon-backup')

    await db.execute('INSERT INTO orders (total) VALUES (1)')
    destination.refusePiece(0)
    await expect(db.captureBackupChanges()).rejects.toThrow()

    expect(db.backupStatus().lastError?.code).toBe('BACKUP_DESTINATION_ERROR')
  })

  it('reports how far the run that failed had got, and which chain it was extending', async () => {
    const destination = slowDestination(memoryDestination())
    const { db } = await openWithCycle('failed-detail', { pieceBytes: 512, destination })
    await db.captureBackupChanges()
    const chainId = db.backupStatus().chainId

    await db.execute('INSERT INTO orders (total) VALUES (1)')
    destination.refusePiece(1)
    await db.captureBackupChanges().catch(() => {})
    const failure = db.backupStatus().lastError

    expect(failure?.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(failure?.chainId).toBe(chainId)
    expect(failure?.progress?.runId).toMatch(/^[0-9a-f]{16}$/)
    expect(failure?.progress?.phase).toBe('transfer')
    expect(failure?.durationMs).toBeGreaterThan(0)
  })

  it('keeps that detail when the cycle runs the failing turn on its own timer', async () => {
    const destination = slowDestination(memoryDestination())
    const { db } = await openWithCycle('failed-on-timer', { pieceBytes: 512, intervalMs: 20, destination })
    await db.captureBackupChanges()
    const chainId = db.backupStatus().chainId

    await db.execute('INSERT INTO orders (total) VALUES (1)')
    destination.refusePiece(1)
    await waitFor(() => db.backupStatus().lastError !== undefined)
    const failure = db.backupStatus().lastError

    expect(failure?.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(failure?.chainId).toBe(chainId)
    expect(failure?.progress?.phase).toBe('transfer')
    expect(failure?.durationMs).toBeGreaterThan(0)
  })

  it('reports the turn a node its group backs up from somewhere else passed over', async () => {
    const { db } = await openWithCycle('stood-down', {
      replicationGroup: {
        nodeId: 'node-b',
        readMembership: async () => ({ primaryNodeId: 'node-a', nodeIds: ['node-a'] }),
      },
      preferredNode: 'primary',
    })

    await db.captureBackupChanges()
    const status = db.backupStatus()

    expect(status.lastSkip?.reason).toBe('not-preferred')
    expect(status.lastSkip?.preferredNodeId).toBe('node-a')
    expect(status.lastRun).toBeUndefined()
  })
})
