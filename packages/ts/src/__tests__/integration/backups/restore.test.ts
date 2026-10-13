import { existsSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import type { BackupRestoreProgress } from '../../../core/backup/restore-options.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { crashSnapshotDriver } from '../../helpers/fault-drivers/crash-snapshot-driver.js'
import { addOrders, changeRecords, createOrders, onlyChainId, openCycleNode } from './cycle-harness.js'
import {
  CHAIN_LIST_NAME,
  codeOf,
  createWorkspace,
  type DirectoryDestination,
  failureOf,
  filesIn,
  integrityOf,
  orderLabels,
  pause,
  readRaw,
  type StoredRecord,
  storedChain,
  type Workspace,
} from './harness.js'

const ROUNDS = 5
const LEDGER_ROW = 'opening balance'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('restore')
})

afterEach(async () => {
  await workspace.dispose()
})

interface BuiltChain {
  destination: DirectoryDestination
  chainId: string
  records: StoredRecord[]
  labels: string[]
}

async function buildChain(rounds: number, pieceBytes?: number): Promise<BuiltChain> {
  const node = await openCycleNode(workspace, 'source', { backups: pieceBytes ? { pieceBytes } : {} })
  await node.db.captureBackupChanges()
  await createOrders(node.db)
  await node.db.captureBackupChanges()
  const labels: string[] = []
  for (let round = 1; round <= rounds; round++) {
    const label = `note-${round}`
    labels.push(label)
    await addOrders(node.db, [label])
    await node.db.captureBackupChanges()
    await pause(3)
  }
  await node.db.close()
  const chainId = onlyChainId(node.destination)
  return { destination: node.destination, chainId, records: storedChain(node.destination, chainId), labels }
}

function liveDatabase(path: string): BetterSqlite3.Database {
  const live = new BetterSqlite3(path)
  live.pragma('journal_mode = WAL')
  live.pragma('wal_autocheckpoint = 0')
  live.exec('CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT)')
  live.prepare('INSERT INTO ledger (entry) VALUES (?)').run(LEDGER_ROW)
  return live
}

function nothingLeftBeside(path: string): string[] {
  return filesIn(workspace.dir).filter(name => name.startsWith(path.split('/').at(-1) ?? path))
}

describe.each<[string, () => SQLiteDriver]>([
  ['better-sqlite3', () => betterSqlite3()],
  ['node:sqlite', () => nodeSqlite()],
])('restoring a chain through %s', (_label, driver) => {
  it('rebuilds the newest backup in batches, reports each piece it fetches, and leaves no log beside the file', async () => {
    const chain = await buildChain(ROUNDS, 512)
    const changes = changeRecords(chain.records)
    const base = chain.records[0]
    const seen: BackupRestoreProgress[] = []
    const destPath = workspace.path('restored.db')

    const report = await restoreBackup({
      destination: chain.destination,
      driver: driver(),
      destPath,
      batchSize: 2,
      onProgress: progress => seen.push({ ...progress }),
    })

    expect(nothingLeftBeside(destPath)).toEqual(['restored.db'])
    expect(integrityOf(destPath)).toBe('ok')
    expect(orderLabels(destPath)).toEqual(chain.labels)
    expect(report).toMatchObject({
      chainId: chain.chainId,
      destPath,
      baseName: base?.name,
      restoresTo: changes.at(-1)?.capturedAt,
      changesApplied: changes.length,
      batchCount: Math.ceil(changes.length / 2),
      framesApplied: changes.reduce((sum, record) => sum + (record.frameCount ?? 0), 0),
      pieceCount: chain.records.reduce((sum, record) => sum + record.pieceCount, 0),
      bytesFetched: chain.records.reduce((sum, record) => sum + record.bytesWritten, 0),
    })
    expect(report.durationMs).toBe(report.finishedAt - report.startedAt)
    expect(seen[0]?.phase).toBe('full-copy')
    expect(seen.at(-1)).toMatchObject({
      phase: 'changes',
      changesApplied: changes.length,
      changesTotal: changes.length,
    })
    expect(seen.map(progress => progress.piecesFetched)).toEqual(
      [...seen.map(p => p.piecesFetched)].sort((a, b) => a - b),
    )
    expect(seen.at(-1)?.bytesFetched).toBe(report.bytesFetched)
  })

  it('stops at the moment named, at every batch size from one to the largest, and refuses a size outside them', async () => {
    const chain = await buildChain(ROUNDS)
    const changes = changeRecords(chain.records)
    const throughSecondNote = 3
    const second = changes[throughSecondNote - 1]?.capturedAt ?? 0
    const baseFinished = chain.records[0]?.finishedAt ?? 0

    for (const batchSize of [1, 3, 4096]) {
      const destPath = workspace.path(`moment-${batchSize}.db`)
      const report = await restoreBackup({
        destination: chain.destination,
        driver: driver(),
        destPath,
        moment: second,
        batchSize,
      })
      expect(report.restoresTo, `${batchSize}`).toBe(second)
      expect(report.changesApplied, `${batchSize}`).toBe(throughSecondNote)
      expect(report.batchCount, `${batchSize}`).toBe(Math.ceil(throughSecondNote / batchSize))
      expect(orderLabels(destPath), `${batchSize}`).toEqual(['note-1', 'note-2'])
    }
    const baseOnly = workspace.path('base-only.db')
    const report = await restoreBackup({
      destination: chain.destination,
      driver: driver(),
      destPath: baseOnly,
      moment: baseFinished,
    })
    expect(report).toMatchObject({ restoresTo: baseFinished, changesApplied: 0, batchCount: 0 })
    expect(readRaw(baseOnly, "SELECT name FROM sqlite_master WHERE name = 'orders'")).toEqual([])

    for (const batchSize of [0, -1, 4097, 1.5, Number.NaN]) {
      const destPath = workspace.path(`refused-${batchSize}.db`)
      const error = await failureOf(
        restoreBackup({ destination: chain.destination, driver: driver(), destPath, batchSize }),
      )
      expect(codeOf(error), `${batchSize}`).toBe('BACKUP_ERROR')
      expect(existsSync(destPath), `${batchSize}`).toBe(false)
    }
  })
})

describe('restoring a chain the destination damaged or lost part of', () => {
  it('refuses a moment no full copy reaches and names the earliest one it can restore', async () => {
    const chain = await buildChain(1)
    const earliest = new Date(chain.records[0]?.finishedAt ?? 0).toISOString()

    const error = await failureOf(
      restoreBackup({
        destination: chain.destination,
        driver: betterSqlite3(),
        destPath: workspace.path('old.db'),
        moment: 1,
      }),
    )

    expect(codeOf(error)).toBe('BACKUP_CHAIN_BROKEN')
    expect(error.message).toContain(earliest)
    expect(existsSync(workspace.path('old.db'))).toBe(false)
  })

  it('names the change piece a chain is missing, and still restores to a moment before it', async () => {
    const chain = await buildChain(3)
    const logName = `${CHAIN_LIST_NAME}.${chain.chainId}`
    chain.destination.remove(logName, 3)

    const broken = await failureOf(
      restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath: workspace.path('gap.db') }),
    )
    const before = await restoreBackup({
      destination: chain.destination,
      driver: betterSqlite3(),
      destPath: workspace.path('before-gap.db'),
      moment: changeRecords(chain.records)[1]?.capturedAt,
    })

    expect(codeOf(broken)).toBe('BACKUP_CHAIN_BROKEN')
    expect(broken.message).toContain('missing change piece 3')
    expect(existsSync(workspace.path('gap.db'))).toBe(false)
    expect(before.changesApplied).toBe(2)
    expect(orderLabels(workspace.path('before-gap.db'))).toEqual(['note-1'])
  })

  it('refuses stored bytes that were altered, lost, or cut short, and leaves nothing at the path', async () => {
    const chain = await buildChain(2, 512)
    const second = changeRecords(chain.records)[1]
    const base = chain.records[0]
    if (!second || !base) throw new Error('the chain holds too few records')
    const damage: [string, string, number, (bytes: Buffer) => Buffer | null, string][] = [
      ['a flipped byte in a change piece', second.name, 1, bytes => flip(bytes, 10), 'do not match the fingerprint'],
      ['a lost piece of a change piece', second.name, 1, () => null, `missing piece 1 of '${second.name}'`],
      [
        'a short last piece of a change piece',
        second.name,
        second.pieceCount - 1,
        bytes => bytes.subarray(0, -1),
        'bytes where the run wrote',
      ],
      ['a flipped byte in the full copy', base.name, 0, bytes => flip(bytes, 200), 'do not match the fingerprint'],
    ]

    for (const [label, name, index, alter, message] of damage) {
      const original = chain.destination.stored(name, index)
      const altered = alter(Buffer.from(original))
      if (altered) chain.destination.replace(name, index, altered)
      else chain.destination.remove(name, index)
      const destPath = workspace.path('damaged.db')
      const error = await failureOf(
        restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath }),
      )
      chain.destination.replace(name, index, original)
      expect(codeOf(error), label).toBe('BACKUP_DESTINATION_ERROR')
      expect(error.message, label).toContain(message)
      expect(nothingLeftBeside(destPath), label).toEqual([])
    }
  })

  it('refuses a change piece whose record contradicts its own bytes', async () => {
    const chain = await buildChain(2)
    const logName = `${CHAIN_LIST_NAME}.${chain.chainId}`
    const [first, second] = changeRecords(chain.records)
    if (!first || !second) throw new Error('the chain holds too few change pieces')
    const lies: [string, number, Record<string, unknown>, string][] = [
      [
        'a frame count from another page size',
        2,
        { ...second, frameCount: (second.frameCount ?? 0) + 1 },
        'Change piece 2',
      ],
      [
        'salts from another log',
        1,
        { ...first, position: { ...first.position, salt1: (first.position?.salt1 ?? 0) ^ 1 } },
        'salted',
      ],
      ['more pieces than were stored', 2, { ...second, pieceCount: second.pieceCount + 3 }, 'missing piece'],
      [
        'a first piece that starts past frame one',
        1,
        { ...first, position: { ...first.position, firstFrame: 3, lastFrame: (first.position?.lastFrame ?? 0) + 2 } },
        'the full copy underneath it',
      ],
    ]

    for (const [label, index, record, message] of lies) {
      const original = chain.destination.stored(logName, index)
      chain.destination.replace(logName, index, Buffer.from(JSON.stringify(record)))
      const destPath = workspace.path('lying.db')
      const error = await failureOf(
        restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath }),
      )
      chain.destination.replace(logName, index, original)
      expect(error.message, label).toContain(message)
      expect(codeOf(error), label).toMatch(/^BACKUP_(CHAIN_BROKEN|DESTINATION_ERROR)$/)
      expect(nothingLeftBeside(destPath), label).toEqual([])
    }

    const firstPiece = chain.destination.stored(first.name, 0)
    chain.destination.replace(first.name, 0, Buffer.concat([Buffer.alloc(32), firstPiece.subarray(32)]))
    const damagedHeader = await failureOf(
      restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath: workspace.path('header.db') }),
    )
    expect(codeOf(damagedHeader)).toBe('BACKUP_CHAIN_BROKEN')
    expect(damagedHeader.message).toContain('no log header')
  })

  it('stops a destination that hangs during a restore once destinationTimeoutMs passes', async () => {
    const chain = await buildChain(1)
    const change = changeRecords(chain.records)[0]
    const destPath = workspace.path('hung.db')
    const hangs: [string, () => void, string][] = [
      [
        'a read',
        () => {
          chain.destination.faults.holdRead = name => (name === change?.name ? new Promise<void>(() => {}) : undefined)
        },
        `did not return piece 0 of '${change?.name}' within 150ms`,
      ],
      [
        'a listing',
        () => {
          chain.destination.faults.holdList = name => (name === change?.name ? new Promise<void>(() => {}) : undefined)
        },
        `did not list the pieces of '${change?.name}' within 150ms`,
      ],
    ]

    for (const [label, hang, message] of hangs) {
      chain.destination.faults.holdRead = undefined
      chain.destination.faults.holdList = undefined
      hang()
      const startedAt = Date.now()
      const error = await failureOf(
        restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath, destinationTimeoutMs: 150 }),
      )

      expect(codeOf(error), label).toBe('BACKUP_DESTINATION_ERROR')
      expect(error.message, label).toContain(message)
      expect(Date.now() - startedAt, label).toBeLessThan(5_000)
      expect(nothingLeftBeside(destPath), label).toEqual([])
    }
  })
})

describe('restoring over a path that already holds a database', () => {
  it('refuses the path unless asked to replace it, then replaces it', async () => {
    const chain = await buildChain(2)
    const destPath = workspace.path('occupied.db')
    writeFileSync(destPath, new Uint8Array(4096))

    const refused = await failureOf(
      restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath }),
    )
    expect(codeOf(refused)).toBe('BACKUP_ERROR')
    expect(refused.message).toContain('replaceExisting')
    expect(statSync(destPath).size).toBe(4096)

    await restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath, replaceExisting: true })
    expect(orderLabels(destPath)).toEqual(chain.labels)
  })

  it('folds the log of a live database into it before removing that log, so a crash before the rename loses no commit', async () => {
    const chain = await buildChain(2)
    const destPath = workspace.path('live.db')
    const snapshotPath = workspace.path('state-at-the-crash.db')
    const live = liveDatabase(destPath)
    try {
      expect(readFileSync(`${destPath}-wal`).byteLength).toBeGreaterThan(0)

      await restoreBackup({
        destination: chain.destination,
        driver: crashSnapshotDriver({ watchedPath: destPath, snapshotPath }),
        destPath,
        replaceExisting: true,
      })

      expect(existsSync(`${destPath}-wal`)).toBe(false)
      expect(readRaw(snapshotPath, 'SELECT entry FROM ledger')).toEqual([{ entry: LEDGER_ROW }])
      expect(orderLabels(destPath)).toEqual(chain.labels)
    } finally {
      live.close()
    }
  })

  it('removes a database whose log a reader keeps it from folding, and publishes the rebuild in its place', async () => {
    const chain = await buildChain(2)
    const destPath = workspace.path('held.db')
    const live = liveDatabase(destPath)
    const reader = new BetterSqlite3(destPath, { readonly: true })
    reader.exec('BEGIN')
    reader.prepare('SELECT count(*) FROM ledger').get()
    try {
      await restoreBackup({
        destination: chain.destination,
        driver: betterSqlite3({ busyTimeout: 20 }),
        destPath,
        replaceExisting: true,
      })

      expect(existsSync(`${destPath}-wal`)).toBe(false)
      expect(integrityOf(destPath)).toBe('ok')
      expect(orderLabels(destPath)).toEqual(chain.labels)
      expect(readRaw(destPath, "SELECT name FROM sqlite_master WHERE name = 'ledger'")).toEqual([])
    } finally {
      reader.exec('COMMIT')
      reader.close()
      live.close()
    }
  })

  it('leaves a live database and its log as they were when the restore fails', async () => {
    const chain = await buildChain(2)
    const destPath = workspace.path('kept.db')
    const live = liveDatabase(destPath)
    try {
      const walBefore = readFileSync(`${destPath}-wal`)
      chain.destination.faults.refuseList = name => name === changeRecords(chain.records)[1]?.name

      const error = await failureOf(
        restoreBackup({ destination: chain.destination, driver: betterSqlite3(), destPath, replaceExisting: true }),
      )

      expect(codeOf(error)).toBe('BACKUP_DESTINATION_ERROR')
      expect(readFileSync(`${destPath}-wal`)).toEqual(walBefore)
      expect(live.prepare('SELECT entry FROM ledger').all()).toEqual([{ entry: LEDGER_ROW }])
      expect(filesIn(workspace.dir).filter(name => name.includes('.restoring'))).toEqual([])
    } finally {
      live.close()
    }
  })
})

function flip(bytes: Buffer, at: number): Buffer {
  const altered = Buffer.from(bytes)
  altered[at] = (altered[at] ?? 0) ^ 0xff
  return altered
}
