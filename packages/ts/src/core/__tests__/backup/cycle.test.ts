import { existsSync, rmSync, statSync } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { createBackupCycle } from '../../backup/cycle-factory.js'
import type { BackupCycleRequest } from '../../backup/cycle-options.js'
import type { BackupDestination } from '../../backup/destination.js'
import type { BackupRunReport } from '../../backup/report.js'
import type { SQLiteConnection, SQLiteStatement } from '../../driver/types.js'
import type { SirannonError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { type MemoryDestination, memoryDestination } from './memory-destination.js'
import { tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()
const openConnections: SQLiteConnection[] = []

afterEach(async () => {
  for (const conn of openConnections.splice(0)) {
    await conn.close()
  }
})

interface Harness {
  cycle: ReturnType<typeof createBackupCycle>
  build: (overrides: Partial<BackupCycleRequest>) => ReturnType<typeof createBackupCycle>
  destination: MemoryDestination
  conn: SQLiteConnection
  dbPath: string
  reports: BackupRunReport[]
  checkpointCalls: () => number
  refuseCheckpoint: (refuse: boolean) => void
}

async function harness(): Promise<Harness> {
  const dbPath = join(temp.path, 'source.db')
  const conn = await testDriver.open(dbPath, { walMode: true, walAutoCheckpoint: 0 })
  openConnections.push(conn)
  await conn.exec('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')

  const destination = memoryDestination()
  const reports: BackupRunReport[] = []
  const manager = new BackupManager()

  let checkpoints = 0
  let refusing = false
  const busyRow = { busy: 1, log: 1, checkpointed: 0 }
  const writer: SQLiteConnection = {
    ...conn,
    prepare: async (sql: string): Promise<SQLiteStatement> => {
      if (!sql.includes('wal_checkpoint')) return conn.prepare(sql)
      checkpoints++
      if (!refusing) return conn.prepare(sql)
      return {
        all: async () => [busyRow] as never[],
        get: async () => busyRow as never,
        run: async () => ({ changes: 0, lastInsertRowId: 0 }),
      }
    },
  }

  const request: BackupCycleRequest = {
    destination,
    intervalMs: 0,
    databaseId: 'main',
    sourcePath: dbPath,
    stagingDir: join(temp.path, 'staging'),
    onRun: report => reports.push(report),
    runExclusive: op => op(),
    acquireWriter: () => writer,
    fullCopy: options => manager.copyToDestination(conn, { ...options, databaseId: 'main', sourcePath: dbPath }),
  }

  return {
    cycle: createBackupCycle(request),
    build: overrides => createBackupCycle({ ...request, ...overrides }),
    destination,
    conn,
    dbPath,
    reports,
    checkpointCalls: () => checkpoints,
    refuseCheckpoint: refuse => {
      refusing = refuse
    },
  }
}

async function insert(conn: SQLiteConnection, body: string): Promise<void> {
  await conn.exec(`INSERT INTO notes (body) VALUES ('${body}')`)
}

describe('the checkpoint cycle', () => {
  it('starts a chain with a full copy and records it at the destination', async () => {
    const { cycle, destination, reports } = await harness()
    await cycle.start()

    const chains = await cycle.chains()
    expect(chains).toHaveLength(1)
    expect(chains[0]?.base?.kind).toBe('full')
    expect(chains[0]?.changes).toEqual([])
    expect(reports[0]?.kind).toBe('full')
    expect(destination.names()).toContain(chains[0]?.base?.name)
  })

  it('captures the frames written since the previous turn and then checkpoints', async () => {
    const { cycle, conn, dbPath, checkpointCalls } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await insert(conn, 'second')

    const before = checkpointCalls()
    const report = await cycle.runOnce()

    expect(report?.kind).toBe('change')
    expect(report?.position?.firstFrame).toBe(1)
    expect(report?.position?.lastFrame).toBeGreaterThanOrEqual(1)
    expect(checkpointCalls()).toBeGreaterThan(before)
    expect(statSync(`${dbPath}-wal`).size).toBe(0)
  })

  it('names the full copy each change piece depends on and the frames it covers', async () => {
    const { cycle, conn } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()
    await insert(conn, 'second')
    await cycle.runOnce()

    const chain = (await cycle.chains())[0]
    expect(chain?.changes.map(piece => piece.sequence)).toEqual([1, 2])
    for (const piece of chain?.changes ?? []) {
      expect(piece.chainId).toBe(chain?.base?.chainId)
      expect(piece.position.firstFrame).toBe(1)
      expect(piece.frameCount).toBeGreaterThan(0)
    }
    expect(chain?.changes[0]?.position.salt1).not.toBe(chain?.changes[1]?.position.salt1)
  })

  it('takes nothing where the log holds no frame the chain lacks', async () => {
    const { cycle, conn } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()

    expect(await cycle.runOnce()).toBeUndefined()
    expect((await cycle.chains())[0]?.changes).toHaveLength(1)
  })

  it('reads on from the middle of a log its checkpoint could not empty', async () => {
    const { cycle, conn, refuseCheckpoint } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    refuseCheckpoint(true)
    await cycle.runOnce()
    await insert(conn, 'second')
    const second = await cycle.runOnce()

    expect(second?.position?.firstFrame).toBeGreaterThan(1)
    expect(second?.position?.salt1).toBe((await cycle.chains())[0]?.changes[0]?.position.salt1)
  })

  it('reports a log that restarted before the capture read it, and runs no checkpoint after it', async () => {
    const { cycle, conn, refuseCheckpoint, checkpointCalls } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    refuseCheckpoint(true)
    await cycle.runOnce()

    refuseCheckpoint(false)
    await conn.exec('PRAGMA wal_checkpoint(TRUNCATE)')
    await insert(conn, 'second')

    const before = checkpointCalls()
    const error = await cycle.runOnce().catch((err: unknown) => err as SirannonError)

    expect((error as SirannonError).code).toBe('BACKUP_LOG_REWOUND')
    expect(checkpointCalls()).toBe(before)
    expect(await cycle.chains()).toHaveLength(2)
  })

  it('starts a fresh chain where the destination no longer holds the one it was extending', async () => {
    const { cycle, build, conn } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()

    const emptied = build({ destination: memoryDestination() })
    await emptied.start()

    const chains = await emptied.chains()
    expect(chains).toHaveLength(1)
    expect(chains[0]?.chainId).not.toBe((await cycle.chains())[0]?.chainId)
  })

  it('extends no chain the list lost after it started, and says the writes reached no backup', async () => {
    const errors: Error[] = []
    const { build, conn, destination } = await harness()
    const cycle = build({ onError: err => errors.push(err) })
    await cycle.start()
    const lost = (await cycle.chains())[0]?.chainId
    await destination.writePiece(
      'sirannon-backup-chain',
      0,
      new TextEncoder().encode(JSON.stringify({ chainId: 'another-node', startedAt: 1 })),
    )
    await insert(conn, 'first')

    await cycle.runOnce()

    const chains = await cycle.chains()
    expect(chains.map(chain => chain.chainId)).not.toContain(lost)
    expect(chains[0]?.base?.kind).toBe('full')
    expect(errors[0]?.message).toContain(String(lost))
  })

  it('checks its chain by reading the one record it wrote, however many chains the list holds', async () => {
    const { build, conn, destination } = await harness()
    let reads = 0
    const counting: BackupDestination = {
      ...destination,
      async readPiece(name, index) {
        if (name === 'sirannon-backup-chain') reads++
        return destination.readPiece(name, index)
      },
    }
    const cycle = build({ destination: counting })
    await cycle.start()
    const encoder = new TextEncoder()
    for (const [index, chainId] of [
      [1, 'other-one'],
      [2, 'other-two'],
    ] as const) {
      await destination.writePiece(
        'sirannon-backup-chain',
        index,
        encoder.encode(JSON.stringify({ chainId, startedAt: 1 })),
      )
    }
    await insert(conn, 'first')
    reads = 0

    await cycle.runOnce()

    expect(reads).toBe(1)
  })

  it('captures the log a restart finds, whatever limit the operator put on an uncaptured one', async () => {
    const errors: Error[] = []
    const { cycle, build, conn } = await harness()
    await cycle.start()
    const chainId = (await cycle.chains())[0]?.chainId
    await insert(conn, 'first')

    const restarted = build({ maxUncapturedLogBytes: 1, onError: err => errors.push(err) })
    await restarted.start()

    const chains = await restarted.chains()
    expect(chains).toHaveLength(1)
    expect(chains[0]?.chainId).toBe(chainId)
    expect(chains[0]?.changes.map(piece => piece.sequence)).toEqual([1])
    expect(errors).toEqual([])
  })

  it('hands the operator the same failure it records, and records how far the turn had got', async () => {
    const errors: Error[] = []
    const { build, conn, destination } = await harness()
    const failing = build({ pieceBytes: 512, onError: err => errors.push(err) })
    await failing.start()
    await insert(conn, 'first')

    errors.length = 0
    destination.refusePiece(1)
    await failing.runOnce().catch(() => {})

    const recorded = failing.status().lastError
    expect(recorded?.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(recorded?.progress?.phase).toBe('transfer')
    expect(errors).toHaveLength(1)
    expect(errors[0]?.message).toBe(recorded?.message)
  })

  it('leaves the writes reaching no backup as the failure it records, over the one that stopped the turn', async () => {
    const errors: Error[] = []
    const { build, conn, destination } = await harness()
    const failing = build({ onError: err => errors.push(err) })
    await failing.start()
    await insert(conn, 'first')

    destination.refusePiece(0)
    await failing.runOnce().catch(() => {})
    await insert(conn, 'second')

    errors.length = 0
    const emptying = build({ maxUncapturedLogBytes: 1, onError: err => errors.push(err) })
    await emptying.start().catch(() => {})

    expect(errors.map(err => (err as SirannonError).code)).toContain('BACKUP_CHAIN_BROKEN')
    expect(emptying.status().lastError?.code).toBe('BACKUP_CHAIN_BROKEN')
  })

  it('records a log that restarted against the chain it broke, not the one that replaced it', async () => {
    const { cycle, conn, refuseCheckpoint } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    refuseCheckpoint(true)
    await cycle.runOnce()
    const broken = cycle.status().chainId

    refuseCheckpoint(false)
    await conn.exec('PRAGMA wal_checkpoint(TRUNCATE)')
    await insert(conn, 'second')
    await cycle.runOnce().catch(() => {})

    const failure = cycle.status().lastError
    expect(failure?.code).toBe('BACKUP_LOG_REWOUND')
    expect(failure?.chainId).toBe(broken)
    expect(cycle.status().chainId).not.toBe(broken)
  })

  it('hands the operator each failure once when the chain that replaces a broken one also fails', async () => {
    const errors: SirannonError[] = []
    const { build, conn, destination, refuseCheckpoint } = await harness()
    const cycle = build({ onError: err => errors.push(err as SirannonError) })
    await cycle.start()
    await insert(conn, 'first')
    refuseCheckpoint(true)
    await cycle.runOnce()
    const broken = cycle.status().chainId

    refuseCheckpoint(false)
    await conn.exec('PRAGMA wal_checkpoint(TRUNCATE)')
    await insert(conn, 'second')
    errors.length = 0
    destination.refusePiece(0)
    await cycle.runOnce().catch(() => {})

    expect(errors.map(err => err.code)).toEqual(['BACKUP_LOG_REWOUND', 'BACKUP_DESTINATION_ERROR'])
    expect(cycle.status().lastError?.code).toBe('BACKUP_DESTINATION_ERROR')
    expect(cycle.status().lastError?.chainId).toBe(broken)
  })

  it('states the same log salts on a full copy and on the first change piece extending it', async () => {
    const { cycle, conn, reports } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()
    await insert(conn, 'second')
    await cycle.runOnce()

    const full = reports.find(report => report.kind === 'full')
    const changes = reports.filter(report => report.kind === 'change')
    expect(full?.logPosition).toBeDefined()
    expect(changes).toHaveLength(2)
    expect(full?.logPosition?.salt1).toBe(changes[0]?.position?.salt1)
    expect(full?.logPosition?.salt2).toBe(changes[0]?.position?.salt2)
    expect(full?.logPosition?.logSequence).toBe(changes[0]?.position?.logSequence)
    expect(changes[1]?.position?.logSequence).toBe((changes[0]?.position?.logSequence ?? 0) + 1)
    expect(changes[1]?.position?.salt1).not.toBe(changes[0]?.position?.salt1)
  })

  it('takes the same chain up again after a restart rather than copying the database afresh', async () => {
    const { cycle, build, conn } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()
    await cycle.stop()

    const restarted = build({})
    await restarted.start()
    await insert(conn, 'second')
    await restarted.runOnce()

    const chains = await restarted.chains()
    expect(chains).toHaveLength(1)
    expect(chains[0]?.changes.map(piece => piece.sequence)).toEqual([1, 2])
  })

  it('sends a capture the destination refused on the next turn, under the same place in the chain', async () => {
    const { cycle, conn, destination, dbPath } = await harness()
    await cycle.start()
    await insert(conn, 'first')

    destination.refuseName(`sirannon-backup-${(await cycle.chains())[0]?.chainId}-000001.wal`)
    const refused = await cycle.runOnce().catch((err: unknown) => err as Error)
    expect((refused as Error).message).toContain('refusing')
    expect(statSync(`${dbPath}-wal`).size).toBe(0)

    destination.refuseName(null)
    const sent = await cycle.runOnce()

    expect(sent?.position?.firstFrame).toBe(1)
    expect((await cycle.chains())[0]?.changes.map(piece => piece.sequence)).toEqual([1])
  })

  it('starts a fresh chain once the chain has run for its full-copy interval', async () => {
    const { cycle, build, conn } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()

    const replaced = build({ fullCopyIntervalMs: 0 })
    await replaced.start()
    await replaced.runOnce()

    const chains = await replaced.chains()
    expect(chains.length).toBeGreaterThanOrEqual(2)
    expect(chains[0]?.previousChainId).toBe(chains[1]?.chainId)
  })
  it('appends nothing to a chain it could not check, and takes that chain up once it can', async () => {
    const { cycle, build, conn, destination } = await harness()
    await cycle.start()
    await cycle.stop()
    const chain = (await cycle.chains())[0]
    const captured = chain?.changes.length ?? 0

    const errors: Error[] = []
    let unreachable = true
    const refusing: BackupDestination = {
      ...destination,
      async readPiece(name, index) {
        if (unreachable && name === 'sirannon-backup-chain') throw new Error('refusing to read the list')
        return destination.readPiece(name, index)
      },
    }
    destination.refuseListing('sirannon-backup-chain')
    const restarted = build({ destination: refusing, onError: err => errors.push(err) })
    await restarted.start()
    await insert(conn, 'first')
    const refused = await restarted.runOnce().catch((err: unknown) => err as Error)

    expect(errors[0]?.message).toContain('refusing to list')
    expect((refused as Error).message).toContain('refusing to list')
    destination.refuseListing(null)
    unreachable = false
    expect((await restarted.chains())[0]?.changes).toHaveLength(captured)

    const sent = await restarted.runOnce()
    expect(sent?.chainId).toBe(chain?.chainId)
    expect((await restarted.chains())[0]?.changes).toHaveLength(captured + 1)
  })

  it('reports staged frames that have gone, and starts a fresh chain', async () => {
    const { cycle, conn, destination } = await harness()
    await cycle.start()
    const chainId = (await cycle.chains())[0]?.chainId
    await insert(conn, 'first')

    destination.refuseName(`sirannon-backup-${chainId}-000001.wal`)
    await cycle.runOnce().catch(() => {})
    destination.refuseName(null)
    rmSync(join(temp.path, 'staging', 'capture-1.wal'))

    const error = await cycle.runOnce().catch((err: unknown) => err as SirannonError)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect(await cycle.chains()).toHaveLength(2)
  })

  it('refuses a chain whose record is missing fields Sirannon writes into every one it stores', async () => {
    const { cycle, destination } = await harness()
    await cycle.start()
    const chainId = (await cycle.chains())[0]?.chainId
    const record = JSON.stringify({ kind: 'full', chainId, name: 'sirannon-backup-truncated.db' })
    await destination.writePiece(`sirannon-backup-chain.${chainId}`, 0, new TextEncoder().encode(record))

    const error = await cycle.chains().catch((err: unknown) => err as SirannonError)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('missing fields')
  })

  it('starts a fresh chain where the state it left behind is incomplete', async () => {
    const { cycle, build, conn } = await harness()
    await cycle.start()
    const chainId = (await cycle.chains())[0]?.chainId
    await insert(conn, 'first')
    await cycle.runOnce()
    await cycle.stop()
    await writeFile(join(temp.path, 'staging', 'cycle.json'), JSON.stringify({ chainId, records: 2 }))

    const restarted = build({})
    await restarted.start()

    const chains = await restarted.chains()
    expect(chains).toHaveLength(2)
    expect(chains[0]?.previousChainId).toBeUndefined()
  })
  it('removes the staged frames of a chain the destination no longer holds', async () => {
    const { cycle, build, conn, destination } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    destination.refuseName(`sirannon-backup-${(await cycle.chains())[0]?.chainId}-000001.wal`)
    await cycle.runOnce().catch(() => {})

    const staged = join(temp.path, 'staging', 'capture-1.wal')
    expect(existsSync(staged)).toBe(true)

    const elsewhere = build({ destination: memoryDestination() })
    await elsewhere.start()

    expect(existsSync(staged)).toBe(false)
  })

  it('refuses a chain holding a change record whose frames run backwards', async () => {
    const { cycle, conn, destination } = await harness()
    await cycle.start()
    await insert(conn, 'first')
    await cycle.runOnce()
    const chain = (await cycle.chains())[0]
    const change = chain?.changes[0]
    const backwards = JSON.stringify({ ...change, position: { ...change?.position, firstFrame: 4, lastFrame: 2 } })
    await destination.writePiece(`sirannon-backup-chain.${chain?.chainId}`, 1, new TextEncoder().encode(backwards))

    const error = await cycle.chains().catch((err: unknown) => err as SirannonError)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
  })
})

describe('a caller whose reporting callbacks throw', () => {
  it('finishes the turn and records it, so a fault in the reporting fails no backup', async () => {
    const { build, destination } = await harness()
    const failing = (): never => {
      throw new Error('the operator log went away')
    }
    const cycle = build({ onRun: failing, onProgress: failing, onError: failing })

    await cycle.start()

    const chains = await cycle.chains()
    expect(chains[0]?.base?.kind).toBe('full')
    expect(destination.names()).toContain(chains[0]?.base?.name)
    expect(cycle.status().lastRun?.kind).toBe('full')
    expect(cycle.status().lastError).toBeUndefined()
    await cycle.stop()
  })

  it('takes the next turn after one whose callbacks threw', async () => {
    const { build, conn } = await harness()
    const failing = (): never => {
      throw new Error('the operator log went away')
    }
    const cycle = build({ onRun: failing, onProgress: failing, onError: failing })
    await cycle.start()
    await insert(conn, 'first')

    const report = await cycle.runOnce()

    expect(report?.kind).toBe('change')
    await cycle.stop()
  })
})

describe('the turns a caller can ask for at once', () => {
  it('joins every call made before the queued turn starts, so no more than one turn ever waits', async () => {
    const { cycle } = await harness()
    await cycle.start()

    const first = cycle.runOnce()
    const second = cycle.runOnce()
    const third = cycle.runOnce()

    expect(second).toBe(first)
    expect(third).toBe(first)
    await Promise.all([first, second, third])
    expect(cycle.status().running).toBe(false)
    await cycle.stop()
  })

  it('takes a fresh turn once the queued one has started', async () => {
    const { cycle } = await harness()
    await cycle.start()

    const queued = cycle.runOnce()
    await queued
    const later = cycle.runOnce()

    expect(later).not.toBe(queued)
    await later
    await cycle.stop()
  })
})
