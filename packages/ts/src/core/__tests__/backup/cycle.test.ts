import { existsSync, rmSync, statSync } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { createBackupCycle } from '../../backup/cycle.js'
import type { BackupCycleRequest } from '../../backup/cycle-options.js'
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
    destination.refuseListing('sirannon-backup-chain')
    const restarted = build({ onError: err => errors.push(err) })
    await restarted.start()
    await insert(conn, 'first')
    const refused = await restarted.runOnce().catch((err: unknown) => err as Error)

    expect(errors[0]?.message).toContain('refusing to list')
    expect((refused as Error).message).toContain('refusing to list')
    destination.refuseListing(null)
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
