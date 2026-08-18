import { mkdtempSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { assembleFromDestination } from '../../backup/assemble.js'
import { readBackupChains } from '../../backup/chain.js'
import type { BackupCycleOptions } from '../../backup/cycle-options.js'
import type { BackupRunReport } from '../../backup/report.js'
import { ConnectionPool } from '../../connection-pool.js'
import { Database } from '../../database.js'
import { type MemoryDestination, memoryDestination } from '../backup/memory-destination.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cycle-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

const THOUSAND_FRAME_LOG_BYTES = 4_120_032

interface Captured {
  db: Database
  destination: MemoryDestination
  reports: BackupRunReport[]
  logPath: string
}

async function openWithCycle(name: string, overrides?: Partial<BackupCycleOptions>): Promise<Captured> {
  const dbPath = join(tempDir, `${name}.db`)
  const destination = memoryDestination()
  const reports: BackupRunReport[] = []
  const db = await Database.create(name, dbPath, testDriver, {
    backups: {
      destination,
      intervalMs: 0,
      stagingDir: join(tempDir, `${name}-staging`),
      onRun: report => reports.push(report),
      ...overrides,
    },
  })
  return { db, destination, reports, logPath: `${dbPath}-wal` }
}

async function seedPages(db: Database, rows: number): Promise<void> {
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.bulkLoad(
    'INSERT INTO users (name) VALUES (?)',
    Array.from({ length: rows }, (_, index) => [`user-${index}`.padEnd(200, 'x')]),
  )
}

describe('a database that owns its checkpoint cycle', () => {
  it('opens its writer with automatic checkpointing turned off', async () => {
    const pool = await ConnectionPool.create({
      driver: testDriver,
      path: join(tempDir, 'pragma.db'),
      walMode: true,
      walAutoCheckpoint: 0,
    })
    const stmt = await pool.acquireWriter().prepare('PRAGMA wal_autocheckpoint')
    const row = await stmt.get<{ wal_autocheckpoint: number | bigint }>()

    expect(Number(row?.wal_autocheckpoint)).toBe(0)
    await pool.close()
  })

  it('refuses to open a database whose change log the cycle could never read', async () => {
    const opening = Database.create('rollback-journal', join(tempDir, 'no-log.db'), testDriver, {
      walMode: false,
      backups: { destination: memoryDestination(), intervalMs: 0 },
    })

    await expect(opening).rejects.toThrow(/write-ahead logging mode/)
  })

  it('holds every frame a bulk load wrote, because the cycle checkpoints rather than the load', async () => {
    const { db, logPath } = await openWithCycle('owned')
    await seedPages(db, 20000)
    const afterLoad = statSync(logPath).size
    await db.execute("INSERT INTO users (name) VALUES ('after')")
    const afterWrite = statSync(logPath).size

    expect(afterLoad).toBeGreaterThan(THOUSAND_FRAME_LOG_BYTES)
    expect(afterWrite).toBeGreaterThanOrEqual(afterLoad)
    await db.close()
  })

  it('leaves a database without the option checkpointing its own bulk loads', async () => {
    const dbPath = join(tempDir, 'unowned.db')
    const db = await Database.create('unowned', dbPath, testDriver)
    await seedPages(db, 20000)

    expect(statSync(`${dbPath}-wal`).size).toBe(0)
    await db.close()
  })

  it('answers which pieces a restore needs and which no restore needs', async () => {
    const { db, destination } = await openWithCycle('answers')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.captureBackupChanges()
    await db.execute("INSERT INTO notes (body) VALUES ('one')")
    await db.captureBackupChanges()

    const chain = await db.backupChain()
    const plan = await db.backupRestorePlan(Date.now())

    expect(chain).toHaveLength(1)
    expect(plan.chainId).toBe(chain[0]?.chainId)
    expect(plan.changes.map(piece => piece.sequence)).toEqual([1, 2])
    expect(plan.restoresTo).toBe(plan.changes[1]?.capturedAt)
    expect(await db.backupPiecesSafeToDelete()).toEqual([])

    await db.close()
    expect(await readBackupChains(destination)).toHaveLength(1)
  })

  it('captures the log once more while it closes', async () => {
    const { db, destination } = await openWithCycle('closing')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.captureBackupChanges()
    await db.execute("INSERT INTO notes (body) VALUES ('written last')")
    await db.close()

    const chains = await readBackupChains(destination)
    expect(chains[0]?.changes).toHaveLength(2)
  })

  it('rebuilds the database from the full copy and the change pieces the chain names', async () => {
    const { db, destination, reports } = await openWithCycle('restorable')
    await db.captureBackupChanges()
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.captureBackupChanges()
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.close()

    const chains = await readBackupChains(destination)
    const changes = chains[0]?.changes ?? []
    const full = reports.find(report => report.kind === 'full')
    if (!full) throw new Error('the cycle reported no full copy')

    const restorePath = join(tempDir, 'restored.db')
    await assembleFromDestination(destination, full, restorePath)

    const beforeChanges = await Database.create('before', restorePath, testDriver)
    const tables = await beforeChanges.query<{ name: string }>("SELECT name FROM sqlite_schema WHERE name = 'users'")
    await beforeChanges.close()
    expect(tables).toEqual([])

    for (const piece of changes) {
      expect(piece.position.firstFrame).toBe(1)
      writeFileSync(`${restorePath}-wal`, Buffer.from(destination.bytesFor(piece.name)))
      const conn = await testDriver.open(restorePath, { walMode: false })
      await conn.exec('PRAGMA wal_checkpoint(TRUNCATE)')
      await conn.close()
      rmSync(`${restorePath}-shm`, { force: true })
    }

    const verify = await Database.create('verify', restorePath, testDriver)
    const rows = await verify.query<{ name: string }>('SELECT name FROM users ORDER BY id')

    expect(rows.map(row => row.name)).toEqual(['Alice', 'Bob'])
    await verify.close()
  })
})
