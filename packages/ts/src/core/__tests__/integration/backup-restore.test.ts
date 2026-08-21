import { existsSync, statSync } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { type BackupChain, type BackupChainChange, chainLogName, DEFAULT_CHAIN_NAME } from '../../backup/chain.js'
import { createBackupCycle } from '../../backup/cycle-factory.js'
import type { BackupCycleRequest } from '../../backup/cycle-options.js'
import { restoreBackup } from '../../backup/restore.js'
import type { BackupRestoreProgress } from '../../backup/restore-options.js'
import { LOG_HEADER_BYTES } from '../../backup/wal-format.js'
import type { SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../../driver/types.js'
import type { SirannonError } from '../../errors.js'
import { type MemoryDestination, memoryDestination } from '../backup/memory-destination.js'
import { tempDirPerTest } from '../backup/shared.js'
import { testDriver } from '../helpers/test-driver.js'

const temp = tempDirPerTest()
const openConnections: SQLiteConnection[] = []

afterEach(async () => {
  for (const conn of openConnections.splice(0)) {
    await conn.close().catch(() => {})
  }
})

interface Harness {
  cycle: ReturnType<typeof createBackupCycle>
  destination: MemoryDestination
  conn: SQLiteConnection
  refuseCheckpoint: (refuse: boolean) => void
}

async function harness(): Promise<Harness> {
  const dbPath = join(temp.path, 'source.db')
  const conn = await testDriver.open(dbPath, { walMode: true, walAutoCheckpoint: 0 })
  openConnections.push(conn)
  await conn.exec('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')

  const destination = memoryDestination()
  const manager = new BackupManager()
  let refusing = false
  const busyRow = { busy: 1, log: 1, checkpointed: 0 }
  const writer: SQLiteConnection = {
    ...conn,
    prepare: async (sql: string): Promise<SQLiteStatement> => {
      if (!sql.includes('wal_checkpoint') || !refusing) return conn.prepare(sql)
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
    runExclusive: op => op(),
    acquireWriter: () => writer,
    fullCopy: options => manager.copyToDestination(conn, { ...options, databaseId: 'main', sourcePath: dbPath }),
  }

  return {
    cycle: createBackupCycle(request),
    destination,
    conn,
    refuseCheckpoint: refuse => {
      refusing = refuse
    },
  }
}

async function insertAndCapture(context: Harness, rounds: number, from = 1): Promise<void> {
  for (let round = from; round < from + rounds; round++) {
    await context.conn.exec(`INSERT INTO notes (body) VALUES ('note-${round}')`)
    await context.cycle.runOnce()
    await new Promise(resolve => setTimeout(resolve, 2))
  }
}

async function readNotes(path: string): Promise<string[]> {
  const conn = await testDriver.open(path, { walMode: false })
  try {
    const rows = await (await conn.prepare('SELECT body FROM notes ORDER BY id')).all<{ body: string }>()
    return rows.map(row => row.body)
  } finally {
    await conn.close()
  }
}

function onlyChain(chains: BackupChain[]): BackupChain {
  const chain = chains[0]
  if (!chain) throw new Error('the destination holds no chain')
  return chain
}

function changeAt(chain: BackupChain, sequence: number): BackupChainChange {
  const change = chain.changes.find(candidate => candidate.sequence === sequence)
  if (!change) throw new Error(`the chain holds no change piece ${sequence}`)
  return change
}

async function rewriteChangeRecord(
  destination: MemoryDestination,
  chain: BackupChain,
  sequence: number,
  overrides: Partial<BackupChainChange>,
): Promise<void> {
  const record = { ...changeAt(chain, sequence), ...overrides }
  const logName = chainLogName(DEFAULT_CHAIN_NAME, chain.chainId)
  await destination.writePiece(logName, sequence, new TextEncoder().encode(JSON.stringify(record)))
}

describe('restoreBackup', () => {
  it('rebuilds a database a caller can open and read', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 3)

    const destPath = join(temp.path, 'restored.db')
    const report = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
    })

    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2', 'note-3'])
    expect(report.changesApplied).toBe(3)
    expect(report.batchCount).toBe(1)
    expect(report.framesApplied).toBeGreaterThan(0)
    expect(report.bytesFetched).toBeGreaterThan(0)
    expect(existsSync(`${destPath}-wal`)).toBe(false)
    expect(existsSync(`${destPath}-shm`)).toBe(false)
  })

  it('reaches the moment the caller names rather than the newest one', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 4)

    const chain = onlyChain(await context.cycle.chains())
    const second = changeAt(chain, 2)
    const destPath = join(temp.path, 'earlier.db')
    const report = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
      moment: second.capturedAt,
    })

    expect(report.restoresTo).toBe(second.capturedAt)
    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2'])
  })

  it('replays a batch at a time and leaves the log empty after each one', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 5)

    const destPath = join(temp.path, 'batched.db')
    const report = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
      batchSize: 2,
    })

    expect(report.batchCount).toBe(3)
    expect(report.changesApplied).toBe(5)
    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2', 'note-3', 'note-4', 'note-5'])
    expect(existsSync(`${destPath}-wal`)).toBe(false)
  })

  it('replays pieces that continue one run of the log', async () => {
    const context = await harness()
    await context.cycle.start()
    context.refuseCheckpoint(true)
    await insertAndCapture(context, 4)

    const chain = onlyChain(await context.cycle.chains())
    expect(changeAt(chain, 2).position.firstFrame).toBeGreaterThan(1)
    expect(changeAt(chain, 2).position.salt1).toBe(changeAt(chain, 1).position.salt1)

    const destPath = join(temp.path, 'one-run.db')
    const report = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
      batchSize: 1,
    })

    expect(report.batchCount).toBe(4)
    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2', 'note-3', 'note-4'])
  })

  it('reports each piece as it arrives', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const seen: BackupRestoreProgress[] = []
    await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'watched.db'),
      onProgress: progress => seen.push({ ...progress }),
    })

    expect(seen.some(progress => progress.phase === 'full-copy')).toBe(true)
    expect(seen.some(progress => progress.phase === 'changes')).toBe(true)
    expect(seen[seen.length - 1]?.changesApplied).toBe(2)
    expect(seen[seen.length - 1]?.changesTotal).toBe(2)
  })

  it('names the change piece a broken chain is missing', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 3)

    const chain = onlyChain(await context.cycle.chains())
    const logName = chainLogName(DEFAULT_CHAIN_NAME, chain.chainId)
    await context.destination.writePiece(logName, 2, new TextEncoder().encode(JSON.stringify({ kind: 'gap' })))

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'broken.db'),
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect((error as SirannonError).message).toContain('missing change piece 2')
  })

  it('refuses a change piece that does not match its fingerprint', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const chain = onlyChain(await context.cycle.chains())
    const second = changeAt(chain, 2)
    const stored = await context.destination.readPiece(second.name, 0)
    const tampered = stored.slice()
    const last = tampered.length - 1
    tampered[last] = (tampered[last] ?? 0) ^ 0xff
    await context.destination.writePiece(second.name, 0, tampered)

    const destPath = join(temp.path, 'tampered.db')
    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('fingerprint')
    expect(existsSync(destPath)).toBe(false)
    expect(existsSync(`${destPath}.restoring`)).toBe(false)
    expect(existsSync(`${destPath}-wal`)).toBe(false)
  })

  it('names the piece of a change file the destination has lost', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const chain = onlyChain(await context.cycle.chains())
    await rewriteChangeRecord(context.destination, chain, 2, { pieceCount: 4 })

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'lost-piece.db'),
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('missing piece 1')
  })

  it('refuses a change piece taken from a database with a different page size', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const chain = onlyChain(await context.cycle.chains())
    await rewriteChangeRecord(context.destination, chain, 2, { frameCount: changeAt(chain, 2).frameCount + 1 })

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'wrong-pages.db'),
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect((error as SirannonError).message).toContain('Change piece 2')
  })

  it('refuses a path already holding a database', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 1)

    const destPath = join(temp.path, 'occupied.db')
    await writeFile(destPath, new Uint8Array(4096))

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_ERROR')
    expect((error as SirannonError).message).toContain('replaceExisting')
    expect(statSync(destPath).size).toBe(4096)
  })

  it('replaces a database at that path once the caller asks for it', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const destPath = join(temp.path, 'replaced.db')
    await writeFile(destPath, new Uint8Array(4096))

    await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
      replaceExisting: true,
    })

    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2'])
  })

  it('folds the log of the database it replaces back in before it removes that log', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const destPath = join(temp.path, 'folded.db')
    const live = await testDriver.open(destPath, { walMode: true, walAutoCheckpoint: 0 })
    openConnections.push(live)
    await live.exec('CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT)')
    await live.exec("INSERT INTO ledger (entry) VALUES ('opening balance')")
    expect(existsSync(`${destPath}-wal`)).toBe(true)

    const checkpointedPaths: string[] = []
    const watchingDriver: SQLiteDriver = {
      ...testDriver,
      open: async (path, openOptions) => {
        const conn = await testDriver.open(path, openOptions)
        return {
          ...conn,
          prepare: async (sql: string): Promise<SQLiteStatement> => {
            if (sql.includes('wal_checkpoint')) checkpointedPaths.push(path)
            return conn.prepare(sql)
          },
        }
      },
    }

    await restoreBackup({
      destination: context.destination,
      driver: watchingDriver,
      destPath,
      replaceExisting: true,
    })

    expect(checkpointedPaths).toContain(destPath)
    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2'])
    expect(existsSync(`${destPath}-wal`)).toBe(false)
  })

  it('replaces a database whose log it cannot fold back in', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const destPath = join(temp.path, 'busy-log.db')
    const live = await testDriver.open(destPath, { walMode: true, walAutoCheckpoint: 0 })
    openConnections.push(live)
    await live.exec('CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT)')
    await live.exec("INSERT INTO ledger (entry) VALUES ('opening balance')")
    expect(existsSync(`${destPath}-wal`)).toBe(true)

    const busyRow = { busy: 1, log: 1, checkpointed: 0 }
    const refusingDriver: SQLiteDriver = {
      ...testDriver,
      open: async (path, openOptions) => {
        const conn = await testDriver.open(path, openOptions)
        if (path !== destPath) return conn
        return {
          ...conn,
          prepare: async (sql: string): Promise<SQLiteStatement> => {
            if (!sql.includes('wal_checkpoint')) return conn.prepare(sql)
            return {
              all: async () => [busyRow] as never[],
              get: async () => busyRow as never,
              run: async () => ({ changes: 0, lastInsertRowId: 0 }),
            }
          },
        }
      },
    }

    await restoreBackup({
      destination: context.destination,
      driver: refusingDriver,
      destPath,
      replaceExisting: true,
    })

    expect(await readNotes(destPath)).toEqual(['note-1', 'note-2'])
    expect(existsSync(`${destPath}-wal`)).toBe(false)
  })

  it('leaves the log of a database at that path alone until the rebuild is whole', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const destPath = join(temp.path, 'live.db')
    const live = await testDriver.open(destPath, { walMode: true, walAutoCheckpoint: 0 })
    openConnections.push(live)
    await live.exec('CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT)')
    await live.exec("INSERT INTO ledger (entry) VALUES ('opening balance')")
    expect(existsSync(`${destPath}-wal`)).toBe(true)

    context.destination.refuseListing(changeAt(onlyChain(await context.cycle.chains()), 1).name)
    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath,
      replaceExisting: true,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect(existsSync(`${destPath}-wal`)).toBe(true)
    const rows = await (await live.prepare('SELECT entry FROM ledger')).all<{ entry: string }>()
    expect(rows).toEqual([{ entry: 'opening balance' }])
  })

  it('refuses a change piece whose log header contradicts its record', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 2)

    const chain = onlyChain(await context.cycle.chains())
    const first = changeAt(chain, 1)
    await rewriteChangeRecord(context.destination, chain, 1, {
      position: { ...first.position, salt1: first.position.salt1 === 7 ? 8 : 7 },
    })

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'wrong-log.db'),
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect((error as SirannonError).message).toContain('salted')
  })

  it('refuses a change piece whose log header was damaged in storage', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 1)

    const chain = onlyChain(await context.cycle.chains())
    const first = changeAt(chain, 1)
    const stored = (await context.destination.readPiece(first.name, 0)).slice()
    stored.set(new Uint8Array(LOG_HEADER_BYTES), 0)
    await context.destination.writePiece(first.name, 0, stored)

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'damaged-log.db'),
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect((error as SirannonError).message).toContain('no log header')
  })

  it('refuses a moment no full copy reaches', async () => {
    const context = await harness()
    await context.cycle.start()
    await insertAndCapture(context, 1)

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'too-old.db'),
      moment: 1,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
  })

  it('refuses a batch size below one', async () => {
    const context = await harness()
    await context.cycle.start()

    const error = await restoreBackup({
      destination: context.destination,
      driver: testDriver,
      destPath: join(temp.path, 'unbatched.db'),
      batchSize: 0,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_ERROR')
  })
})
