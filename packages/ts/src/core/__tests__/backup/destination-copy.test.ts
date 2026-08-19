import { existsSync, mkdirSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { assembleFromDestination } from '../../backup/assemble.js'
import type { BackupProgress, BackupRunReport } from '../../backup/report.js'
import { copyToDestinationStaged } from '../../backup/staged-copy.js'
import type { SQLiteConnection } from '../../driver/types.js'
import type { SirannonError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { memoryDestination } from './memory-destination.js'
import { createTestDb, tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()

describe('copyToDestinationStaged', () => {
  it('sends the whole database to the destination in fixed-size pieces', async () => {
    const conn = await createTestDb(temp.path)
    const destination = memoryDestination()

    const report = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination,
      name: 'copy.db',
      pieceBytes: 4096,
      stagingDir: temp.path,
    })

    expect(report.destinationName).toBe('copy.db')
    expect(report.kind).toBe('full')
    expect(report.route).toBe('staged')
    expect(report.pieceCount).toBeGreaterThan(0)
    expect(report.bytesWritten).toBe(report.pageCount * report.pageSize)
    expect(destination.bytesFor('copy.db').byteLength).toBe(report.bytesWritten)
    await conn.close()
  })

  it('reports the run identifier, the timings, the pages, and the restart count', async () => {
    const conn = await createTestDb(temp.path)

    const report = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: memoryDestination(),
      stagingDir: temp.path,
    })

    expect(report.runId).toMatch(/^[0-9a-f]{16}$/)
    expect(report.databaseId).toBe('main')
    expect(report.finishedAt).toBeGreaterThanOrEqual(report.startedAt)
    expect(report.durationMs).toBeGreaterThanOrEqual(0)
    expect(report.copyMs).toBeGreaterThanOrEqual(0)
    expect(report.transferMs).toBeGreaterThanOrEqual(0)
    expect(report.pageCount).toBeGreaterThan(0)
    expect(report.pageSize).toBeGreaterThan(0)
    expect(report.restarts).toBe(0)
    expect(report.fingerprint).toMatch(/^[0-9a-f]{64}$/)
    await conn.close()
  })

  it('reports progress at step resolution while the copy runs', async () => {
    const conn = await createTestDb(temp.path)
    const seen: BackupProgress[] = []

    const report = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: memoryDestination(),
      pieceBytes: 4096,
      pagesPerStep: 1,
      stagingDir: temp.path,
      onProgress: progress => seen.push(progress),
    })

    expect(seen.filter(p => p.phase === 'copy').length).toBeGreaterThan(0)
    expect(seen.filter(p => p.phase === 'transfer').length).toBe(report.pieceCount)
    expect(seen.every(p => p.runId === report.runId)).toBe(true)
    expect(seen.at(-1)?.bytesWritten).toBe(report.bytesWritten)
    await conn.close()
  })

  it('leaves SQLite able to open the file the destination assembles', async () => {
    const conn = await createTestDb(temp.path)
    const destination = memoryDestination()

    const report = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination,
      name: 'assembled.db',
      pieceBytes: 4096,
      stagingDir: temp.path,
    })

    const assembledPath = join(temp.path, 'assembled-copy.db')
    const assembled = await assembleFromDestination(destination, report, assembledPath)
    expect(assembled.bytesWritten).toBe(report.bytesWritten)
    expect(assembled.fingerprint).toBe(report.fingerprint)

    const verify = await testDriver.open(assembledPath, { readonly: true, walMode: false })
    const stmt = await verify.prepare('SELECT name FROM users ORDER BY id')
    expect(await stmt.all()).toEqual([{ name: 'Alice' }, { name: 'Bob' }])
    await verify.close()
    await conn.close()
  })

  it('keeps a second name beside the target apart from the first', async () => {
    const conn = await createTestDb(temp.path)
    const destination = memoryDestination()

    await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination,
      name: 'copy.db',
      stagingDir: temp.path,
    })
    await destination.writePiece('copy.db-journal', 0, new Uint8Array([1, 2, 3]))

    expect(destination.names().sort()).toEqual(['copy.db', 'copy.db-journal'])
    expect(await destination.listPieces('copy.db-journal')).toEqual([{ index: 0, byteLength: 3 }])
    await conn.close()
  })

  it('names the piece the destination refused', async () => {
    const conn = await createTestDb(temp.path)
    const destination = memoryDestination()
    destination.refusePiece(0)

    const error = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination,
      name: 'copy.db',
      stagingDir: temp.path,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain("piece 0 of 'copy.db'")
    await conn.close()
  })

  it('removes the staged local file whether the run succeeds or fails', async () => {
    const conn = await createTestDb(temp.path)
    const stagingDir = join(temp.path, 'staging')
    const { mkdirSync } = await import('node:fs')
    mkdirSync(stagingDir, { recursive: true })

    await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: memoryDestination(),
      stagingDir,
    })
    expect(readdirSync(stagingDir)).toEqual([])

    const refusing = memoryDestination()
    refusing.refusePiece(0)
    await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: refusing,
      stagingDir,
    }).catch(() => {})
    expect(readdirSync(stagingDir)).toEqual([])
    expect(existsSync(stagingDir)).toBe(true)
    await conn.close()
  })

  it('leaves the staged file alone until a stalled copy settles', async () => {
    const stagingDir = join(temp.path, 'stalled-staging')
    mkdirSync(stagingDir, { recursive: true })
    let releaseCopy: (() => void) | undefined
    const conn = {
      copyDatabase() {
        return new Promise(resolve => {
          releaseCopy = () => resolve({ totalPages: 0, remainingPages: 0 })
        })
      },
      async prepare() {
        return {
          async get() {
            return { page_size: 4096 }
          },
        }
      },
    } as unknown as SQLiteConnection

    const error = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: memoryDestination(),
      stagingDir,
      stallTimeoutMs: 20,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_STALLED')
    expect(readdirSync(stagingDir)).toHaveLength(1)

    releaseCopy?.()
    await vi.waitFor(() => expect(readdirSync(stagingDir)).toHaveLength(0))
  })

  it('leaves the fingerprint out when the caller turns it off', async () => {
    const conn = await createTestDb(temp.path)

    const report = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: memoryDestination(),
      fingerprint: false,
      stagingDir: temp.path,
    })

    expect(report.fingerprint).toBeUndefined()
    await conn.close()
  })

  it('refuses a piece size that is not a positive whole number of bytes', async () => {
    const conn = await createTestDb(temp.path)

    const error = await copyToDestinationStaged(conn, {
      databaseId: 'main',
      sourcePath: join(temp.path, 'source.db'),
      destination: memoryDestination(),
      pieceBytes: 0,
      stagingDir: temp.path,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_ERROR')
    await conn.close()
  })
})

describe('assembleFromDestination', () => {
  function reportFor(overrides: Partial<BackupRunReport>): BackupRunReport {
    return {
      runId: 'aaaaaaaaaaaaaaaa',
      databaseId: 'main',
      sourcePath: '/tmp/source.db',
      kind: 'full',
      chainId: 'bbbbbbbbbbbbbbbb',
      route: 'staged',
      destinationName: 'gapped.db',
      startedAt: 0,
      finishedAt: 1,
      durationMs: 1,
      copyMs: 1,
      transferMs: 0,
      pageCount: 1,
      pageSize: 4096,
      bytesWritten: 8,
      pieceCount: 2,
      pieceBytes: 4,
      restarts: 0,
      ...overrides,
    }
  }

  it('names the piece that is missing rather than writing a short file', async () => {
    const destination = memoryDestination()
    await destination.writePiece('gapped.db', 0, new Uint8Array(4))
    await destination.writePiece('gapped.db', 2, new Uint8Array(4))

    const error = await assembleFromDestination(destination, reportFor({}), join(temp.path, 'gapped-copy.db')).catch(
      (err: unknown) => err,
    )

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('missing piece 1')
  })

  it('refuses a name the destination holds no pieces for', async () => {
    const error = await assembleFromDestination(
      memoryDestination(),
      reportFor({ destinationName: 'absent.db' }),
      join(temp.path, 'absent-copy.db'),
    ).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
  })

  it('refuses a piece left behind by a longer run under the same name', async () => {
    const destination = memoryDestination()
    await destination.writePiece('gapped.db', 0, new Uint8Array(4))
    await destination.writePiece('gapped.db', 1, new Uint8Array(4))
    await destination.writePiece('gapped.db', 2, new Uint8Array(4))

    const error = await assembleFromDestination(destination, reportFor({}), join(temp.path, 'stale-copy.db')).catch(
      (err: unknown) => err,
    )

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('belongs to a different run')
  })

  it('refuses pieces whose bytes do not add up to what the run wrote', async () => {
    const destination = memoryDestination()
    await destination.writePiece('gapped.db', 0, new Uint8Array(4))
    await destination.writePiece('gapped.db', 1, new Uint8Array(2))

    const error = await assembleFromDestination(destination, reportFor({}), join(temp.path, 'short-copy.db')).catch(
      (err: unknown) => err,
    )

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('6 bytes where the run wrote 8')
  })

  it('refuses pieces that do not match the fingerprint the run recorded', async () => {
    const destination = memoryDestination()
    await destination.writePiece('gapped.db', 0, new Uint8Array(4))
    await destination.writePiece('gapped.db', 1, new Uint8Array([9, 9, 9, 9]))

    const error = await assembleFromDestination(
      destination,
      reportFor({ fingerprint: 'f'.repeat(64) }),
      join(temp.path, 'wrong-copy.db'),
    ).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_DESTINATION_ERROR')
    expect((error as SirannonError).message).toContain('fingerprint')
  })
})
