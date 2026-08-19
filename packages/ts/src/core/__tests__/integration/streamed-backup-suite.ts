import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { builtStreamingExtensionPath } from '../../../__tests__/helpers/streaming-extension.js'
import { assembleFromDestination } from '../../backup/assemble.js'
import type { BackupProgress } from '../../backup/report.js'
import { Database } from '../../database.js'
import type { SQLiteDriver } from '../../driver/types.js'
import { memoryDestination } from '../backup/memory-destination.js'

export interface StreamingExtensionOptions {
  vfsExtensionPath?: string
}

async function seedPages(db: Database, rows: number): Promise<void> {
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.bulkLoad(
    'INSERT INTO users (name) VALUES (?)',
    Array.from({ length: rows }, (_, index) => [`user-${index}`.padEnd(200, 'x')]),
  )
}

/**
 * Runs the streamed backup suite against one driver. Each driver needs a test
 * file of its own, because a process loads the compiled extension into one
 * SQLite build and the two drivers carry a build each.
 */
export function describeStreamedBackup(
  label: string,
  buildDriver: (options: StreamingExtensionOptions) => SQLiteDriver,
): void {
  const extensionPath = builtStreamingExtensionPath()
  const driver = buildDriver(extensionPath ? { vfsExtensionPath: extensionPath } : {})
  let tempDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-streamed-'))
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  describe(`Streamed backup on ${label}`, () => {
    it.skipIf(!extensionPath)('reports streaming as available', async () => {
      const db = await Database.create('test', join(tempDir, 'capabilities.db'), driver)

      expect(db.backupCapabilities()).toEqual({
        fullCopy: true,
        streamedCopy: true,
        stagedCopy: true,
        localDiskRequired: 'none',
        schedule: true,
      })

      await db.close()
    })

    it.skipIf(!extensionPath)('carries a whole copy to the destination and writes no local file', async () => {
      const db = await Database.create('test', join(tempDir, 'source.db'), driver)
      await seedPages(db, 4000)
      const destination = memoryDestination()
      const beforeRun = readdirSync(tempDir).sort()

      const report = await db.backupTo({ destination, name: 'copy.db', pieceBytes: 65536 })

      expect(report.route).toBe('streamed')
      expect(report.pieceCount).toBeGreaterThan(1)
      expect(report.bytesWritten).toBe(report.pageCount * report.pageSize)
      expect(report.fingerprint).toMatch(/^[0-9a-f]{64}$/)
      expect(destination.names()).toEqual(['copy.db'])
      expect(readdirSync(tempDir).sort()).toEqual(beforeRun)

      const assembledPath = join(tempDir, 'assembled.db')
      await assembleFromDestination(destination, report, assembledPath)
      await db.close()

      const restored = await Database.create('restored', assembledPath, driver)
      const rows = await restored.query<{ total: number }>('SELECT count(*) AS total FROM users')
      expect(rows[0]?.total).toBe(4000)
      await restored.close()
    })

    it.skipIf(!extensionPath)('reports the pieces travelling while the copy runs', async () => {
      const db = await Database.create('test', join(tempDir, 'progress.db'), driver)
      await seedPages(db, 4000)
      const seen: BackupProgress[] = []

      const report = await db.backupTo({
        destination: memoryDestination(),
        pieceBytes: 65536,
        pagesPerStep: 8,
        onProgress: progress => seen.push(progress),
      })

      expect(seen.filter(progress => progress.phase === 'copy').length).toBeGreaterThan(0)
      expect(seen.filter(progress => progress.phase === 'transfer').length).toBe(report.pieceCount)
      expect(seen.every(progress => progress.runId === report.runId)).toBe(true)
      expect(seen.at(-1)?.bytesWritten).toBe(report.bytesWritten)
      await db.close()
    })

    it.skipIf(!extensionPath)('refuses a piece size SQLite cannot write in whole blocks', async () => {
      const db = await Database.create('test', join(tempDir, 'blocks.db'), driver)
      await seedPages(db, 10)

      await expect(db.backupTo({ destination: memoryDestination(), pieceBytes: 5000 })).rejects.toThrow(
        'must divide by 512',
      )

      await db.close()
    })

    it.skipIf(!extensionPath)('keeps its virtual file system after the connection that loaded it closes', async () => {
      const sourcePath = join(tempDir, 'permanent.db')
      const db = await Database.create('test', sourcePath, driver)
      await seedPages(db, 10)
      await db.backupTo({ destination: memoryDestination(), pieceBytes: 65536 })
      await db.close()

      const connection = await driver.open(sourcePath, { walMode: false })
      const failure = await connection
        .copyDatabase?.({ destPath: 'file:sirannon-stream-999999?vfs=sirannon', pagesPerStep: 16 })
        .then(() => 'the copy reached a stream that was never opened')
        .catch((err: Error) => err.message)
      await connection.close()

      expect(failure).toBeDefined()
      expect(failure).not.toContain('no such vfs')
    })

    it.skipIf(!extensionPath)('reports what the destination refused', async () => {
      const db = await Database.create('test', join(tempDir, 'refusal.db'), driver)
      await seedPages(db, 4000)
      const destination = memoryDestination()
      destination.refuseName('refused.db')

      await expect(db.backupTo({ destination, name: 'refused.db', pieceBytes: 65536 })).rejects.toThrow(
        'The destination refused piece',
      )

      await db.close()
    })
  })
}
