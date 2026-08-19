import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { assembleFromDestination } from '../../core/backup/assemble.js'
import type { BackupDestination, BackupPiece } from '../../core/backup/destination.js'
import { Database } from '../../core/database.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { builtStreamingExtensionPath } from '../helpers/streaming-extension.js'

const PENDING_BYTE_OFFSET = 1_073_741_824
const ROW_BYTES = 4000
const ROWS_PER_BATCH = 10_000
const BATCHES = 34
const PIECE_BYTES = 16 * 1024 * 1024
const COPY_TIMEOUT_MS = 900_000

function fileDestination(root: string): BackupDestination {
  mkdirSync(root, { recursive: true })
  const pathFor = (name: string, index: number) => join(root, `${name}.${index}`)
  return {
    async writePiece(name, index, bytes) {
      writeFileSync(pathFor(name, index), bytes)
    },
    async readPiece(name, index) {
      return new Uint8Array(readFileSync(pathFor(name, index)))
    },
    async listPieces(name): Promise<BackupPiece[]> {
      return readdirSync(root)
        .filter(entry => entry.startsWith(`${name}.`))
        .map(entry => ({
          index: Number(entry.slice(name.length + 1)),
          byteLength: statSync(join(root, entry)).size,
        }))
    },
  }
}

const extensionPath = builtStreamingExtensionPath()

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-large-stream-'))
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

describe('streamed copy past the pending byte page', () => {
  it.skipIf(!extensionPath)(
    'carries a source larger than 1 GiB to the destination without a local file',
    async () => {
      const sourcePath = join(dir, 'large.db')
      const driver = betterSqlite3(extensionPath ? { vfsExtensionPath: extensionPath } : {})
      const db = await Database.create('large', sourcePath, driver, { synchronous: 'off' })
      await db.execute('CREATE TABLE blobs (id INTEGER PRIMARY KEY, payload TEXT)')
      const payload = 'p'.repeat(ROW_BYTES)
      for (let batch = 0; batch < BATCHES; batch++) {
        await db.bulkLoad(
          'INSERT INTO blobs (payload) VALUES (?)',
          Array.from({ length: ROWS_PER_BATCH }, () => [payload]),
          { checkpoint: batch === BATCHES - 1 },
        )
      }
      expect(statSync(sourcePath).size).toBeGreaterThan(PENDING_BYTE_OFFSET)
      expect(db.backupCapabilities().streamedCopy).toBe(true)

      const destination = fileDestination(join(dir, 'pieces'))
      const filesBeforeRun = readdirSync(dir).sort()
      const report = await db.backupTo({ destination, name: 'large.db', pieceBytes: PIECE_BYTES })
      const filesAfterRun = readdirSync(dir).sort()
      const rowsBefore = await db.queryOne<{ n: number }>('SELECT count(*) AS n FROM blobs')
      await db.close()

      expect(report.route).toBe('streamed')
      expect(report.restarts).toBe(0)
      expect(report.bytesWritten).toBeGreaterThan(PENDING_BYTE_OFFSET)
      expect(report.pieceCount).toBe(Math.ceil(report.bytesWritten / PIECE_BYTES))
      expect(filesAfterRun).toEqual(filesBeforeRun)

      const assembledPath = join(dir, 'assembled.db')
      const assembled = await assembleFromDestination(destination, report, assembledPath)
      expect(assembled.bytesWritten).toBe(report.bytesWritten)
      expect(statSync(assembledPath).size).toBe(report.bytesWritten)

      const restored = await Database.create('restored', assembledPath, betterSqlite3())
      const rowsAfter = await restored.queryOne<{ n: number }>('SELECT count(*) AS n FROM blobs')
      const integrity = await restored.queryOne<{ integrity_check: string }>('PRAGMA integrity_check')
      await restored.close()

      expect(rowsAfter?.n).toBe(rowsBefore?.n)
      expect(integrity?.integrity_check).toBe('ok')
    },
    COPY_TIMEOUT_MS,
  )
})
