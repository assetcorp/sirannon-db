import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { assembleFromDestination } from '../../core/backup/assemble.js'
import type { BackupDestination, BackupPiece } from '../../core/backup/destination.js'
import { Database } from '../../core/database.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'

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

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-large-copy-'))
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

describe('full copy past the pending byte page', () => {
  it(
    'assembles a source larger than 1 GiB into a file SQLite opens',
    async () => {
      const sourcePath = join(dir, 'large.db')
      const db = await Database.create('large', sourcePath, betterSqlite3(), { synchronous: 'off' })
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

      const destination = fileDestination(join(dir, 'pieces'))
      const report = await db.backupTo({
        destination,
        name: 'large.db',
        pieceBytes: PIECE_BYTES,
        stagingDir: dir,
      })
      const rowsBefore = await db.queryOne<{ n: number }>('SELECT count(*) AS n FROM blobs')
      await db.close()

      expect(report.restarts).toBe(0)
      expect(report.bytesWritten).toBeGreaterThan(PENDING_BYTE_OFFSET)
      expect(report.pieceCount).toBe(Math.ceil(report.bytesWritten / PIECE_BYTES))

      const assembledPath = join(dir, 'assembled.db')
      const assembled = await assembleFromDestination(destination, report, assembledPath)
      expect(assembled.bytesWritten).toBe(report.bytesWritten)

      const verify = await Database.create('verify', assembledPath, betterSqlite3(), { walMode: false })
      expect(await verify.queryOne<{ integrity_check: string }>('PRAGMA integrity_check')).toEqual({
        integrity_check: 'ok',
      })
      expect(await verify.queryOne<{ n: number }>('SELECT count(*) AS n FROM blobs')).toEqual(rowsBefore)
      await verify.close()
    },
    COPY_TIMEOUT_MS,
  )
})
