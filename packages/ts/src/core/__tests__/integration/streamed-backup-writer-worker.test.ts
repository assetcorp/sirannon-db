import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  builtStreamingExtensionPath,
  driverStreamsToDestination,
} from '../../../__tests__/helpers/streaming-extension.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { assembleFromDestination } from '../../backup/assemble.js'
import { Database } from '../../database.js'
import { memoryDestination } from '../backup/memory-destination.js'

const extensionPath = builtStreamingExtensionPath()
const driver = betterSqlite3(extensionPath ? { vfsExtensionPath: extensionPath } : {})
const streams = driverStreamsToDestination(driver)

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-worker-stream-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Streamed backup through the writer worker', () => {
  it.skipIf(!streams)(
    'carries the copy while the writes run on the worker thread',
    async () => {
      const sourcePath = join(tempDir, 'source.db')
      const db = await Database.create('test', sourcePath, driver, { writerWorker: true })
      await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await db.bulkLoad(
        'INSERT INTO users (name) VALUES (?)',
        Array.from({ length: 4000 }, (_, index) => [`user-${index}`.padEnd(200, 'x')]),
      )
      const destination = memoryDestination()
      const beforeRun = readdirSync(tempDir).sort()

      const report = await db.backupTo({ destination, name: 'copy.db', pieceBytes: 65536 })
      const afterRun = readdirSync(tempDir).sort()

      expect(report.route).toBe('streamed')
      expect(report.pieceCount).toBeGreaterThan(1)
      expect(afterRun).toEqual(beforeRun)

      const assembledPath = join(tempDir, 'assembled.db')
      await assembleFromDestination(destination, report, assembledPath)
      await db.close()

      const restored = await Database.create('restored', assembledPath, betterSqlite3())
      const rows = await restored.query<{ total: number }>('SELECT count(*) AS total FROM users')
      expect(rows[0]?.total).toBe(4000)
      await restored.close()
    },
    30_000,
  )
})
