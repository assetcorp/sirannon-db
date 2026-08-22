import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  builtStreamingExtensionPath,
  nodeSqliteParsesBackupUris,
} from '../../../__tests__/helpers/streaming-extension.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { Database } from '../../database.js'
import type { SQLiteDriver } from '../../driver/types.js'
import { memoryDestination } from '../backup/memory-destination.js'

const extensionPath = builtStreamingExtensionPath()
const vfsOptions = extensionPath ? { vfsExtensionPath: extensionPath } : {}
const bothDriversStream = extensionPath !== null && nodeSqliteParsesBackupUris()

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-one-build-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

async function streamOneCopy(id: string, driver: SQLiteDriver): Promise<void> {
  const db = await Database.create(id, join(tempDir, `${id}.db`), driver)
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.bulkLoad(
    'INSERT INTO users (name) VALUES (?)',
    Array.from({ length: 200 }, (_, index) => [`user-${index}`.padEnd(200, 'x')]),
  )
  try {
    await db.backupTo({ destination: memoryDestination(), pieceBytes: 65536 })
  } finally {
    await db.close()
  }
}

describe('Streaming through two SQLite builds in one process', () => {
  it.skipIf(!bothDriversStream)('refuses the second build and names the reason', async () => {
    await streamOneCopy('first', betterSqlite3(vfsOptions))

    await expect(streamOneCopy('second', nodeSqlite(vfsOptions))).rejects.toThrow('one process loads it into one build')
  })
})
