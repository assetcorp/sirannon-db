import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { memoryDestination } from '../../helpers/memory-destination.js'
import { builtStreamingExtensionPath, nodeSqliteParsesBackupUris } from '../../helpers/streaming-extension.js'
import { codeOf, createWorkspace, failureOf, ORDER_PADDING, seedOrders, sha256, type Workspace } from './harness.js'

const extensionPath = builtStreamingExtensionPath()
const vfsOptions = extensionPath ? { vfsExtensionPath: extensionPath } : {}
const bothDriversStream = extensionPath !== null && nodeSqliteParsesBackupUris()

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('two-builds')
})

afterEach(async () => {
  await workspace.dispose()
})

async function streamOneCopy(id: string, driver: SQLiteDriver): Promise<void> {
  const db = await workspace.open(id, {}, driver)
  await seedOrders(db, id, 200, ORDER_PADDING)
  const destination = memoryDestination()
  try {
    const report = await db.backupTo({ destination, name: 'copy.db', pieceBytes: 65_536 })
    expect(report.route).toBe('streamed')
    expect(sha256(destination.bytesFor('copy.db'))).toBe(report.fingerprint)
  } finally {
    await db.close()
  }
}

describe('streaming through two SQLite builds in one process', () => {
  it.skipIf(!bothDriversStream)(
    'streams through the first build, refuses the second, and names the reason',
    async () => {
      await streamOneCopy('first', betterSqlite3(vfsOptions))

      const error = await failureOf(streamOneCopy('second', nodeSqlite(vfsOptions)))

      expect(error.message).toContain('one process loads it into one build')
      expect(codeOf(error)).toBe('EXTENSION_ERROR')
      await streamOneCopy('third', betterSqlite3(vfsOptions))
    },
  )
})
