import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import type { Database } from '../../database.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { Sirannon } from '../../sirannon.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let db: Database
let reader: SQLiteConnection
let dbPath: string

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-poll-boundary-'))
  dbPath = join(tempDir, 'poll.db')
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('polldb', dbPath)
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  reader = await driver.open(dbPath, { walMode: true })
})

afterEach(async () => {
  await reader.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('change log polling', () => {
  it('returns a whole transaction even when it is larger than the batch size', async () => {
    const tracker = new ChangeTracker({ pollBatchSize: 3 })
    await tracker.advanceToLatest(reader)

    await db.transaction(async tx => {
      for (let id = 1; id <= 8; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'row'])
      }
    })

    const events = await tracker.poll(reader)

    expect(events).toHaveLength(8)
    expect(new Set(events.map(event => event.txId)).size).toBe(1)
    expect(tracker.pollEndedAtTxBoundary).toBe(true)
  })

  it('stops at the boundary between two transactions', async () => {
    const tracker = new ChangeTracker({ pollBatchSize: 10 })
    await tracker.advanceToLatest(reader)

    await db.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (2, 'first')")
    })
    await db.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (3, 'second')")
    })

    const events = await tracker.poll(reader)

    expect(events).toHaveLength(3)
    expect(new Set(events.map(event => event.txId)).size).toBe(2)
    expect(tracker.pollEndedAtTxBoundary).toBe(true)
  })
})
