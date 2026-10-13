import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { type CopylessDriverConfig, copylessDriver } from '../../helpers/fault-drivers/copyless-driver.js'
import { memoryDestination } from '../../helpers/memory-destination.js'

const SCHEDULED_FAILURE_WAIT_MS = 5_000

const CASES = (['better-sqlite3', 'node:sqlite'] as const).flatMap(base =>
  [false, true].flatMap(declaresSteppedCopy =>
    [false, true].map(writerWorker => ({
      label: `${base}, ${declaresSteppedCopy ? 'claiming' : 'declaring no'} stepped copy, ${writerWorker ? 'writes on the writer worker' : 'writes on the calling thread'}`,
      config: { base, declaresSteppedCopy } satisfies CopylessDriverConfig,
      writerWorker,
    })),
  ),
)

let dir: string
const registries: Sirannon[] = []

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-backup-without-copy-'))
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
  rmSync(dir, { recursive: true, force: true })
})

function readBodies(path: string): unknown[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    return raw
      .prepare('SELECT body FROM ledger ORDER BY id')
      .all()
      .map(row => (row as { body: string }).body)
  } finally {
    raw.close()
  }
}

describe.each(CASES)('backing up through a driver whose connections carry no copy call ($label)', ({
  config,
  writerWorker,
}) => {
  it('refuses each kind of backup with BACKUP_UNSUPPORTED, writes nothing anywhere, and keeps serving writes', async () => {
    const path = join(dir, 'ledger.db')
    const sirannon = new Sirannon({ driver: copylessDriver(config) })
    registries.push(sirannon)
    const db = await sirannon.open('ledger', path, { writerWorker })
    await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.execute("INSERT INTO ledger (body) VALUES ('opening balance')")
    if (!config.declaresSteppedCopy) {
      expect(db.backupCapabilities()).toMatchObject({
        fullCopy: false,
        streamedCopy: false,
        stagedCopy: false,
        schedule: false,
      })
    }

    const fileCopy = join(dir, 'copies', 'ledger-copy.db')
    await expect(db.backup(fileCopy)).rejects.toMatchObject({ code: 'BACKUP_UNSUPPORTED' })
    expect(existsSync(fileCopy)).toBe(false)

    const destination = memoryDestination()
    const stagingDir = join(dir, 'staging')
    mkdirSync(stagingDir)
    await expect(db.backupTo({ destination, stagingDir })).rejects.toMatchObject({ code: 'BACKUP_UNSUPPORTED' })
    expect(destination.names()).toEqual([])
    expect(readdirSync(stagingDir)).toEqual([])

    const scheduledDir = join(dir, 'scheduled')
    const scheduledFailures: unknown[] = []
    db.scheduleBackup({ cron: '* * * * * *', destDir: scheduledDir, onError: err => scheduledFailures.push(err) })
    await vi.waitFor(() => expect(scheduledFailures.length).toBeGreaterThan(0), {
      timeout: SCHEDULED_FAILURE_WAIT_MS,
    })
    expect(scheduledFailures[0]).toMatchObject({ code: 'BACKUP_UNSUPPORTED' })
    expect(readdirSync(scheduledDir)).toEqual([])

    await db.execute("INSERT INTO ledger (body) VALUES ('after the refusals')")
    expect(readBodies(path)).toEqual(['opening balance', 'after the refusals'])
  })
})
