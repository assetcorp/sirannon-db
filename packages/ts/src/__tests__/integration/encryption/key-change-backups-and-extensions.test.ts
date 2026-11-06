import { randomBytes } from 'node:crypto'
import { writeFileSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  compiledExtensionPath,
  EXTENSION_PROBE_FUNCTION,
  EXTENSION_PROBE_VALUE,
} from '../../helpers/compiled-extension.js'
import { holdingCopyDriver } from '../../helpers/fault-drivers/holding-copy-driver.js'
import { readWithIndependentAegis } from '../../helpers/independent-aegis.js'
import { readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import { createWorkspace, joinPieces, type Workspace } from '../backups/harness.js'
import { schedule } from '../backups/schedule-harness.js'

const binding = builtSirannonSqliteBindingPath()
const extension = compiledExtensionPath()
const SEEDED_INVOICES = 30_000
const INVOICES_AFTER_CHANGE = 25
const READ_POOL_SIZE = 3
const PIECE_BYTES = 65_536
const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 20_000
const SWAP_WAIT_MS = 250

interface Invoice {
  id: number
  secret: string
}

function secretOf(id: number): string {
  return `invoice-${id.toString().padStart(6, '0')}-confidential-${'x'.repeat(160)}`
}

function invoices(from: number, to: number): Invoice[] {
  return Array.from({ length: to - from + 1 }, (_, at) => ({ id: from + at, secret: secretOf(from + at) }))
}

function keyed(key: Uint8Array, id: string) {
  return { key: () => ({ key: Uint8Array.from(key), id }) }
}

function encryptingDriver(): SQLiteDriver {
  return betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined })
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise(resolve => setTimeout(resolve, WAIT_STEP_MS))
  }
}

async function seed(db: Database, from: number, to: number): Promise<void> {
  await db.bulkLoad(
    'INSERT INTO invoices (id, secret) VALUES (?, ?)',
    invoices(from, to).map(invoice => [invoice.id, invoice.secret]),
  )
}

function readInvoices(path: string, key: Uint8Array): Invoice[] {
  return readWithIndependentAegis<Invoice>(path, key, 'SELECT id, secret FROM invoices ORDER BY id').map(row => ({
    id: Number(row.id),
    secret: String(row.secret),
  }))
}

describe.skipIf(binding === null)('backups and extensions after a key change on the better-sqlite3 driver', () => {
  let workspace: Workspace
  const firstKey = randomBytes(32)
  const secondKey = randomBytes(32)

  beforeEach(() => {
    workspace = createWorkspace('key-change-reopened')
  })

  afterEach(async () => {
    await workspace.dispose()
  })

  it('writes every scheduled backup after a key change from the new file under the new key', async () => {
    const db = await workspace
      .registry(encryptingDriver())
      .open('scheduled', workspace.path('scheduled.db'), { encryption: keyed(firstKey, 'first') })
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY, secret TEXT NOT NULL)')
    await seed(db, 1, 200)
    const scheduled = schedule(db, { destDir: workspace.path('scheduled-copies') })
    await waitFor(() => scheduled.reports.length >= 1, 'a scheduled backup before the key change')

    await db.changeEncryption({ key: keyed(secondKey, 'second').key })
    await seed(db, 201, 200 + INVOICES_AFTER_CHANGE)
    const reportsBeforeWrites = scheduled.reports.length
    await waitFor(
      () => scheduled.reports.length >= reportsBeforeWrites + 2 || scheduled.errors.length > 0,
      'two scheduled backups after the key change',
    )

    expect(scheduled.errors).toEqual([])
    const newest = scheduled.reports.at(-1)
    if (newest === undefined) throw new Error('no scheduled backup report')
    expect(readInvoices(newest.destPath, secondKey)).toEqual(invoices(1, 200 + INVOICES_AFTER_CHANGE))
    expect(() => readInvoices(newest.destPath, firstKey)).toThrow()
    expect(() => readWithStockSqlite(newest.destPath, 'SELECT count(*) FROM invoices')).toThrow()
    const restored = await workspace
      .registry(encryptingDriver())
      .open('restored', newest.destPath, { encryption: keyed(secondKey, 'second'), readOnly: true })
    expect(await restored.query('SELECT count(*) AS n FROM invoices')).toEqual([{ n: 200 + INVOICES_AFTER_CHANGE }])
  }, 30_000)

  describe.skipIf(extension === null).each([false, true])('with the writer worker set to %s', writerWorker => {
    it('keeps every loaded extension on the writer and on every reader after a key change', async () => {
      const db = await workspace.registry(encryptingDriver()).open('extended', workspace.path('extended.db'), {
        encryption: keyed(firstKey, 'first'),
        readPoolSize: READ_POOL_SIZE,
        writerWorker,
      })
      await db.execute('CREATE TABLE marks (id INTEGER PRIMARY KEY, mark TEXT NOT NULL)')
      await db.loadExtension(extension ?? '')
      const probe = `SELECT ${EXTENSION_PROBE_FUNCTION}() AS mark`
      expect(await db.query(probe)).toEqual([{ mark: EXTENSION_PROBE_VALUE }])

      await db.changeEncryption({ key: keyed(secondKey, 'second').key })

      for (let read = 0; read <= READ_POOL_SIZE; read++) {
        expect(await db.query(probe)).toEqual([{ mark: EXTENSION_PROBE_VALUE }])
      }
      await db.execute(`INSERT INTO marks (mark) VALUES (${EXTENSION_PROBE_FUNCTION}())`)
      expect(await db.query('SELECT mark FROM marks')).toEqual([{ mark: EXTENSION_PROBE_VALUE }])
    })
  })

  it('finishes a file copy and a destination copy that run when the swap begins, each under the old key alone', async () => {
    const releaseAtCount = SEEDED_INVOICES + 1
    const driver = holdingCopyDriver({
      runtime: 'better-sqlite3',
      countSql: 'SELECT count(*) AS n FROM invoices',
      releaseAtCount,
      encryptingBindingPath: binding ?? undefined,
    })
    const db = await workspace
      .registry(driver)
      .open('in-flight', workspace.path('in-flight.db'), { encryption: keyed(firstKey, 'first') })
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY, secret TEXT NOT NULL)')
    await seed(db, 1, SEEDED_INVOICES)

    const settled: string[] = []
    const fileCopyPath = workspace.path('in-flight-copy.db')
    const fileCopy = db.backup(fileCopyPath).then(report => {
      settled.push('file copy')
      return report
    })
    const destination = workspace.destination()
    const destinationCopy = db.backupTo({ destination, name: 'in-flight', pieceBytes: PIECE_BYTES }).then(report => {
      settled.push('destination copy')
      return report
    })
    const change = db.changeEncryption({ key: keyed(secondKey, 'second').key }).then(status => {
      settled.push('key change')
      return status
    })
    let state = ''
    await waitFor(() => {
      void db.encryptionStatus().then(status => {
        state = status.change?.state ?? state
      })
      return state === 'swapping' || settled.includes('key change')
    }, 'the key change to reach the swap')
    await new Promise(resolve => setTimeout(resolve, SWAP_WAIT_MS))
    await db.execute('INSERT INTO invoices (id, secret) VALUES (?, ?)', [releaseAtCount, secretOf(releaseAtCount)])

    const [fileReport, destinationReport, status] = await Promise.all([fileCopy, destinationCopy, change])

    expect(status.change.state).toBe('done')
    expect(settled.sort()).toEqual(['destination copy', 'file copy', 'key change'])
    expect(readInvoices(fileReport.destPath, firstKey)).toEqual(invoices(1, releaseAtCount))
    expect(() => readInvoices(fileReport.destPath, secondKey)).toThrow()
    const joinedPath = workspace.path('in-flight-joined.db')
    writeFileSync(joinedPath, joinPieces(destination, 'in-flight', PIECE_BYTES))
    expect(destinationReport.bytesWritten).toBe(destinationReport.pageCount * destinationReport.pageSize)
    expect(readInvoices(joinedPath, firstKey)).toEqual(invoices(1, releaseAtCount))
    expect(() => readInvoices(joinedPath, secondKey)).toThrow()
    expect(await db.query('SELECT count(*) AS n FROM invoices')).toEqual([{ n: releaseAtCount }])
    const after = await db.backup(workspace.path('after-change.db'))
    expect(readInvoices(after.destPath, secondKey)).toEqual(invoices(1, releaseAtCount))
  }, 30_000)
})
