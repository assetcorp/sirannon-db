import { randomBytes } from 'node:crypto'
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { readWithIndependentAegis, readWithMultipleCiphersAegis } from '../../helpers/independent-aegis.js'
import { filesHoldingText, readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import { builtStreamingExtensionPath, driverStreamsToDestination } from '../../helpers/streaming-extension.js'
import { newestChainId } from '../backups/cycle-harness.js'
import { createWorkspace, joinPieces, type Workspace } from '../backups/harness.js'
import { advanceSeconds, schedule, startScheduleClock } from '../backups/schedule-harness.js'

const SEEDED_INVOICES = 2000
const PIECE_BYTES = 65_536
const PLAINTEXT_MARKERS = ['confidential', 'CREATE TABLE invoices']

const binding = builtSirannonSqliteBindingPath()
const extensionPath = builtStreamingExtensionPath()

interface Invoice {
  id: number
  secret: string
}

function secretOf(id: number): string {
  return `invoice-${id.toString().padStart(6, '0')}-confidential-${'x'.repeat(120)}`
}

function encryptingDriver(streamed: boolean): SQLiteDriver {
  return betterSqlite3({
    encryption: true,
    sqliteBindingPath: binding ?? undefined,
    ...(streamed && extensionPath !== null ? { vfsExtensionPath: extensionPath } : {}),
  })
}

function keyed(key: Uint8Array, id: string) {
  return { encryption: { key: () => ({ key: Uint8Array.from(key), id }) } }
}

function filesUnder(root: string): string[] {
  if (!existsSync(root)) return []
  return readdirSync(root, { recursive: true, encoding: 'utf8' })
    .map(entry => join(root, entry))
    .filter(path => statSync(path).isFile())
}

function filesHoldingKey(paths: readonly string[], key: Uint8Array): string[] {
  const forms = [
    Buffer.from(key),
    Buffer.from(Buffer.from(key).toString('hex')),
    Buffer.from(Buffer.from(key).toString('base64')),
  ]
  return paths.filter(path => {
    const bytes = readFileSync(path)
    return forms.some(form => bytes.includes(form))
  })
}

async function seedInvoices(db: Database, from: number, count: number): Promise<void> {
  const rows = Array.from({ length: count }, (_, at) => [from + at, secretOf(from + at)])
  await db.bulkLoad('INSERT INTO invoices (id, secret) VALUES (?, ?)', rows)
}

function expectedInvoices(ids: readonly number[]): Invoice[] {
  return ids.map(id => ({ id, secret: secretOf(id) }))
}

function range(from: number, count: number): number[] {
  return Array.from({ length: count }, (_, at) => from + at)
}

function expectCiphertextCopy(path: string, key: Uint8Array, ids: readonly number[]): void {
  expect(statSync(path).size).toBeGreaterThan(SEEDED_INVOICES * 100)
  expect(filesHoldingText([path, `${path}-wal`], PLAINTEXT_MARKERS)).toEqual([])
  expect(filesHoldingKey([path], key)).toEqual([])
  expect(() => readWithStockSqlite(path, 'SELECT count(*) FROM invoices')).toThrow()
  expect(readWithIndependentAegis<Invoice>(path, key, 'SELECT id, secret FROM invoices ORDER BY id')).toEqual(
    expectedInvoices(ids),
  )
  expect(readWithMultipleCiphersAegis<Invoice>(path, key, 'SELECT id, secret FROM invoices ORDER BY id')).toEqual(
    expectedInvoices(ids),
  )
}

describe.skipIf(binding === null)('backups of an encrypted database', () => {
  let workspace: Workspace
  const key = randomBytes(32)

  beforeEach(() => {
    workspace = createWorkspace('encrypted')
  })

  afterEach(async () => {
    vi.useRealTimers()
    await workspace.dispose()
  })

  async function openSeeded(name: string, driver: SQLiteDriver): Promise<Database> {
    const db = await workspace.registry(driver).open(name, workspace.path(`${name}.db`), keyed(key, 'invoices-2026'))
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY, secret TEXT NOT NULL)')
    await seedInvoices(db, 1, SEEDED_INVOICES)
    return db
  }

  it('writes a file copy that holds only ciphertext under the same key and opens again through Sirannon', async () => {
    const db = await openSeeded('filed', encryptingDriver(false))
    const copy = workspace.path('filed-copy.db')
    await db.backup(copy)

    expectCiphertextCopy(copy, key, range(1, SEEDED_INVOICES))
    const reopened = await workspace.registry(encryptingDriver(false)).open('copy', copy, keyed(key, 'invoices-2026'))
    expect(await reopened.query('SELECT count(*) AS n FROM invoices')).toEqual([{ n: SEEDED_INVOICES }])
    const refused = await workspace
      .registry(encryptingDriver(false))
      .open('copy-wrong', copy, keyed(randomBytes(32), 'other'))
      .catch(err => err)
    expect(refused).toMatchObject({ code: 'ENCRYPTION_KEY_REJECTED' })
  })

  it('writes every scheduled copy as ciphertext under the same key', async () => {
    const db = await openSeeded('scheduled', encryptingDriver(false))
    const destDir = workspace.path('scheduled-copies')
    startScheduleClock('2026-03-01T10:00:00.500Z')
    const scheduled = schedule(db, { destDir })
    await advanceSeconds(2)
    vi.useRealTimers()

    expect(scheduled.errors).toEqual([])
    expect(scheduled.reports.length).toBeGreaterThanOrEqual(1)
    for (const report of scheduled.reports) expectCiphertextCopy(report.destPath, key, range(1, SEEDED_INVOICES))
  })

  it.each([
    { route: 'through a local file', streamed: false },
    { route: 'streamed through the compiled extension', streamed: true },
  ])('sends a copy to a destination $route that holds only ciphertext under the same key', async ({ streamed }) => {
    const driver = encryptingDriver(streamed)
    if (streamed && !driverStreamsToDestination(driver)) return
    expect(driverStreamsToDestination(driver)).toBe(streamed)
    const db = await openSeeded('sent', driver)
    const destination = workspace.destination()
    const report = await db.backupTo({ destination, name: 'invoices-copy', pieceBytes: PIECE_BYTES })

    expect(report.route).toBe(streamed ? 'streamed' : 'staged')
    expect(filesHoldingText(filesUnder(destination.root), PLAINTEXT_MARKERS)).toEqual([])
    expect(filesHoldingKey(filesUnder(destination.root), key)).toEqual([])
    const joined = joinPieces(destination, 'invoices-copy', PIECE_BYTES)
    expect(joined.byteLength).toBe(report.bytesWritten)
    expect(report.bytesWritten).toBe(report.pageCount * report.pageSize)
    const joinedPath = workspace.path('sent-joined.db')
    writeFileSync(joinedPath, joined)
    expectCiphertextCopy(joinedPath, key, range(1, SEEDED_INVOICES))
  })

  it('captures a chain whose every piece is ciphertext, records the key identifier, and restores only under that key', async () => {
    const destination = workspace.destination()
    const stagingDir = workspace.path('staging')
    const errors: Error[] = []
    const path = workspace.path('chained.db')
    const sirannon = workspace.registry(encryptingDriver(false))
    const db = await sirannon.open('chained', path, {
      ...keyed(key, 'invoices-2026'),
      backups: { destination, intervalMs: 0, stagingDir, onError: err => errors.push(err) },
    })
    await db.captureBackupChanges()
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY, secret TEXT NOT NULL)')
    await seedInvoices(db, 1, SEEDED_INVOICES)
    await db.captureBackupChanges()
    await seedInvoices(db, SEEDED_INVOICES + 1, 50)
    await db.captureBackupChanges()

    expect(errors).toEqual([])
    const stored = [...filesUnder(destination.root), ...filesUnder(stagingDir)]
    expect(stored.length).toBeGreaterThan(2)
    expect(filesHoldingText(stored, PLAINTEXT_MARKERS)).toEqual([])
    expect(filesHoldingKey(stored, key)).toEqual([])
    const [chain] = await db.backupChain()
    expect(chain?.base?.keyId).toBe('invoices-2026')
    expect(chain?.changes.length).toBeGreaterThanOrEqual(2)
    for (const change of chain?.changes ?? []) expect(change.keyId).toBe('invoices-2026')

    const restore = (destPath: string, encryption?: { key: () => Uint8Array }) =>
      restoreBackup({ destination, driver: encryptingDriver(false), destPath, ...(encryption ? { encryption } : {}) })
    const missing = await restore(workspace.path('restored-without-key.db')).catch(err => err)
    expect(missing).toMatchObject({ code: 'ENCRYPTION_KEY_MISSING' })
    const wrong = await restore(workspace.path('restored-wrong-key.db'), { key: () => randomBytes(32) }).catch(
      err => err,
    )
    expect(wrong).toMatchObject({ code: 'ENCRYPTION_KEY_REJECTED' })
    expect(String(wrong.message)).toContain('invoices-2026')

    const restored = workspace.path('restored.db')
    await restore(restored, { key: () => Uint8Array.from(key) })
    expectCiphertextCopy(restored, key, range(1, SEEDED_INVOICES + 50))
  })

  it('starts a new chain under the new key after a key change, and restores it only under that key', async () => {
    const destination = workspace.destination()
    const stagingDir = workspace.path('staging')
    const errors: Error[] = []
    const sirannon = workspace.registry(encryptingDriver(false))
    const db = await sirannon.open('rekeyed', workspace.path('rekeyed.db'), {
      ...keyed(key, 'invoices-2026'),
      backups: { destination, intervalMs: 0, stagingDir, onError: err => errors.push(err) },
    })
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY, secret TEXT NOT NULL)')
    await seedInvoices(db, 1, SEEDED_INVOICES)
    await db.captureBackupChanges()
    const firstChain = newestChainId(destination)

    const newKey = randomBytes(32)
    await db.changeEncryption({ key: () => ({ key: Uint8Array.from(newKey), id: 'invoices-2027' }) })
    await seedInvoices(db, SEEDED_INVOICES + 1, 10)
    await db.captureBackupChanges()
    await db.captureBackupChanges()

    expect(errors).toEqual([])
    const secondChain = newestChainId(destination)
    expect(secondChain).not.toBe(firstChain)
    const chains = await db.backupChain()
    const newest = chains.find(chain => chain.chainId === secondChain)
    expect(newest?.base?.keyId).toBe('invoices-2027')
    const stored = [...filesUnder(destination.root), ...filesUnder(stagingDir)]
    expect(filesHoldingText(stored, PLAINTEXT_MARKERS)).toEqual([])
    expect(filesHoldingKey(stored, key)).toEqual([])
    expect(filesHoldingKey(stored, newKey)).toEqual([])

    const oldKeyed = await restoreBackup({
      destination,
      driver: encryptingDriver(false),
      destPath: workspace.path('restored-old-key.db'),
      encryption: { key: () => Uint8Array.from(key) },
    }).catch(err => err)
    expect(oldKeyed).toMatchObject({ code: 'ENCRYPTION_KEY_REJECTED' })
    expect(String(oldKeyed.message)).toContain('invoices-2027')

    const restored = workspace.path('restored-new-key.db')
    await restoreBackup({
      destination,
      driver: encryptingDriver(false),
      destPath: restored,
      encryption: { key: () => Uint8Array.from(newKey) },
    })
    expectCiphertextCopy(restored, newKey, range(1, SEEDED_INVOICES + 10))
  })
})
