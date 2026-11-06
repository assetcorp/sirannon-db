import { spawnSync } from 'node:child_process'
import { createHash, randomBytes } from 'node:crypto'
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { restoreBackup } from '../../../backup/index.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { readWithIndependentAegis } from '../../helpers/independent-aegis.js'
import { filesHoldingText, readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import {
  KILL_POINTS,
  KILLED_KEY_CHANGE_ENV,
  type KillPoint,
  ledgerBackups,
  NOT_KILLED,
  SEEDED_ENTRIES,
  secretOf,
} from './killed-key-change.js'

const KILLED_KEY_CHANGE_SCRIPT = join(dirname(fileURLToPath(import.meta.url)), 'killed-key-change.ts')
const PACKAGE_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..', '..')
const POINTS_AFTER_THE_NEW_FILE_REPLACED_THE_ORIGINAL: readonly KillPoint[] = [
  'between-delete-and-rename',
  'renamed',
  'marker-flushed',
  'marker-removed',
]
const POINTS_WHERE_THE_MARKER_EXPLAINS_THE_KEY: readonly KillPoint[] = [
  'marker-created',
  'first-round',
  'final-round',
  'replacement-begun',
  'checkpoint',
  'wal-removed',
  'between-delete-and-rename',
  'renamed',
  'marker-flushed',
]
const ENTRY_WRITTEN_AFTER_RECOVERY = 'after-recovery'

const binding = builtSirannonSqliteBindingPath()

interface LedgerRow {
  id: number
  secret: string
}

function killDuring(dir: string, point: KillPoint, oldKey: Uint8Array, newKey: Uint8Array, backupsDir?: string) {
  const path = join(dir, 'ledger.db')
  const log = join(dir, 'committed.log')
  const keys = `${Buffer.from(oldKey).toString('hex')}:${Buffer.from(newKey).toString('hex')}`
  const child = spawnSync(
    process.execPath,
    [
      '--import',
      'tsx',
      KILLED_KEY_CHANGE_SCRIPT,
      path,
      log,
      binding ?? '',
      point,
      keys,
      ...(backupsDir === undefined ? [] : [backupsDir]),
    ],
    {
      cwd: PACKAGE_ROOT,
      encoding: 'utf8',
      env: { ...process.env, [KILLED_KEY_CHANGE_ENV]: '1' },
      timeout: 60_000,
    },
  )
  expect(child.stdout, child.stderr).not.toContain(NOT_KILLED)
  expect(child.signal, child.stderr).toBe('SIGKILL')
  const committed = readFileSync(log, 'utf8')
    .split('\n')
    .filter(line => line !== '')
    .map(Number)
  return { path, committed }
}

function keyChangeLeftovers(dir: string): string[] {
  return readdirSync(dir).filter(name => name.includes('.sirannon-key-change'))
}

function fingerprints(dir: string): Record<string, string> {
  const result: Record<string, string> = {}
  for (const name of readdirSync(dir).sort()) {
    result[name] = createHash('sha256')
      .update(readFileSync(join(dir, name)))
      .digest('hex')
  }
  return result
}

function openWith(sirannon: Sirannon, path: string, key: Uint8Array) {
  return sirannon.open('ledger', path, { encryption: { key: () => Uint8Array.from(key) } })
}

function expectEveryCommittedEntry(rows: readonly LedgerRow[], committed: readonly number[]): void {
  const byId = new Map(rows.map(row => [Number(row.id), row.secret]))
  for (const id of committed) expect(byId.get(id), `committed entry ${id}`).toBe(secretOf(id))
  for (const [id, secret] of byId)
    expect([secretOf(id), 'pending', ENTRY_WRITTEN_AFTER_RECOVERY], `entry ${id}`).toContain(secret)
}

describe.skipIf(binding === null)('a key change killed part way on the better-sqlite3 driver', () => {
  let dir: string
  let sirannon: Sirannon
  const oldKey = randomBytes(32)
  const newKey = randomBytes(32)

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-key-change-kill-'))
    sirannon = new Sirannon({ driver: betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined }) })
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  it.each(KILL_POINTS)('keeps every committed entry under the right key when killed at %s', async point => {
    const { path, committed } = killDuring(dir, point, oldKey, newKey)
    const replaced = POINTS_AFTER_THE_NEW_FILE_REPLACED_THE_ORIGINAL.includes(point)
    const expectedKey = replaced ? newKey : oldKey
    const otherKey = replaced ? oldKey : newKey
    expect(committed.length).toBeGreaterThanOrEqual(SEEDED_ENTRIES)

    const refused = await openWith(sirannon, path, otherKey).catch(err => err)
    expect(refused).toMatchObject({ code: 'ENCRYPTION_KEY_REJECTED' })
    if (POINTS_WHERE_THE_MARKER_EXPLAINS_THE_KEY.includes(point)) {
      expect(String(refused.message)).toContain(
        replaced ? 'the key from after the change' : 'the key from before the change',
      )
    }
    expect(keyChangeLeftovers(dir)).toEqual([])

    const db = await openWith(sirannon, path, expectedKey)
    expectEveryCommittedEntry(await db.query<LedgerRow>('SELECT id, secret FROM ledger'), committed)
    const next = Number(
      (await db.execute('INSERT INTO ledger (secret) VALUES (?)', [ENTRY_WRITTEN_AFTER_RECOVERY])).lastInsertRowId,
    )
    expect(next).toBeGreaterThan(Math.max(...committed))
    await sirannon.close('ledger')

    expect(keyChangeLeftovers(dir)).toEqual([])
    const secrets = committed.map(secretOf)
    expect(filesHoldingText([path, `${path}-wal`], secrets)).toEqual([])
    expect(() => readWithStockSqlite(path, 'SELECT count(*) FROM ledger')).toThrow()
    const independent = readWithIndependentAegis<LedgerRow>(path, expectedKey, 'SELECT id, secret FROM ledger')
    expectEveryCommittedEntry(independent, committed)
  }, 90_000)

  it.each([
    'renamed',
    'marker-flushed',
  ] as const)('restores every committed entry from a new chain under the new key when killed at %s with backups on', async point => {
    const backupsDir = join(dir, 'backups')
    const { path, committed } = killDuring(dir, point, oldKey, newKey, backupsDir)
    const errors: Error[] = []
    const db = await sirannon.open('ledger', path, {
      encryption: { key: () => Uint8Array.from(newKey) },
      backups: { ...ledgerBackups(backupsDir), onError: err => errors.push(err) },
    })
    await db.execute('INSERT INTO ledger (secret) VALUES (?)', [ENTRY_WRITTEN_AFTER_RECOVERY])
    await db.captureBackupChanges()
    await db.captureBackupChanges()
    await sirannon.close('ledger')
    expect(errors).toEqual([])

    const restored = join(dir, 'restored.db')
    await restoreBackup({
      destination: ledgerBackups(backupsDir).destination,
      driver: betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined }),
      destPath: restored,
      encryption: { key: () => Uint8Array.from(newKey) },
    })
    const rows = readWithIndependentAegis<LedgerRow>(restored, newKey, 'SELECT id, secret FROM ledger')
    expectEveryCommittedEntry(rows, committed)
    expect(rows.map(row => row.secret)).toContain(ENTRY_WRITTEN_AFTER_RECOVERY)
  }, 90_000)

  it('fails to open and touches no file when the database file is missing before the replacement began', async () => {
    const { path } = killDuring(dir, 'marker-created', oldKey, newKey)
    rmSync(path)
    expect(existsSync(`${path}.sirannon-key-change-marker`)).toBe(true)
    const before = fingerprints(dir)

    const refused = await openWith(sirannon, path, oldKey).catch(err => err)
    expect(refused).toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    const second = await openWith(sirannon, path, newKey).catch(err => err)
    expect(second).toMatchObject({ code: 'DATABASE_OPEN_FAILED' })
    expect(fingerprints(dir)).toEqual(before)
  }, 90_000)
})
