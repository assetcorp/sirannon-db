import { randomBytes } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { EncryptionKey } from '../../../core/encryption/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { readWithIndependentAegis } from '../../helpers/independent-aegis.js'
import { readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'

const binding = builtSirannonSqliteBindingPath()
const CONSOLE_METHODS = ['log', 'info', 'warn', 'error', 'debug', 'trace'] as const

function encryptingDriver() {
  return betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined })
}

function keyForms(key: Uint8Array): string[] {
  const bytes = Buffer.from(key)
  return [
    bytes.toString('hex'),
    bytes.toString('hex').toUpperCase(),
    bytes.toString('base64'),
    bytes.toString('base64url'),
    bytes.toString('latin1'),
    Array.from(bytes).join(','),
    Array.from(bytes).join(', '),
  ]
}

function textOf(value: unknown, seen = new Set<unknown>()): string {
  if (value === null || value === undefined) return String(value)
  if (typeof value === 'bigint') return value.toString()
  if (typeof value !== 'object') return String(value)
  if (seen.has(value)) return ''
  seen.add(value)
  if (value instanceof Uint8Array) return Buffer.from(value).toString('latin1')
  const parts: string[] = []
  if (value instanceof Error)
    parts.push(value.message, value.stack ?? '', textOf((value as { cause?: unknown }).cause, seen))
  for (const name of Object.getOwnPropertyNames(value)) {
    parts.push(name, textOf((value as Record<string, unknown>)[name], seen))
  }
  return parts.join('\n')
}

function leakedKeys(captured: readonly unknown[], keys: readonly Uint8Array[]): string[] {
  const text = captured.map(value => textOf(value)).join('\n')
  return keys.flatMap(key => keyForms(key).filter(form => text.includes(form)))
}

describe.skipIf(binding === null)('the key of an encrypted database', () => {
  let dir: string
  let sirannon: Sirannon
  const captured: unknown[] = []

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-key-secrecy-'))
    captured.length = 0
    for (const method of CONSOLE_METHODS) {
      vi.spyOn(console, method).mockImplementation((...args: unknown[]) => {
        captured.push(...args)
      })
    }
    vi.spyOn(process.stdout, 'write').mockImplementation(chunk => {
      captured.push(chunk)
      return true
    })
    vi.spyOn(process.stderr, 'write').mockImplementation(chunk => {
      captured.push(chunk)
      return true
    })
    sirannon = new Sirannon({
      driver: encryptingDriver(),
      metrics: {
        onQueryComplete: metrics => captured.push(metrics),
        onConnectionOpen: metrics => captured.push(metrics),
        onConnectionClose: metrics => captured.push(metrics),
        onCDCEvent: metrics => captured.push(metrics),
      },
      hooks: {
        onBeforeQuery: context => {
          captured.push(context)
        },
        onAfterQuery: context => {
          captured.push(context)
        },
      },
    })
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  async function attempt(work: () => Promise<unknown>): Promise<void> {
    try {
      captured.push(await work())
    } catch (err) {
      captured.push(err)
    }
  }

  it('appears in no error, log line, metric, hook, or status across opens, failures, and key changes', async () => {
    const firstKey = randomBytes(32)
    const secondKey = randomBytes(32)
    const wrongKey = randomBytes(32)
    const path = join(dir, 'accounts.db')
    const keyed = (bytes: Uint8Array, id: string) => () => ({ key: Uint8Array.from(bytes), id })

    const db = await sirannon.open('accounts', path, { encryption: { key: keyed(firstKey, 'accounts-1') } })
    await db.execute('CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL)')
    await db.watch('accounts')
    db.on('accounts').subscribe(event => {
      captured.push(event)
    })
    for (let n = 0; n < 20; n++) await db.execute('INSERT INTO accounts (holder) VALUES (?)', [`holder-${n}`])
    await attempt(() => db.encryptionStatus())
    await attempt(() =>
      new Sirannon({ driver: encryptingDriver() }).open('wrong', path, { encryption: { key: keyed(wrongKey, 'x') } }),
    )
    await attempt(() => db.changeEncryption({ key: () => wrongKey.subarray(0, 31) as EncryptionKey }))
    await attempt(() => db.encryptionStatus())
    await attempt(() =>
      db.changeEncryption({
        key: () => {
          throw new Error('the key service timed out')
        },
      }),
    )
    await attempt(() => db.changeEncryption({ key: keyed(secondKey, 'accounts-2') }))
    await attempt(() =>
      db
        .changeEncryption({ key: keyed(secondKey, 'accounts-2') })
        .then(() => db.changeEncryption({ key: keyed(wrongKey, 'x') })),
    )
    await attempt(() => db.encryptionStatus())
    await attempt(() => db.query('SELECT * FROM accounts'))
    await sirannon.close('accounts')
    await attempt(() => sirannon.open('accounts', path, { encryption: { key: keyed(firstKey, 'accounts-1') } }))

    expect(captured.length).toBeGreaterThan(40)
    expect(leakedKeys(captured, [firstKey, secondKey, wrongKey])).toEqual([])
  })

  it('stores the key identifier and the time of the last change in the database, where a reopen reads them', async () => {
    const firstKey = randomBytes(32)
    const secondKey = randomBytes(32)
    const path = join(dir, 'ledger.db')
    const db = await sirannon.open('ledger', path, {
      encryption: { key: () => ({ key: Uint8Array.from(firstKey), id: 'ledger-2026' }) },
    })
    await db.execute('CREATE TABLE entries (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)')
    await db.execute('INSERT INTO entries (amount) VALUES (125)')
    const opened = await db.encryptionStatus()
    expect(opened).toMatchObject({ encrypted: true, keyId: 'ledger-2026', change: { state: 'idle' } })
    expect(opened.cipher).toBe('aegis-256')
    expect(opened.cipherVersion).toMatch(/^SQLite3 Multiple Ciphers 2\./)
    expect(opened.changedAt).toBeUndefined()

    const before = Date.now()
    const changed = await db.changeEncryption({ key: () => ({ key: Uint8Array.from(secondKey), id: 'ledger-2027' }) })
    const after = Date.now()
    expect(changed).toMatchObject({
      encrypted: true,
      keyId: 'ledger-2027',
      change: { state: 'done', target: 'encrypted' },
    })
    expect(changed.changedAt).toBeGreaterThanOrEqual(before)
    expect(changed.changedAt).toBeLessThanOrEqual(after)
    await sirannon.close('ledger')

    const stored = readWithIndependentAegis<{ key: string; value: string }>(
      path,
      secondKey,
      "SELECT key, value FROM _sirannon_meta WHERE key LIKE 'encryption_%' ORDER BY key",
    )
    expect(stored).toEqual([
      { key: 'encryption_changed_at', value: String(changed.changedAt) },
      { key: 'encryption_key_id', value: 'ledger-2027' },
    ])
    const reopened = await sirannon.open('ledger', path, {
      encryption: { key: () => ({ key: Uint8Array.from(secondKey), id: 'ledger-2027' }) },
    })
    expect(await reopened.encryptionStatus()).toMatchObject({
      encrypted: true,
      keyId: 'ledger-2027',
      changedAt: changed.changedAt,
    })
  })

  it('removes encryption so that stock SQLite reads every row, and encrypts the plaintext file again', async () => {
    const key = randomBytes(32)
    const path = join(dir, 'orders.db')
    const db: Database = await sirannon.open('orders', path, { encryption: { key: () => Uint8Array.from(key) } })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)')
    for (let n = 1; n <= 50; n++) await db.execute('INSERT INTO orders (id, item) VALUES (?, ?)', [n, `item-${n}`])

    expect(await db.changeEncryption({ key: null })).toMatchObject({
      encrypted: false,
      change: { state: 'done', target: 'plaintext' },
    })
    await db.execute('INSERT INTO orders (id, item) VALUES (51, ?)', ['item-51'])
    await sirannon.close('orders')
    const plain = readWithStockSqlite<{ id: number; item: string }>(path, 'SELECT id, item FROM orders ORDER BY id')
    expect(plain).toEqual(Array.from({ length: 51 }, (_, at) => ({ id: at + 1, item: `item-${at + 1}` })))
    await expect(
      sirannon.open('orders', path, { encryption: { key: () => Uint8Array.from(key) } }),
    ).rejects.toMatchObject({
      code: 'DATABASE_NOT_ENCRYPTED',
    })

    const reopened = await sirannon.open('orders', path)
    const newKey = randomBytes(32)
    expect(await reopened.changeEncryption({ key: () => Uint8Array.from(newKey) })).toMatchObject({ encrypted: true })
    await sirannon.close('orders')
    expect(() => readWithStockSqlite(path, 'SELECT count(*) FROM orders')).toThrow()
    expect(readWithIndependentAegis<{ n: number }>(path, newKey, 'SELECT count(*) AS n FROM orders')).toEqual([
      { n: 51 },
    ])
  })
})
