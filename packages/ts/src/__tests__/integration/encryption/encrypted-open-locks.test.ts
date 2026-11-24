import { execFileSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { EncryptionOptions } from '../../../core/encryption/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'

const binding = builtSirannonSqliteBindingPath()

const EXCLUSIVE_LOCK_PROBE = `
const { DatabaseSync } = require('node:sqlite')
const db = new DatabaseSync(process.argv[1], { timeout: 0 })
let outcome = 'taken'
try {
  db.exec('PRAGMA locking_mode = EXCLUSIVE')
  db.prepare('SELECT count(*) FROM sqlite_schema').get()
} catch (err) {
  if (err.errcode === 5) outcome = 'refused'
}
try { db.close() } catch {}
process.stdout.write(outcome)
`

function exclusiveLockFromAnotherProcess(path: string): string {
  return execFileSync(process.execPath, ['-e', EXCLUSIVE_LOCK_PROBE, path], { encoding: 'utf8' })
}

describe.skipIf(binding === null || process.platform === 'win32')(
  'a database that the encrypting better-sqlite3 driver opens twice in one process',
  () => {
    let dir: string
    let sirannon: Sirannon

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'sirannon-encrypted-open-locks-'))
      sirannon = new Sirannon({ driver: betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined }) })
    })

    afterEach(async () => {
      await sirannon.shutdown().catch(() => undefined)
      rmSync(dir, { recursive: true, force: true })
    })

    const key = randomBytes(32)
    const keyed: { encryption: EncryptionOptions } = { encryption: { key: () => Uint8Array.from(key) } }

    it.each([
      { file: 'an encrypted file', options: keyed },
      { file: 'a plaintext file', options: {} },
    ])('keeps another process from locking $file exclusively', async ({ options }) => {
      const path = join(dir, 'orders.db')
      const first = await sirannon.open('orders', path, options)
      await first.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
      await first.execute('INSERT INTO orders (id, total) VALUES (1, 250)')
      expect(exclusiveLockFromAnotherProcess(path)).toBe('refused')

      const second = await sirannon.open('orders-again', path, options)
      expect(exclusiveLockFromAnotherProcess(path)).toBe('refused')

      await second.execute('INSERT INTO orders (id, total) VALUES (2, 400)')
      expect(await first.query('SELECT id, total FROM orders ORDER BY id')).toEqual([
        { id: 1, total: 250 },
        { id: 2, total: 400 },
      ])
    })
  },
)
