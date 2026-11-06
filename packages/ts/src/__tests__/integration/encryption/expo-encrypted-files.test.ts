import { createHash, randomBytes } from 'node:crypto'
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { EncryptionOptions } from '../../../core/encryption/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { expoSqlcipherBuild } from '../../helpers/expo-modules.js'
import {
  filesHoldingText,
  readWithIndependentSqlcipher,
  readWithStockSqlite,
} from '../../helpers/independent-sqlcipher.js'
import {
  builtExpoSqlcipherBindings,
  builtSirannonSqliteBindingPath,
  EXPO_SQLCIPHER_REQUIRED,
  stockBetterSqliteBindingPath,
} from '../../helpers/sirannon-sqlite.js'

vi.mock('expo-sqlite', async () => (await import('../../helpers/expo-modules.js')).expoSqliteModule())
vi.mock('expo-file-system/legacy', async () =>
  (await import('../../helpers/expo-modules.js')).expoFileSystemLegacyModule(),
)

const EXPO_DRIVER_MODULE = '../../../drivers/expo/index.js'
const { expoSqlite } = (await import(EXPO_DRIVER_MODULE)) as { expoSqlite: () => SQLiteDriver }
const builds = builtExpoSqlcipherBindings()
const aegisBinding = builtSirannonSqliteBindingPath()
const VISITS = 300
const PLAINTEXT_MARKERS = ['visit-note-', 'CREATE TABLE visits']

interface Visit {
  id: number
  note: string
}

function expectedVisits(): Visit[] {
  return Array.from({ length: VISITS }, (_, at) => ({ id: at + 1, note: `visit-note-${at + 1}-${'v'.repeat(100)}` }))
}

function withKey(key: Uint8Array): { encryption: EncryptionOptions } {
  return { encryption: { key: () => Uint8Array.from(key) } }
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

describe('the SQLCipher builds that expo-sqlite vendors', () => {
  it.skipIf(!EXPO_SQLCIPHER_REQUIRED)('are built for this runner', () => {
    expect(builds.filter(build => build.binding === null)).toEqual([])
  })
})

describe('the Expo driver on an expo-sqlite without SQLCipher', () => {
  let dir: string
  let sirannon: Sirannon

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-expo-plain-'))
    expoSqlcipherBuild.binding = stockBetterSqliteBindingPath()
    sirannon = new Sirannon({ driver: expoSqlite() })
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  it('refuses a key with ENCRYPTION_UNSUPPORTED that names the expo-sqlite release it needs, and writes no table', async () => {
    const path = join(dir, 'visits.db')
    const refused = await sirannon.open('visits', path, withKey(randomBytes(32))).catch(err => err)
    expect(refused).toMatchObject({ code: 'ENCRYPTION_UNSUPPORTED' })
    expect(String(refused.message)).toContain('expo-sqlite 15.0.0')
    expect(readWithStockSqlite(path, 'SELECT name FROM sqlite_schema')).toEqual([])
  })
})

for (const { version, binding } of builds) {
  describe.skipIf(binding === null)(`an encrypted database file on the Expo driver over expo-sqlite ${version}`, () => {
    let dir: string
    let sirannon: Sirannon
    const key = randomBytes(32)

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'sirannon-expo-files-'))
      expoSqlcipherBuild.binding = binding
      sirannon = new Sirannon({ driver: expoSqlite() })
    })

    afterEach(async () => {
      await sirannon.shutdown()
      rmSync(dir, { recursive: true, force: true })
    })

    async function writeVisits(name: string, path: string, options: { encryption?: EncryptionOptions }) {
      const db = await sirannon.open(name, path, options)
      await db.execute('CREATE TABLE visits (id INTEGER PRIMARY KEY, note TEXT NOT NULL)')
      await db.executeBatch(
        'INSERT INTO visits (id, note) VALUES (?, ?)',
        expectedVisits().map(visit => [visit.id, visit.note]),
      )
      await sirannon.close(name)
    }

    it.each([
      { file: 'a missing file', create: (_: string) => undefined },
      { file: 'a zero-byte file', create: (path: string) => writeFileSync(path, '') },
    ])('creates an encrypted database from $file with a key, and a plaintext one without', async ({ create }) => {
      const encryptedPath = join(dir, 'encrypted.db')
      const plainPath = join(dir, 'plain.db')
      create(encryptedPath)
      create(plainPath)
      await writeVisits('encrypted', encryptedPath, withKey(key))
      await writeVisits('plain', plainPath, {})

      expect(filesHoldingText([encryptedPath, `${encryptedPath}-wal`], PLAINTEXT_MARKERS)).toEqual([])
      expect(() => readWithStockSqlite(encryptedPath, 'SELECT count(*) FROM visits')).toThrow()
      expect(
        readWithIndependentSqlcipher<Visit>(encryptedPath, key, 'SELECT id, note FROM visits ORDER BY id'),
      ).toEqual(expectedVisits())
      expect(readWithStockSqlite<Visit>(plainPath, 'SELECT id, note FROM visits ORDER BY id')).toEqual(expectedVisits())
    })

    it('refuses a wrong key, a missing key, and a key on a plaintext file by name, and changes no byte of either file', async () => {
      const path = join(dir, 'encrypted.db')
      const plainPath = join(dir, 'plain.db')
      await writeVisits('encrypted', path, withKey(key))
      await writeVisits('plain', plainPath, {})
      const before = fingerprints(dir)

      await expect(sirannon.open('wrong', path, withKey(randomBytes(32)))).rejects.toMatchObject({
        code: 'ENCRYPTION_KEY_REJECTED',
      })
      await expect(sirannon.open('missing', path)).rejects.toMatchObject({ code: 'ENCRYPTION_KEY_MISSING' })
      await expect(sirannon.open('missing-unlogged', path, { walMode: false })).rejects.toMatchObject({
        code: 'ENCRYPTION_KEY_MISSING',
      })
      await expect(sirannon.open('keyed-plain', plainPath, withKey(key))).rejects.toMatchObject({
        code: 'DATABASE_NOT_ENCRYPTED',
      })
      expect(fingerprints(dir)).toEqual(before)
    })

    it.skipIf(aegisBinding === null)(
      'refuses a file from the other encryption scheme by name in both directions, and changes no byte of either file',
      async () => {
        const expoPath = join(dir, 'expo.db')
        const nodePath = join(dir, 'node.db')
        await writeVisits('expo', expoPath, withKey(key))
        const nodeRegistry = new Sirannon({
          driver: betterSqlite3({ encryption: true, sqliteBindingPath: aegisBinding ?? undefined }),
        })
        try {
          const nodeDb = await nodeRegistry.open('node', nodePath, withKey(key))
          await nodeDb.execute('CREATE TABLE visits (id INTEGER PRIMARY KEY, note TEXT NOT NULL)')
          await nodeDb.execute('INSERT INTO visits (id, note) VALUES (1, ?)', ['visit-note-1'])
          await nodeRegistry.close('node')
          const before = fingerprints(dir)

          await expect(nodeRegistry.open('expo-file', expoPath, withKey(key))).rejects.toMatchObject({
            code: 'ENCRYPTION_KEY_REJECTED',
          })
          await expect(sirannon.open('node-file', nodePath, withKey(key))).rejects.toMatchObject({
            code: 'ENCRYPTION_KEY_REJECTED',
          })
          expect(fingerprints(dir)).toEqual(before)
        } finally {
          await nodeRegistry.shutdown()
        }
      },
    )
  })
}
