import { randomBytes } from 'node:crypto'
import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { Sirannon as Registry } from '../../../core/sirannon.js'
import { expoSqlcipherBuild } from '../../helpers/expo-modules.js'
import { readWithIndependentSqlcipher } from '../../helpers/independent-sqlcipher.js'
import { builtExpoSqlcipherBindingPath } from '../../helpers/sirannon-sqlite.js'

vi.mock('expo-sqlite', async () => (await import('../../helpers/expo-modules.js')).expoSqliteModule())
vi.mock('expo-file-system/legacy', async () =>
  (await import('../../helpers/expo-modules.js')).expoFileSystemLegacyModule(),
)

const CORE = '../../../../dist/core/index.mjs'
const DRIVER = '../../../../dist/driver/expo.mjs'
const { Sirannon } = (await import(CORE)) as { Sirannon: typeof Registry }
const { expoSqlite } = (await import(DRIVER)) as { expoSqlite: () => SQLiteDriver }
const binding = builtExpoSqlcipherBindingPath('58.0.15')
const directory = mkdtempSync(join(tmpdir(), 'sirannon-expo-package-'))
const key = randomBytes(32)
const nextKey = randomBytes(32)
const keyed = (bytes: Uint8Array) => ({ encryption: { key: () => Uint8Array.from(bytes) } })

afterAll(() => rmSync(directory, { recursive: true, force: true }))

describe.skipIf(binding === null)('the published Expo entry with database names relative to its directory', () => {
  let registry: Registry

  beforeEach(() => {
    expoSqlcipherBuild.binding = binding
    expoSqlcipherBuild.directory = directory
    registry = new Sirannon({ driver: expoSqlite() })
  })

  afterEach(async () => {
    await registry.shutdown()
    for (const file of readdirSync(directory)) rmSync(join(directory, file), { force: true })
  })

  it('preserves encryption refusal codes across the published entry points', async () => {
    const encrypted = await registry.open('encrypted', 'encrypted.db', keyed(key))
    await encrypted.execute('CREATE TABLE visits (note TEXT)')
    const plain = await registry.open('plain', 'plain.db')
    await plain.execute('CREATE TABLE visits (note TEXT)')
    await registry.close('encrypted')
    await registry.close('plain')
    await expect(registry.open('wrong', 'encrypted.db', keyed(nextKey))).rejects.toMatchObject({
      code: 'ENCRYPTION_KEY_REJECTED',
    })
    await expect(registry.open('missing', 'encrypted.db')).rejects.toMatchObject({ code: 'ENCRYPTION_KEY_MISSING' })
    await expect(registry.open('keyed-plain', 'plain.db', keyed(key))).rejects.toMatchObject({
      code: 'DATABASE_NOT_ENCRYPTED',
    })
  })

  it.each([false, true])('changes the key of a named database with encryption initially %s', async encrypted => {
    const name = `visits-${randomBytes(8).toString('hex')}.db`
    const db = await registry.open('visits', name, encrypted ? keyed(key) : undefined)
    await db.execute('CREATE TABLE visits (note TEXT)')
    await db.execute("INSERT INTO visits (rowid, note) VALUES (5000, 'retained visit')")
    await db.changeEncryption(keyed(nextKey).encryption)
    expect(await db.query('SELECT rowid, note FROM visits')).toEqual([{ rowid: 5000, note: 'retained visit' }])
    await registry.close('visits')
    expect(readWithIndependentSqlcipher(join(directory, name), nextKey, 'SELECT rowid, note FROM visits')).toEqual([
      { rowid: 5000, note: 'retained visit' },
    ])
    expect(readdirSync(directory).filter(file => file.includes('.sirannon-key-change'))).toEqual([])
  })

  it('rolls back every encrypted transaction and batch write when a later statement fails', async () => {
    const db = await registry.open('visits', 'visits.db', keyed(key))
    await db.execute('CREATE TABLE visits (id INTEGER PRIMARY KEY, note TEXT NOT NULL)')
    await expect(
      db.transaction(async tx => {
        await tx.execute("INSERT INTO visits VALUES (1, 'must roll back')")
        throw new Error('cancel transaction')
      }),
    ).rejects.toThrow('cancel transaction')
    await expect(
      db.executeBatch('INSERT INTO visits VALUES (?, ?)', [
        [2, 'must roll back'],
        [2, 'duplicate'],
      ]),
    ).rejects.toThrow()
    await registry.close('visits')
    expect(readWithIndependentSqlcipher(join(directory, 'visits.db'), key, 'SELECT * FROM visits')).toEqual([])
  })
})
