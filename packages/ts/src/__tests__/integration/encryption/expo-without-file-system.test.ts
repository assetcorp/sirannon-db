import { randomBytes } from 'node:crypto'
import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { expoSqlcipherBuild } from '../../helpers/expo-modules.js'
import { readWithIndependentSqlcipher, readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'
import { builtExpoSqlcipherBindings } from '../../helpers/sirannon-sqlite.js'

vi.mock('expo-sqlite', async () => (await import('../../helpers/expo-modules.js')).expoSqliteModule())
vi.mock('expo-file-system/legacy', () => {
  throw new Error("Unable to resolve module 'expo-file-system/legacy'")
})

const EXPO_DRIVER_MODULE = '../../../drivers/expo/index.js'
const { expoSqlite } = (await import(EXPO_DRIVER_MODULE)) as { expoSqlite: () => SQLiteDriver }
const [oldest] = builtExpoSqlcipherBindings()

interface Visit {
  id: number
  note: string
}

const VISITS: Visit[] = Array.from({ length: 50 }, (_, at) => ({ id: at + 1, note: `visit-note-${at + 1}` }))

describe.skipIf(oldest?.binding == null)('the Expo driver in an app without expo-file-system 19.0.0', () => {
  let dir: string
  let sirannon: Sirannon

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-expo-no-files-'))
    expoSqlcipherBuild.binding = oldest?.binding ?? null
    sirannon = new Sirannon({ driver: expoSqlite() })
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  async function writeVisits(name: string, path: string, key?: Uint8Array) {
    const db = await sirannon.open(
      name,
      path,
      key === undefined ? {} : { encryption: { key: () => Uint8Array.from(key) } },
    )
    await db.execute('CREATE TABLE visits (id INTEGER PRIMARY KEY, note TEXT NOT NULL)')
    await db.executeBatch(
      'INSERT INTO visits (id, note) VALUES (?, ?)',
      VISITS.map(visit => [visit.id, visit.note]),
    )
    return db
  }

  it('opens plaintext and encrypted databases, and refuses a key change by name before it writes any file', async () => {
    const plainPath = join(dir, 'plain.db')
    await writeVisits('plain', plainPath)
    await sirannon.close('plain')
    const reopened = await sirannon.open('plain', plainPath)
    expect(await reopened.query<Visit>('SELECT id, note FROM visits ORDER BY id')).toEqual(VISITS)
    await sirannon.close('plain')

    const key = randomBytes(32)
    const path = join(dir, 'encrypted.db')
    const db = await writeVisits('encrypted', path, key)
    const refused = await db.changeEncryption({ key: () => Uint8Array.from(randomBytes(32)) }).catch(err => err)
    expect(refused).toMatchObject({ code: 'ENCRYPTION_UNSUPPORTED' })
    expect(String(refused.message)).toContain('expo-file-system 19.0.0')
    await db.execute('INSERT INTO visits (id, note) VALUES (?, ?)', [51, 'visit-note-51'])
    await sirannon.close('encrypted')

    expect(readdirSync(dir).filter(name => name.includes('sirannon-key-change'))).toEqual([])
    expect(readWithIndependentSqlcipher<Visit>(path, key, 'SELECT id, note FROM visits ORDER BY id')).toEqual([
      ...VISITS,
      { id: 51, note: 'visit-note-51' },
    ])
    expect(readWithStockSqlite<Visit>(plainPath, 'SELECT id, note FROM visits ORDER BY id')).toEqual(VISITS)
  })
})
