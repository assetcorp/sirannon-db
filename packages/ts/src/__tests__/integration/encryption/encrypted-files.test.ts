import { createHash, randomBytes } from 'node:crypto'
import {
  closeSync,
  existsSync,
  mkdtempSync,
  openSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { EncryptionOptions } from '../../../core/encryption/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { waSqlite } from '../../../drivers/wa-sqlite/index.js'
import {
  decryptAegisDatabase,
  readWithIndependentAegis,
  readWithMultipleCiphersAegis,
} from '../../helpers/independent-aegis.js'
import { filesHoldingText, readWithStockSqlite } from '../../helpers/independent-sqlcipher.js'
import { builtSirannonSqliteBindingPath, SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'

const PATIENTS = 600
const PLAINTEXT_MARKERS = ['diagnosis-', 'CREATE TABLE patients']
const PAGE_BYTES = 4096
const AEGIS_RESERVED_BYTES = 64

const binding = builtSirannonSqliteBindingPath()

interface Patient {
  id: number
  note: string
}

function noteOf(id: number): string {
  return `diagnosis-${id.toString().padStart(5, '0')}-${'r'.repeat(150)}`
}

function expectedPatients(count: number): Patient[] {
  return Array.from({ length: count }, (_, at) => ({ id: at + 1, note: noteOf(at + 1) }))
}

function encryptingDriver(): SQLiteDriver {
  return betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined })
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

function flipByte(path: string, offset: number): void {
  const fd = openSync(path, 'r+')
  try {
    const byte = Buffer.alloc(1)
    const current = readFileSync(path)[offset]
    if (current === undefined) throw new Error(`the file is shorter than ${offset} bytes`)
    byte[0] = current ^ 0x01
    writeSync(fd, byte, 0, 1, offset)
  } finally {
    closeSync(fd)
  }
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe.skipIf(binding === null)('an encrypted database file on the better-sqlite3 driver', () => {
  let dir: string
  let registries: Sirannon[]
  const key = randomBytes(32)

  const registry = (driver: SQLiteDriver = encryptingDriver()) => {
    const sirannon = new Sirannon({ driver })
    registries.push(sirannon)
    return sirannon
  }

  async function writePatients(path: string, options: { encryption?: EncryptionOptions }) {
    const sirannon = registry()
    const db = await sirannon.open('patients', path, options)
    await db.execute('CREATE TABLE patients (id INTEGER PRIMARY KEY, note TEXT NOT NULL)')
    await db.executeBatch(
      'INSERT INTO patients (id, note) VALUES (?, ?)',
      expectedPatients(PATIENTS).map(patient => [patient.id, patient.note]),
    )
    return { sirannon, db }
  }

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-encrypted-files-'))
    registries = []
  })

  afterEach(async () => {
    for (const sirannon of registries) await sirannon.shutdown().catch(() => undefined)
    rmSync(dir, { recursive: true, force: true })
  })

  it('keeps the file and its write-ahead log as ciphertext that only the same key opens', async () => {
    const path = join(dir, 'patients.db')
    const plainPath = join(dir, 'plain.db')
    const { sirannon } = await writePatients(path, withKey(key))
    const { sirannon: plainSirannon } = await writePatients(plainPath, {})

    expect(statSync(`${path}-wal`).size).toBeGreaterThan(PATIENTS * 150)
    expect(filesHoldingText([plainPath, `${plainPath}-wal`], PLAINTEXT_MARKERS)).not.toEqual([])
    expect(filesHoldingText([path, `${path}-wal`], PLAINTEXT_MARKERS)).toEqual([])
    await sirannon.close('patients')
    await plainSirannon.close('patients')

    expect(filesHoldingText([path, `${path}-wal`], PLAINTEXT_MARKERS)).toEqual([])
    expect(() => readWithStockSqlite(path, 'SELECT count(*) FROM patients')).toThrow()
    expect(() => readWithIndependentAegis(path, randomBytes(32), 'SELECT count(*) FROM patients')).toThrow()
    const image = decryptAegisDatabase(path, key)
    expect(image.subarray(0, 16).toString('latin1')).toBe('SQLite format 3\0')
    expect(image.readUInt16BE(16)).toBe(PAGE_BYTES)
    expect(image[20]).toBe(AEGIS_RESERVED_BYTES)
    expect(image.indexOf(noteOf(PATIENTS), PAGE_BYTES)).toBeGreaterThan(PAGE_BYTES)
    expect(readWithIndependentAegis<Patient>(path, key, 'SELECT id, note FROM patients ORDER BY id')).toEqual(
      expectedPatients(PATIENTS),
    )
    expect(readWithMultipleCiphersAegis<Patient>(path, key, 'SELECT id, note FROM patients ORDER BY id')).toEqual(
      expectedPatients(PATIENTS),
    )
    expect(readWithStockSqlite<Patient>(plainPath, 'SELECT id, note FROM patients ORDER BY id')).toEqual(
      expectedPatients(PATIENTS),
    )
  })

  it.each([
    { file: 'a missing file', create: (_: string) => undefined },
    { file: 'a zero-byte file', create: (path: string) => writeFileSync(path, '') },
  ])('creates an encrypted database from $file with a key, and a plaintext one without', async ({ create }) => {
    const encryptedPath = join(dir, 'encrypted.db')
    const plainPath = join(dir, 'plain.db')
    create(encryptedPath)
    create(plainPath)
    const encrypted = await writePatients(encryptedPath, withKey(key))
    const plain = await writePatients(plainPath, {})
    await encrypted.sirannon.close('patients')
    await plain.sirannon.close('patients')

    expect(filesHoldingText([encryptedPath], PLAINTEXT_MARKERS)).toEqual([])
    expect(() => readWithStockSqlite(encryptedPath, 'SELECT count(*) FROM patients')).toThrow()
    expect(readWithIndependentAegis<Patient>(encryptedPath, key, 'SELECT id, note FROM patients ORDER BY id')).toEqual(
      expectedPatients(PATIENTS),
    )
    expect(readWithStockSqlite<Patient>(plainPath, 'SELECT id, note FROM patients ORDER BY id')).toEqual(
      expectedPatients(PATIENTS),
    )
  })

  it('refuses a wrong key, a missing key, and a key on a plaintext file by name, and changes no byte of either file', async () => {
    const path = join(dir, 'patients.db')
    const plainPath = join(dir, 'plain.db')
    await (await writePatients(path, withKey(key))).sirannon.close('patients')
    await (await writePatients(plainPath, {})).sirannon.close('patients')
    const before = fingerprints(dir)

    const sirannon = registry()
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
    await expect(sirannon.open('uri-wrong', `file:${path}`, withKey(randomBytes(32)))).rejects.toMatchObject({
      code: 'ENCRYPTION_KEY_REJECTED',
    })
    await expect(sirannon.open('uri-missing', `file:${path}?cache=private`, { walMode: false })).rejects.toMatchObject({
      code: 'ENCRYPTION_KEY_MISSING',
    })
    await expect(sirannon.open('uri-keyed-plain', `file:${plainPath}`, withKey(key))).rejects.toMatchObject({
      code: 'DATABASE_NOT_ENCRYPTED',
    })
    await expect(sirannon.open('uri-memory', 'file:patients?mode=memory', withKey(key))).rejects.toMatchObject({
      code: 'ENCRYPTION_UNSUPPORTED',
    })
    expect(fingerprints(dir)).toEqual(before)
  })

  it.each([
    { given: 'a 16-byte key', key: () => randomBytes(16), code: 'INVALID_ENCRYPTION_KEY' },
    { given: 'a 33-byte key', key: () => randomBytes(33), code: 'INVALID_ENCRYPTION_KEY' },
    { given: 'an empty key', key: () => new Uint8Array(0), code: 'INVALID_ENCRYPTION_KEY' },
    { given: 'a password', key: () => 'correct horse battery staple', code: 'INVALID_ENCRYPTION_KEY' },
    { given: 'a 64-character hex string', key: () => randomBytes(32).toString('hex'), code: 'INVALID_ENCRYPTION_KEY' },
    { given: 'an object holding a password', key: () => ({ key: 'hunter2', id: 'k' }), code: 'INVALID_ENCRYPTION_KEY' },
    { given: 'a plain array of 32 numbers', key: () => Array.from(randomBytes(32)), code: 'INVALID_ENCRYPTION_KEY' },
    {
      given: 'a function that throws',
      key: () => {
        throw new Error('the key service is down')
      },
      code: 'ENCRYPTION_KEY_UNAVAILABLE',
    },
    {
      given: 'a function that rejects',
      key: () => Promise.reject(new Error('the keychain is locked')),
      code: 'ENCRYPTION_KEY_UNAVAILABLE',
    },
  ])('refuses $given before it creates any file', async ({ key: badKey, code }) => {
    const path = join(dir, 'never.db')
    const opened = registry().open('never', path, { encryption: { key: badKey as EncryptionOptions['key'] } })
    await expect(opened).rejects.toMatchObject({ code })
    expect(readdirSync(dir)).toEqual([])
  })

  it.each([
    { driver: 'node:sqlite', build: () => nodeSqlite() },
    { driver: 'wa-sqlite', build: () => waSqlite() },
    { driver: 'better-sqlite3 created without encryption', build: () => betterSqlite3() },
  ])('refuses the encryption option on $driver with ENCRYPTION_UNSUPPORTED and creates no plaintext file', async ({
    build,
  }) => {
    const path = join(dir, 'never.db')
    await expect(registry(build()).open('never', path, withKey(key))).rejects.toMatchObject({
      code: 'ENCRYPTION_UNSUPPORTED',
    })
    expect(existsSync(path)).toBe(false)
  })

  it('reports a flipped byte in a table page as ENCRYPTED_PAGE_CORRUPT and returns no rows', async () => {
    const path = join(dir, 'patients.db')
    await (await writePatients(path, withKey(key))).sirannon.close('patients')
    const pages = statSync(path).size / PAGE_BYTES
    expect(pages).toBeGreaterThan(10)
    flipByte(path, (pages - 1) * PAGE_BYTES + 1000)

    const db = await registry().open('flipped', path, withKey(key))
    await expect(db.query('SELECT id, note FROM patients')).rejects.toMatchObject({ code: 'ENCRYPTED_PAGE_CORRUPT' })
    await expect(db.query('SELECT count(*) AS n FROM patients')).rejects.toMatchObject({
      code: 'ENCRYPTED_PAGE_CORRUPT',
    })
  })

  it('refuses to open a file with a flipped byte in its first page, by name', async () => {
    const path = join(dir, 'patients.db')
    await (await writePatients(path, withKey(key))).sirannon.close('patients')
    flipByte(path, 200)

    const refused = await registry()
      .open('flipped', path, withKey(key))
      .catch(err => err)
    expect(['ENCRYPTION_KEY_REJECTED', 'ENCRYPTED_PAGE_CORRUPT']).toContain(refused.code)
  })
})
