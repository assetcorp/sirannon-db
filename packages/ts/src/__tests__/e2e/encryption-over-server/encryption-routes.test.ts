import { randomBytes } from 'node:crypto'
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import StockDatabase from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { EncryptionKey } from '../../../core/encryption/types.js'
import type { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer } from '../../../server/server.js'
import { builtSirannonSqliteBindingPath, SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import { gate } from '../../integration/backups/harness.js'
import {
  errorCodeOf,
  errorMessageOf,
  postJson,
  type RawAnswer,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
  waitUntil,
} from '../lib/served-registry.js'

const binding = builtSirannonSqliteBindingPath()
const OPERATOR = { authenticate: () => ({ operator: true }) }
const SECRET_ROWS = 60
const secretOf = (n: number) => `payroll-entry-${n.toString().padStart(4, '0')}-confidential`

let dir: string

beforeEach(() => {
  dir = scratchDirectory('encryption-routes')
})

afterEach(releaseServedRegistries)

function encryptingRegistry(): Sirannon {
  return trackedRegistry({ driver: betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined }) })
}

async function openPayroll(sirannon: Sirannon, key?: Uint8Array): Promise<string> {
  const path = join(dir, 'payroll.db')
  const db = await sirannon.open(
    'payroll',
    path,
    key === undefined ? undefined : { encryption: { key: () => Uint8Array.from(key) } },
  )
  await db.execute('CREATE TABLE payroll (id INTEGER PRIMARY KEY, entry TEXT NOT NULL)')
  for (let n = 1; n <= SECRET_ROWS; n++) await db.execute('INSERT INTO payroll (entry) VALUES (?)', [secretOf(n)])
  return path
}

function fileHoldsPlaintext(path: string): boolean {
  for (const file of [path, `${path}-wal`]) {
    if (!existsSync(file)) continue
    const bytes = readFileSync(file)
    for (let n = 1; n <= SECRET_ROWS; n++) if (bytes.includes(Buffer.from(secretOf(n), 'utf8'))) return true
  }
  return false
}

function stockCount(path: string): number {
  const reader = new StockDatabase(path, { readonly: true })
  try {
    return (reader.prepare('SELECT count(*) AS n FROM payroll').get() as { n: number }).n
  } finally {
    reader.close()
  }
}

async function changeSettled(base: string): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 20_000
  for (;;) {
    const status = (await sendRaw(base, 'GET', '/db/payroll/encryption')).body as Record<string, unknown>
    const state = (status.change as { state?: string } | undefined)?.state
    if (state === 'done' || state === 'failed') return status
    if (Date.now() > deadline) throw new Error(`The key change did not settle: ${JSON.stringify(status)}`)
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

function codeAndStatus(answer: RawAnswer): [number, string | null] {
  return [answer.status, errorCodeOf(answer.body)]
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe('the encryption routes', () => {
  it('refuse to start the server with the key change route open but no authenticate hook or no encryptionKeys function', () => {
    const sirannon = trackedRegistry()
    const keys = () => randomBytes(32)
    const attempts = [
      { acceptEncryptionChange: true, encryptionKeys: keys },
      { acceptEncryptionChange: true, ...OPERATOR },
      { acceptEncryptionChange: true, ...OPERATOR, encryptionKeys: 'payroll-key' as unknown as typeof keys },
    ]
    for (const options of attempts) {
      let thrown: unknown
      try {
        createServer(sirannon, { port: 0, ...options })
      } catch (err) {
        thrown = err
      }
      expect((thrown as { code?: string }).code).toBe('INVALID_ENCRYPTION_CHANGE')
    }
  })

  it('keep the key change route shut until acceptEncryptionChange, before they check the body or the database', async () => {
    const sirannon = trackedRegistry()
    await openPayroll(sirannon)
    let resolved = 0
    const served = await serveRegistry(sirannon, {
      ...OPERATOR,
      encryptionKeys: () => {
        resolved++
        return randomBytes(32)
      },
    })
    for (const id of ['payroll', 'missing']) {
      for (const body of ['{"keyRef":"payroll-2026"}', '{}', 'not json', '']) {
        const answer = await sendRaw(served.base, 'POST', `/db/${id}/encryption`, body)
        expect(codeAndStatus(answer), `${id} ${body}`).toEqual([403, 'ENCRYPTION_CHANGE_NOT_ACCEPTED'])
      }
    }
    expect(resolved).toBe(0)
    expect(stockCount(join(dir, 'payroll.db'))).toBe(SECRET_ROWS)
  })

  it('answer every malformed keyRef with 400, an unknown database with 404, and a driver that cannot encrypt with 501', async () => {
    const sirannon = trackedRegistry()
    const path = await openPayroll(sirannon)
    let resolved = 0
    const served = await serveRegistry(sirannon, {
      ...OPERATOR,
      acceptEncryptionChange: true,
      encryptionKeys: () => {
        resolved++
        return randomBytes(32)
      },
    })
    for (const body of [{}, { keyRef: '' }, { keyRef: 'k'.repeat(257) }, { keyRef: 7 }, { keyRef: ['a'] }, []]) {
      const answer = await postJson(served.base, '/db/payroll/encryption', body)
      expect(codeAndStatus(answer), JSON.stringify(body)).toEqual([400, 'INVALID_REQUEST'])
    }
    expect(resolved).toBe(0)
    expect(codeAndStatus(await postJson(served.base, '/db/missing/encryption', { keyRef: 'payroll-2026' }))).toEqual([
      404,
      'DATABASE_NOT_FOUND',
    ])
    expect(codeAndStatus(await sendRaw(served.base, 'GET', '/db/missing/encryption'))).toEqual([
      404,
      'DATABASE_NOT_FOUND',
    ])
    expect(codeAndStatus(await postJson(served.base, '/db/payroll/encryption', { keyRef: 'k'.repeat(256) }))).toEqual([
      501,
      'ENCRYPTION_UNSUPPORTED',
    ])
    expect(stockCount(path)).toBe(SECRET_ROWS)
  })
})

describe.skipIf(binding === null)('a key change over the server', () => {
  it('resolves the keyRef on the server, encrypts while serving, and reports the key identifier with no key in any answer', async () => {
    const sirannon = encryptingRegistry()
    const path = await openPayroll(sirannon)
    const key = randomBytes(32)
    const requested: string[] = []
    const served = await serveRegistry(sirannon, {
      ...OPERATOR,
      acceptSql: true,
      acceptEncryptionChange: true,
      encryptionKeys: (databaseId, keyRef): EncryptionKey => {
        requested.push(`${databaseId}:${keyRef}`)
        return { key: Uint8Array.from(key), id: 'payroll-2026' }
      },
    })
    expect(fileHoldsPlaintext(path)).toBe(true)

    const accepted = await postJson(served.base, '/db/payroll/encryption', { keyRef: 'vault/payroll-2026' })
    const status = await changeSettled(served.base)
    const rows = await postJson(served.base, '/db/payroll/query', { sql: 'SELECT count(*) AS n FROM payroll' })
    await sirannon.close('payroll')

    expect([accepted.status, accepted.body]).toEqual([202, { started: true }])
    expect(status).toMatchObject({ encrypted: true, keyId: 'payroll-2026', change: { state: 'done' } })
    expect(JSON.stringify(status)).not.toContain(key.toString('hex'))
    expect(new Set(requested)).toEqual(new Set(['payroll:vault/payroll-2026']))
    expect(rows.body).toEqual({ rows: [{ n: SECRET_ROWS }] })
    expect(fileHoldsPlaintext(path)).toBe(false)
    expect(() => stockCount(path)).toThrow()
  })

  it('answers a key function that fails with 503 and a fixed message, a short key with 500, and a second change while one runs with 409', async () => {
    const sirannon = encryptingRegistry()
    const path = await openPayroll(sirannon)
    const held = gate()
    const key = randomBytes(32)
    let calls = 0
    const served = await serveRegistry(sirannon, {
      ...OPERATOR,
      acceptEncryptionChange: true,
      encryptionKeys: async (_databaseId, keyRef) => {
        if (keyRef === 'vault/offline') throw new Error('vault at /etc/vault/payroll.hcl is sealed')
        if (keyRef === 'vault/short') return randomBytes(16)
        calls++
        if (calls === 2) await held.promise
        return Uint8Array.from(key)
      },
    })

    const unavailable = await postJson(served.base, '/db/payroll/encryption', { keyRef: 'vault/offline' })
    const short = await postJson(served.base, '/db/payroll/encryption', { keyRef: 'vault/short' })
    const first = await postJson(served.base, '/db/payroll/encryption', { keyRef: 'vault/payroll' })
    await waitUntil(() => calls === 2, 'the key change resolved its target key')
    const second = await postJson(served.base, '/db/payroll/encryption', { keyRef: 'vault/payroll' })
    held.open()
    const status = await changeSettled(served.base)

    expect(codeAndStatus(unavailable)).toEqual([503, 'ENCRYPTION_KEY_UNAVAILABLE'])
    expect(unavailable.text).not.toContain('/etc/vault')
    expect((unavailable.body as { error: { details?: unknown } }).error.details).toBeUndefined()
    expect(codeAndStatus(short)).toEqual([500, 'INVALID_ENCRYPTION_KEY'])
    expect(first.status).toBe(202)
    expect(codeAndStatus(second)).toEqual([409, 'ENCRYPTION_CHANGE_IN_PROGRESS'])
    expect(errorMessageOf(second.body)).toBe('A key change is running on this database')
    expect(status).toMatchObject({ encrypted: true, change: { state: 'done' } })
    expect(fileHoldsPlaintext(path)).toBe(false)
  })

  it('removes encryption with a null keyRef, after which stock SQLite reads every row', async () => {
    const sirannon = encryptingRegistry()
    const key = randomBytes(32)
    const path = await openPayroll(sirannon, key)
    const served = await serveRegistry(sirannon, {
      ...OPERATOR,
      acceptEncryptionChange: true,
      encryptionKeys: () => randomBytes(32),
    })
    expect(fileHoldsPlaintext(path)).toBe(false)

    const accepted = await postJson(served.base, '/db/payroll/encryption', { keyRef: null })
    const status = await changeSettled(served.base)
    await sirannon.close('payroll')

    expect(accepted.status).toBe(202)
    expect(status).toMatchObject({ encrypted: false, change: { state: 'done', target: 'plaintext' } })
    expect(stockCount(path)).toBe(SECRET_ROWS)
  })
})
