import { afterEach, describe, expect, it } from 'vitest'
import { HookDeniedError } from '../../../core/errors.js'
import type { SnapshotHookContext } from '../../../core/hooks/types.js'
import { singleConnectionDriver } from '../../helpers/fault-drivers/single-connection-driver.js'
import { canonicalForm, decodeWireRow, decodeWireValue, specChecksum, wireValue } from '../lib/device-sync-oracle.js'
import {
  errorCodeOf,
  readSqlite,
  releaseServedRegistries,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'
import {
  authenticateSession,
  type DeviceServer,
  type Identity,
  postDeviceRoute,
  SESSION_TOKEN,
  startDeviceServer,
} from './harness.js'

const SNAPSHOT_PAGE_BYTE_CAP = 8 * 1024 * 1024
const BOOKS = 25
const BASE_ISBN = 9_007_199_254_740_990n
const MEBIBYTE = 1024 * 1024
const SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER)

interface Page {
  rows: Record<string, unknown>[]
  checksum: string
  done: boolean
  nextKey: unknown[] | null
}

afterEach(releaseServedRegistries)

async function seedLibrary(server: DeviceServer): Promise<void> {
  await server.db.execute('CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)')
  await server.db.execute(
    'CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER REFERENCES authors(id), title TEXT, cover BLOB, isbn INTEGER)',
  )
  await server.db.watch('books')
  await server.db.execute("INSERT INTO authors (id, name) VALUES (1, 'Ada')")
  await server.db.executeBatch(
    'INSERT INTO books (id, author_id, title, cover, isbn) VALUES (?, ?, ?, ?, ?)',
    Array.from({ length: BOOKS }, (_, at) => [
      at + 1,
      1,
      `Book ${at + 1}`,
      new Uint8Array([at, 255 - at]),
      BASE_ISBN + BigInt(at),
    ]),
  )
}

function asWireNative(value: unknown): unknown {
  if (value instanceof Uint8Array) return new Uint8Array(value)
  if (typeof value === 'bigint' && value <= SAFE_INTEGER && value >= -SAFE_INTEGER) return Number(value)
  return value
}

function rawRows(path: string, table: string, order: string): Record<string, unknown>[] {
  return readSqlite<Record<string, unknown>>(path, `SELECT * FROM "${table}" ORDER BY ${order}`).map(row =>
    Object.fromEntries(Object.entries(row).map(([column, value]) => [column, asWireNative(value)])),
  )
}

function decodedPage(page: Page): Record<string, unknown>[] {
  return page.rows.map(decodeWireRow)
}

async function walkPages(base: string, table: string, limit?: number): Promise<Page[]> {
  const pages: Page[] = []
  let afterKey: unknown[] | undefined
  for (let round = 0; round < 1_000; round++) {
    const answer = await postDeviceRoute(base, 'snapshot/page', {
      table,
      ...(limit === undefined ? {} : { limit }),
      ...(afterKey === undefined ? {} : { afterKey }),
    })
    expect(answer.status, JSON.stringify(answer.body)).toBe(200)
    const page = answer.body as Page
    pages.push(page)
    if (page.done) return pages
    afterKey = page.nextKey ?? undefined
  }
  throw new Error(`the pages of ${table} never finished`)
}

describe('the snapshot manifest', () => {
  it('lists the schema in foreign-key order, the row counts, the start position, the epoch, and the migrations', async () => {
    const server = await startDeviceServer()
    await server.db.migrate([
      { version: 1, name: 'tags', up: 'CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)' },
    ])
    await seedLibrary(server)
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'kept')")

    const answer = await postDeviceRoute(server.base, 'snapshot', {})
    await server.db.execute("INSERT INTO books (id, title) VALUES (100, 'written during the copy')")
    const manifest = answer.body as {
      databaseId: string
      startSeq: string
      epoch: string
      schema: string[]
      tables: Array<{ name: string; rowCount: number }>
      migrations: Array<{ version: number; name: string; checksum: string | null }>
    }

    expect(answer.status).toBe(200)
    expect(manifest.databaseId).toBe('app')
    expect(manifest.epoch).toBe(
      readSqlite<{ value: string }>(server.path, "SELECT value FROM _sirannon_meta WHERE key = 'cdc_epoch'")[0].value,
    )
    const laterSeqs = readSqlite<{ seq: bigint }>(server.path, "SELECT seq FROM _sirannon_changes WHERE row_id = '100'")
    expect(BigInt(manifest.startSeq) < laterSeqs[0].seq).toBe(true)
    expect(BigInt(manifest.startSeq)).toBe(laterSeqs[0].seq - 1n)
    const position = (name: string) =>
      manifest.schema.findIndex(ddl => new RegExp(`CREATE TABLE "?${name}"?\\b`, 'i').test(ddl))
    expect(position('authors')).toBeGreaterThanOrEqual(0)
    expect(position('authors')).toBeLessThan(position('books'))
    expect(manifest.schema.some(ddl => /_sirannon|sqlite_/i.test(ddl))).toBe(false)
    expect(Object.fromEntries(manifest.tables.map(table => [table.name, table.rowCount]))).toEqual({
      authors: 1,
      books: BOOKS,
      notes: 1,
      tags: 0,
    })
    expect(manifest.migrations).toEqual(
      readSqlite<{ version: bigint; name: string; checksum: string | null }>(
        server.path,
        'SELECT version, name, checksum FROM _sirannon_migrations ORDER BY version',
      ).map(row => ({ version: Number(row.version), name: row.name, checksum: row.checksum })),
    )
  })

  it('refuses a database that does not exist and one that lives only in memory', async () => {
    const server = await startDeviceServer()
    const inMemory = trackedRegistry({ driver: singleConnectionDriver() })
    const scratch = await inMemory.open('scratch', ':memory:')
    await scratch.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY)')
    const memoryServer = await serveRegistry(inMemory, { acceptDeviceSync: true, authenticate: authenticateSession })

    const unknown = await fetch(`${server.base}/db/nowhere/snapshot`, {
      method: 'POST',
      headers: { authorization: `Bearer ${SESSION_TOKEN}`, 'content-type': 'application/json' },
      body: '{}',
    })
    const memory = await fetch(`${memoryServer.base}/db/scratch/snapshot/page`, {
      method: 'POST',
      headers: { authorization: `Bearer ${SESSION_TOKEN}`, 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'notes' }),
    })

    expect([unknown.status, errorCodeOf(await unknown.json())]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect([memory.status, errorCodeOf(await memory.json())]).toEqual([400, 'SNAPSHOT_UNSUPPORTED'])
  })
})

describe('snapshot pages', () => {
  it('pages every table by key at every limit, with values intact and checksums per the spec', async () => {
    const server = await startDeviceServer()
    await seedLibrary(server)
    await server.db.execute(
      'CREATE TABLE stock (shelf TEXT, slot INTEGER, quantity INTEGER, PRIMARY KEY (shelf, slot))',
    )
    await server.db.executeBatch(
      'INSERT INTO stock (shelf, slot, quantity) VALUES (?, ?, ?)',
      ['b', 'a', 'c'].flatMap(shelf => [3, 1, 2].map(slot => [shelf, slot, slot * 10])),
    )
    const expected = rawRows(server.path, 'books', 'id')

    for (const limit of [1, 10, 25, 1_000, undefined]) {
      const pages = await walkPages(server.base, 'books', limit)
      for (const page of pages) expect(page.checksum, `limit ${limit}`).toBe(specChecksum(page.rows.map(decodeWireRow)))
      expect(pages.flatMap(decodedPage), `limit ${limit}`).toEqual(expected)
      expect(pages.length, `limit ${limit}`).toBe(Math.max(1, Math.ceil(BOOKS / (limit ?? 500))))
      for (const page of pages.slice(0, -1)) {
        const last = page.rows[page.rows.length - 1]
        expect(page.nextKey).toEqual([last.id])
      }
      expect(pages[pages.length - 1].nextKey).toBeNull()
    }
    const stock = await walkPages(server.base, 'stock', 2)
    expect(stock.flatMap(decodedPage)).toEqual(rawRows(server.path, 'stock', 'shelf, slot'))
    expect(stock[0].nextKey).toEqual(['a', 2])
    expect(decodeWireValue(stock[0].rows[0].quantity)).toBe(10)
    expect((await walkPages(server.base, 'notes'))[0]).toEqual({ rows: [], checksum: '', done: true, nextKey: null })
  })

  it('keeps each page under the byte cap however large its rows, and still delivers every row', async () => {
    const server = await startDeviceServer({ server: { maxBodyBytes: 4 * MEBIBYTE } })
    await server.db.execute('CREATE TABLE scans (id INTEGER PRIMARY KEY, image BLOB)')
    for (let id = 1; id <= 12; id++) {
      await server.db.execute('INSERT INTO scans (id, image) VALUES (?, ?)', [id, new Uint8Array(MEBIBYTE).fill(id)])
    }

    const pages = await walkPages(server.base, 'scans', 12)

    expect(pages.length).toBeGreaterThan(1)
    for (const page of pages) {
      const decoded = page.rows.map(decodeWireRow)
      expect(page.checksum).toBe(specChecksum(decoded))
      expect(canonicalForm(decoded).length).toBeLessThanOrEqual(SNAPSHOT_PAGE_BYTE_CAP)
    }
    const images = pages.flatMap(page => page.rows.map(decodeWireRow))
    expect(images.map(row => row.id)).toEqual(Array.from({ length: 12 }, (_, at) => at + 1))
    expect(images.every(row => (row.image as Uint8Array).every(byte => byte === row.id))).toBe(true)
  })

  it('refuses internal tables, missing tables, malformed keys, and limits outside 1 to 1000', async () => {
    const server = await startDeviceServer()
    await seedLibrary(server)
    const refusals: Array<[unknown, number, string]> = [
      [{ table: '_sirannon_meta' }, 400, 'INVALID_REQUEST'],
      [{ table: '_sirannon_changes' }, 400, 'INVALID_REQUEST'],
      [{ table: 'sqlite_master' }, 400, 'INVALID_REQUEST'],
      [{ table: 'SQLITE_SCHEMA' }, 400, 'INVALID_REQUEST'],
      [{ table: 'books; DROP TABLE books' }, 400, 'INVALID_REQUEST'],
      [{}, 400, 'INVALID_REQUEST'],
      [[], 400, 'INVALID_REQUEST'],
      [{ table: 'nowhere' }, 404, 'TABLE_NOT_FOUND'],
      [{ table: 'books', afterKey: [1, 2] }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', afterKey: [] }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', afterKey: Array.from({ length: 17 }, (_, at) => at) }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', afterKey: 'id' }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', limit: 0 }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', limit: 1_001 }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', limit: -5 }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', limit: 2.5 }, 400, 'INVALID_REQUEST'],
      [{ table: 'books', limit: '10' }, 400, 'INVALID_REQUEST'],
    ]

    for (const [body, status, code] of refusals) {
      const answer = await postDeviceRoute(server.base, 'snapshot/page', body)
      expect([answer.status, errorCodeOf(answer.body)], JSON.stringify(body)).toEqual([status, code])
    }
    const resumed = await postDeviceRoute(server.base, 'snapshot/page', {
      table: 'books',
      afterKey: [wireValue(24n)],
      limit: 1_000,
    })
    expect(decodedPage(resumed.body as Page).map(row => row.id)).toEqual([25])
  })
})

describe('who may download a snapshot', () => {
  it('asks onBeforeSnapshot about every table with the caller, refuses the whole manifest for one denied table, and refuses its pages', async () => {
    const seen: SnapshotHookContext[] = []
    const server = await startDeviceServer({
      hooks: {
        onBeforeSnapshot: async context => {
          seen.push({ ...context })
          await new Promise(resolve => setTimeout(resolve, 5))
          if (context.table === 'ledger') throw new HookDeniedError('beforeSnapshot', 'the ledger is closed')
        },
      },
    })
    await server.db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)')
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'public')")

    const manifest = await postDeviceRoute(server.base, 'snapshot', {})
    const denied = await postDeviceRoute(server.base, 'snapshot/page', { table: 'ledger' })
    const allowed = await postDeviceRoute(server.base, 'snapshot/page', { table: 'notes' })

    expect([manifest.status, errorCodeOf(manifest.body)]).toEqual([403, 'HOOK_DENIED'])
    expect([denied.status, errorCodeOf(denied.body)]).toEqual([403, 'HOOK_DENIED'])
    expect([allowed.status, (allowed.body as Page).rows]).toEqual([200, [{ id: 1, body: 'public' }]])
    const identity: Identity = { userId: 'u_amara' }
    expect(seen.slice(0, 2)).toEqual([
      { databaseId: 'app', table: 'notes', identity },
      { databaseId: 'app', table: 'ledger', identity },
    ])
  })

  it('tells a refused caller nothing about which tables exist', async () => {
    const server = await startDeviceServer({
      hooks: {
        onBeforeSnapshot: () => {
          throw new HookDeniedError('beforeSnapshot', 'snapshots are closed')
        },
      },
    })
    await server.db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)')

    const known = await postDeviceRoute(server.base, 'snapshot/page', { table: 'ledger' })
    const unknown = await postDeviceRoute(server.base, 'snapshot/page', { table: 'invoices' })

    expect([known.status, errorCodeOf(known.body), known.text.includes('ledger')]).toEqual([403, 'HOOK_DENIED', false])
    expect([unknown.status, errorCodeOf(unknown.body), unknown.text.includes('invoices')]).toEqual([
      403,
      'HOOK_DENIED',
      false,
    ])
  })
})
