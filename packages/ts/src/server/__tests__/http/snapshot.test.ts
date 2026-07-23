import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { decodeTaggedValues } from '../../../core/cdc/encoding.js'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { canonicaliseForChecksum } from '../../../core/sync/canonicalise.js'
import { sha256Hex } from '../../../core/sync/sha256.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server.js'
import { createServer } from '../../server.js'
import type { SnapshotManifestResponse, SnapshotPageResponse } from '../../snapshot-protocol.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let db: Database

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-snapshot-'))
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('snapdb', join(tempDir, 'snap.db'))
  await db.execute('CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)')
  await db.execute(
    'CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER REFERENCES authors(id), title TEXT, cover BLOB, isbn INTEGER)',
  )
  await db.watch('books')
  await db.execute("INSERT INTO authors (id, name) VALUES (1, 'Ada')")
  for (let i = 1; i <= 25; i++) {
    await db.execute('INSERT INTO books (id, author_id, title, cover, isbn) VALUES (?, ?, ?, ?, ?)', [
      i,
      1,
      `Book ${i}`,
      new Uint8Array([i, i + 1]),
      9007199254740990n + BigInt(i),
    ])
  }
  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

async function post<T>(path: string, body: unknown): Promise<{ status: number; data: T }> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: response.status, data: (await response.json()) as T }
}

describe('POST /db/:id/snapshot', () => {
  it('returns schema in FK order, table counts, a start seq, and an epoch', async () => {
    const { status, data } = await post<SnapshotManifestResponse>('/db/snapdb/snapshot', {})
    expect(status).toBe(200)
    expect(data.databaseId).toBe('snapdb')
    expect(/^\d+$/.test(data.startSeq)).toBe(true)
    expect(BigInt(data.startSeq) > 0n).toBe(true)
    expect(data.epoch).toMatch(/^[0-9a-f]{32}$/)
    expect(data.schema.some(ddl => /CREATE TABLE authors/i.test(ddl))).toBe(true)
    expect(data.schema.findIndex(ddl => /authors/.test(ddl))).toBeLessThan(
      data.schema.findIndex(ddl => /books/.test(ddl)),
    )
    expect(data.schema.every(ddl => !ddl.includes('_sirannon'))).toBe(true)
    expect(data.tables).toEqual([
      { name: 'authors', rowCount: 1 },
      { name: 'books', rowCount: 25 },
    ])
  })

  it('rejects unknown databases', async () => {
    const { status } = await post('/db/missing/snapshot', {})
    expect(status).toBe(404)
  })
})

describe('POST /db/:id/snapshot/page', () => {
  it('pages with a resume key and verifiable checksums, preserving blobs and big integers', async () => {
    const seen: Record<string, unknown>[] = []
    let afterKey: unknown[] | undefined
    let pages = 0
    for (;;) {
      const { status, data } = await post<SnapshotPageResponse>('/db/snapdb/snapshot/page', {
        table: 'books',
        limit: 10,
        ...(afterKey !== undefined ? { afterKey } : {}),
      })
      expect(status).toBe(200)
      pages += 1
      const rows = decodeTaggedValues(data.rows) as Record<string, unknown>[]
      if (rows.length > 0) {
        expect(sha256Hex(canonicaliseForChecksum(rows))).toBe(data.checksum)
        seen.push(...rows)
      }
      if (data.done) break
      expect(data.nextKey).not.toBeNull()
      if (data.nextKey === null) break
      afterKey = data.nextKey
    }

    expect(pages).toBe(3)
    expect(seen).toHaveLength(25)
    expect(seen[0].title).toBe('Book 1')
    expect(seen[24].isbn).toBe(9007199254740990n + 25n)
    const cover = seen[4].cover as Uint8Array
    expect(Array.from(cover)).toEqual([5, 6])
  })

  it('returns an empty final page for empty tables', async () => {
    await db.execute('CREATE TABLE empty_one (id INTEGER PRIMARY KEY)')
    const { status, data } = await post<SnapshotPageResponse>('/db/snapdb/snapshot/page', { table: 'empty_one' })
    expect(status).toBe(200)
    expect(data).toEqual({ rows: [], checksum: '', done: true, nextKey: null })
  })

  it('rejects internal tables, unknown tables, and malformed resume keys', async () => {
    expect((await post('/db/snapdb/snapshot/page', { table: '_sirannon_meta' })).status).toBe(400)
    expect((await post('/db/snapdb/snapshot/page', { table: 'sqlite_master' })).status).toBe(400)
    expect((await post('/db/snapdb/snapshot/page', { table: 'nope' })).status).toBe(404)
    expect((await post('/db/snapdb/snapshot/page', { table: 'books', afterKey: [1, 2] })).status).toBe(400)
    expect((await post('/db/snapdb/snapshot/page', { table: 'books', limit: 0 })).status).toBe(400)
    expect((await post('/db/snapdb/snapshot/page', { table: 'books', limit: 100000 })).status).toBe(400)
  })
})
