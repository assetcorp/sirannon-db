import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../cdc/change-tracker.js'
import { decodeTaggedValues } from '../../cdc/encoding.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { createTestDb, insertUser } from './_helpers.js'

describe('ChangeTracker', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker()
  })

  afterEach(async () => {
    await conn.close()
  })

  describe('null and special values', () => {
    it('handles null column values in JSON snapshots', async () => {
      await tracker.watch(conn, 'users')
      await insertUser(conn, 'Alice', null, null)

      const events = await tracker.poll(conn)
      expect(events[0].row).toEqual({
        id: 1,
        name: 'Alice',
        email: null,
        age: null,
      })
    })

    it('encodes BLOB columns as tagged hex envelope in the CDC JSON', async () => {
      await conn.exec('CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB)')
      await tracker.watch(conn, 'files')

      const payload = Buffer.from([0x00, 0x01, 0xff, 0xab, 0xcd, 0xef])
      const stmt = await conn.prepare('INSERT INTO files (id, payload) VALUES (?, ?)')
      await stmt.run(1, payload)

      const rowStmt = await conn.prepare("SELECT new_data FROM _sirannon_changes WHERE table_name = 'files'")
      const row = (await rowStmt.get()) as { new_data: string }
      const parsed = JSON.parse(row.new_data) as { id: number; payload: { __sirannon_blob: string } }
      expect(parsed.id).toBe(1)
      expect(parsed.payload).toEqual({ __sirannon_blob: '0001FFABCDEF' })
    })

    it('emits null for null BLOB columns without the tagged envelope', async () => {
      await conn.exec('CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB)')
      await tracker.watch(conn, 'files')

      const stmt = await conn.prepare('INSERT INTO files (id, payload) VALUES (?, ?)')
      await stmt.run(1, null)

      const rowStmt = await conn.prepare("SELECT new_data FROM _sirannon_changes WHERE table_name = 'files'")
      const row = (await rowStmt.get()) as { new_data: string }
      const parsed = JSON.parse(row.new_data) as { id: number; payload: unknown }
      expect(parsed.payload).toBeNull()
    })

    it('leaves non-BLOB columns alongside BLOBs unchanged in the CDC JSON', async () => {
      await conn.exec('CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, payload BLOB)')
      await tracker.watch(conn, 'files')

      const payload = Buffer.from('hello-blob', 'utf8')
      const stmt = await conn.prepare('INSERT INTO files (id, name, payload) VALUES (?, ?, ?)')
      await stmt.run(7, 'note.bin', payload)

      const rowStmt = await conn.prepare("SELECT new_data FROM _sirannon_changes WHERE table_name = 'files'")
      const row = (await rowStmt.get()) as { new_data: string }
      const parsed = JSON.parse(row.new_data) as { id: number; name: string; payload: { __sirannon_blob: string } }
      expect(parsed.id).toBe(7)
      expect(parsed.name).toBe('note.bin')
      expect(Buffer.from(parsed.payload.__sirannon_blob, 'hex').toString('utf8')).toBe('hello-blob')
    })

    it('wraps integers above 2^53-1 in the __sirannon_int tagged envelope', async () => {
      await conn.exec('CREATE TABLE counters (id INTEGER PRIMARY KEY, big INTEGER)')
      await tracker.watch(conn, 'counters')

      const big = 9007199254740993n
      const stmt = await conn.prepare('INSERT INTO counters (id, big) VALUES (?, ?)')
      await stmt.run(1, big)

      const rowStmt = await conn.prepare("SELECT new_data FROM _sirannon_changes WHERE table_name = 'counters'")
      const row = (await rowStmt.get()) as { new_data: string }
      const parsed = JSON.parse(row.new_data) as { id: number; big: { __sirannon_int: string } }
      expect(parsed.id).toBe(1)
      expect(parsed.big).toEqual({ __sirannon_int: '9007199254740993' })
    })

    it('wraps negative integers below -(2^53-1) in the __sirannon_int tagged envelope', async () => {
      await conn.exec('CREATE TABLE counters (id INTEGER PRIMARY KEY, big INTEGER)')
      await tracker.watch(conn, 'counters')

      const big = -9007199254740993n
      const stmt = await conn.prepare('INSERT INTO counters (id, big) VALUES (?, ?)')
      await stmt.run(1, big)

      const rowStmt = await conn.prepare("SELECT new_data FROM _sirannon_changes WHERE table_name = 'counters'")
      const row = (await rowStmt.get()) as { new_data: string }
      const parsed = JSON.parse(row.new_data) as { big: { __sirannon_int: string } }
      expect(parsed.big).toEqual({ __sirannon_int: '-9007199254740993' })
    })

    it('keeps safe-range integers as plain JSON numbers', async () => {
      await conn.exec('CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER)')
      await tracker.watch(conn, 'counters')

      const stmt = await conn.prepare('INSERT INTO counters (id, val) VALUES (?, ?)')
      await stmt.run(1, 9007199254740991n)
      await stmt.run(2, 0)
      await stmt.run(3, -9007199254740991n)

      const rowStmt = await conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'counters' ORDER BY seq",
      )
      const rows = (await rowStmt.all()) as Array<{ new_data: string }>
      const parsedValues = rows.map(r => (JSON.parse(r.new_data) as { val: unknown }).val)
      expect(parsedValues).toEqual([9007199254740991, 0, -9007199254740991])
    })

    it('decodes __sirannon_int back to BigInt in poll() events', async () => {
      await conn.exec('CREATE TABLE counters (id INTEGER PRIMARY KEY, big INTEGER)')
      await tracker.watch(conn, 'counters')

      const big = 9223372036854775807n
      const stmt = await conn.prepare('INSERT INTO counters (id, big) VALUES (?, ?)')
      await stmt.run(1, big)

      const events = await tracker.poll(conn)
      expect(events).toHaveLength(1)
      const decoded = events[0].row.big
      expect(typeof decoded).toBe('bigint')
      expect(decoded).toBe(big)
    })

    it('decodes tagged BLOB values without a Node Buffer global', () => {
      const globalWithBuffer = globalThis as unknown as { Buffer: typeof Buffer | undefined }
      const originalBuffer = globalWithBuffer.Buffer

      try {
        globalWithBuffer.Buffer = undefined
        const decoded = decodeTaggedValues({ payload: { __sirannon_blob: '0001FFABCDEF' } }) as {
          payload: Uint8Array
        }

        expect(decoded.payload).toBeInstanceOf(Uint8Array)
        expect(Array.from(decoded.payload)).toEqual([0x00, 0x01, 0xff, 0xab, 0xcd, 0xef])
      } finally {
        globalWithBuffer.Buffer = originalBuffer
      }
    })
  })
})
