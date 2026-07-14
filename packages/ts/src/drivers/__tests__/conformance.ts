import { describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../core/driver/types.js'

export function runConformanceTests(driverFactory: () => SQLiteDriver, label: string) {
  describe(`${label} driver conformance`, () => {
    it('opens and closes a connection', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.close()
    })

    it('exec() runs DDL statements', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
      await conn.close()
    })

    it('prepare/all returns rows', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      await conn.exec("INSERT INTO items (name) VALUES ('alpha'), ('beta')")

      const stmt = await conn.prepare('SELECT * FROM items ORDER BY id')
      const rows = await stmt.all<{ id: number; name: string }>()
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('alpha')
      expect(rows[1].name).toBe('beta')
      await conn.close()
    })

    it('prepare/get returns a single row or undefined', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      await conn.exec("INSERT INTO items (name) VALUES ('alpha')")

      const stmt = await conn.prepare('SELECT * FROM items WHERE id = ?')
      const row = await stmt.get<{ id: number; name: string }>(1)
      expect(row).toBeDefined()
      expect(row?.name).toBe('alpha')

      const missing = await stmt.get<{ id: number; name: string }>(999)
      expect(missing).toBeUndefined()
      await conn.close()
    })

    it('prepare/run returns changes and lastInsertRowId', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const insertStmt = await conn.prepare('INSERT INTO items (name) VALUES (?)')
      const result = await insertStmt.run('gamma')
      expect(result.changes).toBe(1)
      expect(Number(result.lastInsertRowId)).toBeGreaterThan(0)
      await conn.close()
    })

    it('transactions commit on success', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      await conn.transaction(async txConn => {
        const stmt = await txConn.prepare('INSERT INTO items (name) VALUES (?)')
        await stmt.run('committed')
      })

      const stmt = await conn.prepare('SELECT COUNT(*) as cnt FROM items')
      const row = await stmt.get<{ cnt: number }>()
      expect(row?.cnt).toBe(1)
      await conn.close()
    })

    it('transactions rollback on error', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      await expect(
        conn.transaction(async txConn => {
          const stmt = await txConn.prepare('INSERT INTO items (name) VALUES (?)')
          await stmt.run('should-rollback')
          throw new Error('forced rollback')
        }),
      ).rejects.toThrow('forced rollback')

      const stmt = await conn.prepare('SELECT COUNT(*) as cnt FROM items')
      const row = await stmt.get<{ cnt: number }>()
      expect(row?.cnt).toBe(0)
      await conn.close()
    })

    it('handles various parameter types', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec(
        'CREATE TABLE mixed (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, text_val TEXT, null_val TEXT)',
      )

      const insertStmt = await conn.prepare(
        'INSERT INTO mixed (int_val, real_val, text_val, null_val) VALUES (?, ?, ?, ?)',
      )
      await insertStmt.run(42, 3.14, 'hello', null)

      const selectStmt = await conn.prepare('SELECT * FROM mixed WHERE id = 1')
      const row = await selectStmt.get<{
        int_val: number
        real_val: number
        text_val: string
        null_val: string | null
      }>()
      expect(row?.int_val).toBe(42)
      expect(row?.real_val).toBeCloseTo(3.14)
      expect(row?.text_val).toBe('hello')
      expect(row?.null_val).toBeNull()
      await conn.close()
    })

    it('concurrent operations do not interfere', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER DEFAULT 0)')
      await conn.exec('INSERT INTO counters (val) VALUES (0)')

      const reads = []
      for (let i = 0; i < 5; i++) {
        const stmt = await conn.prepare('SELECT val FROM counters WHERE id = 1')
        reads.push(stmt.get<{ val: number }>())
      }

      const results = await Promise.all(reads)
      for (const r of results) {
        expect(r?.val).toBe(0)
      }
      await conn.close()
    })

    it('reports correct capabilities', () => {
      const driver = driverFactory()
      expect(driver.capabilities).toBeDefined()
      expect(typeof driver.capabilities.multipleConnections).toBe('boolean')
      expect(typeof driver.capabilities.extensions).toBe('boolean')
    })

    it('reads integers beyond 2^53 - 1 back as exact BigInt values', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER)')

      const values = [9007199254740992n, 9007199254740993n, 9223372036854775807n, -9223372036854775808n]
      const insertStmt = await conn.prepare('INSERT INTO ledgers (id, balance) VALUES (?, ?)')
      for (let i = 0; i < values.length; i++) {
        await insertStmt.run(i + 1, values[i])
      }

      const selectStmt = await conn.prepare('SELECT balance FROM ledgers ORDER BY id')
      const rows = await selectStmt.all<{ balance: unknown }>()
      expect(rows.map(r => r.balance)).toEqual(values)
      for (const row of rows) {
        expect(typeof row.balance).toBe('bigint')
      }
      await conn.close()
    })

    it('reads integers within the safe range back as plain numbers', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE ledgers (id INTEGER PRIMARY KEY, balance INTEGER)')

      const values = [0, 1, -1, 9007199254740991, -9007199254740991]
      const insertStmt = await conn.prepare('INSERT INTO ledgers (id, balance) VALUES (?, ?)')
      for (let i = 0; i < values.length; i++) {
        await insertStmt.run(i + 1, values[i])
      }

      const selectStmt = await conn.prepare('SELECT balance FROM ledgers ORDER BY id')
      const rows = await selectStmt.all<{ balance: unknown }>()
      expect(rows.map(r => r.balance)).toEqual(values)
      for (const row of rows) {
        expect(typeof row.balance).toBe('number')
      }
      await conn.close()
    })

    it('round-trips BLOB values exactly', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB)')

      const payload = Buffer.from([0x00, 0x01, 0x7f, 0x80, 0xff])
      const insertStmt = await conn.prepare('INSERT INTO files (id, payload) VALUES (?, ?)')
      await insertStmt.run(1, payload)

      const selectStmt = await conn.prepare('SELECT payload FROM files WHERE id = 1')
      const row = await selectStmt.get<{ payload: Uint8Array }>()
      expect(row).toBeDefined()
      expect(Buffer.compare(Buffer.from(row?.payload ?? []), payload)).toBe(0)
      await conn.close()
    })

    it('reports lastInsertRowId beyond 2^53 - 1 as exact BigInt', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT)')

      const bigRowId = 9223372036854775806n
      const insertStmt = await conn.prepare('INSERT INTO events (id, note) VALUES (?, ?)')
      const result = await insertStmt.run(bigRowId, 'boundary')
      expect(result.lastInsertRowId).toBe(bigRowId)
      await conn.close()
    })

    it('reports lastInsertRowId within the safe range as a plain number', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT)')

      const insertStmt = await conn.prepare('INSERT INTO events (note) VALUES (?)')
      const result = await insertStmt.run('first')
      expect(result.lastInsertRowId).toBe(1)
      expect(typeof result.lastInsertRowId).toBe('number')
      await conn.close()
    })

    it('runBatch reports lastInsertRowId beyond 2^53 - 1 as exact BigInt', async () => {
      const driver = driverFactory()
      const conn = await driver.open(':memory:')
      await conn.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT)')
      if (!conn.runBatch) {
        await conn.close()
        return
      }

      const bigRowId = 9223372036854775806n
      const results = await conn.runBatch('INSERT INTO events (id, note) VALUES (?, ?)', [
        [bigRowId, 'boundary'],
        [1, 'safe'],
      ])
      expect(results.map(r => r.lastInsertRowId)).toEqual([bigRowId, 1])
      await conn.close()
    })
  })
}
