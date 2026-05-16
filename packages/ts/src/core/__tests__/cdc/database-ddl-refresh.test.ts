import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import type { ChangeEvent } from '../../types.js'
import { testDriver } from '../helpers/test-driver.js'

interface TrackerInternals {
  changeTracker: { watchedTables: ReadonlySet<string> } | null
}

const fixture: { tempDir: string; dbs: Database[] } = { tempDir: '', dbs: [] }

async function createDb(): Promise<Database> {
  const dbPath = join(fixture.tempDir, `ddl-${fixture.dbs.length}.db`)
  const db = await Database.create('ddl-refresh-test', dbPath, testDriver)
  fixture.dbs.push(db)
  await db.execute('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)')
  return db
}

async function collectEvents(db: Database, table: string, minCount: number): Promise<ChangeEvent[]> {
  const received: ChangeEvent[] = []
  const sub = db.on(table).subscribe(e => received.push(e))
  const start = Date.now()
  while (received.length < minCount && Date.now() - start < 2000) {
    await new Promise(r => setTimeout(r, 25))
  }
  sub.unsubscribe()
  return received
}

beforeEach(() => {
  fixture.tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cdc-ddl-'))
  fixture.dbs.length = 0
})

afterEach(async () => {
  for (const db of fixture.dbs) {
    try {
      if (!db.closed) await db.close()
    } catch {
      /* best-effort cleanup */
    }
  }
  fixture.dbs.length = 0
  rmSync(fixture.tempDir, { recursive: true, force: true })
})

describe('Database CDC ALTER/DROP refresh', () => {
  describe('ALTER TABLE via db.execute', () => {
    it('refreshes triggers so a subsequent INSERT captures the new column', async () => {
      const db = await createDb()
      await db.watch('foo')

      await db.execute('ALTER TABLE foo ADD COLUMN bar TEXT')
      await db.execute("INSERT INTO foo (id, name, bar) VALUES (1, 'alice', 'x')")

      const events = await collectEvents(db, 'foo', 1)
      expect(events).toHaveLength(1)
      expect(events[0].type).toBe('insert')
      const row = events[0].row as { id: number; name: string; bar: string }
      expect(row.bar).toBe('x')
      expect(row.name).toBe('alice')
    })
  })

  describe('ALTER TABLE via db.transaction', () => {
    it('refreshes inline so DML against the new column inside the same transaction captures it', async () => {
      const db = await createDb()
      await db.watch('foo')

      await db.transaction(async tx => {
        await tx.execute('ALTER TABLE foo ADD COLUMN bar TEXT')
        await tx.execute("INSERT INTO foo (id, name, bar) VALUES (1, 'alice', 'x')")
      })

      const events = await collectEvents(db, 'foo', 1)
      expect(events).toHaveLength(1)
      const row = events[0].row as { id: number; name: string; bar: string }
      expect(row.bar).toBe('x')
    })

    it('leaves the trigger column list unchanged when the transaction rolls back', async () => {
      const db = await createDb()
      await db.watch('foo')

      const intentional = new Error('rollback for test')
      await expect(
        db.transaction(async tx => {
          await tx.execute('ALTER TABLE foo ADD COLUMN bar TEXT')
          throw intentional
        }),
      ).rejects.toBe(intentional)

      await db.execute("INSERT INTO foo (id, name) VALUES (1, 'alice')")

      const events = await collectEvents(db, 'foo', 1)
      expect(events).toHaveLength(1)
      const row = events[0].row as Record<string, unknown>
      expect(Object.keys(row).sort()).toEqual(['id', 'name'])
      expect(row.name).toBe('alice')
    })
  })

  describe('DROP TABLE via db.execute', () => {
    it('prunes the watched entry', async () => {
      const db = await createDb()
      await db.watch('foo')
      const internals = db as unknown as TrackerInternals
      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(true)

      await db.execute('DROP TABLE foo')

      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(false)
    })
  })

  describe('DROP TABLE via db.transaction', () => {
    it('leaves the watched entry intact on rollback', async () => {
      const db = await createDb()
      await db.watch('foo')
      const internals = db as unknown as TrackerInternals
      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(true)

      const intentional = new Error('rollback drop')
      await expect(
        db.transaction(async tx => {
          await tx.execute('DROP TABLE foo')
          throw intentional
        }),
      ).rejects.toBe(intentional)

      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(true)

      await db.execute("INSERT INTO foo (id, name) VALUES (1, 'alice')")
      const events = await collectEvents(db, 'foo', 1)
      expect(events).toHaveLength(1)
      expect((events[0].row as { name: string }).name).toBe('alice')
    })

    it('prunes the watched entry on successful commit', async () => {
      const db = await createDb()
      await db.watch('foo')
      const internals = db as unknown as TrackerInternals
      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(true)

      await db.transaction(async tx => {
        await tx.execute('DROP TABLE foo')
      })

      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(false)
    })
  })

  describe('DDL on an unwatched table', () => {
    it('does not error and does not add or remove watched entries', async () => {
      const db = await createDb()
      await db.execute('CREATE TABLE other (id INTEGER PRIMARY KEY, v TEXT)')
      await db.watch('foo')

      await db.execute('ALTER TABLE other ADD COLUMN extra TEXT')
      await db.execute('DROP TABLE other')

      const internals = db as unknown as TrackerInternals
      expect(internals.changeTracker?.watchedTables.has('foo')).toBe(true)
      expect(internals.changeTracker?.watchedTables.size).toBe(1)
    })
  })

  describe('Non-DDL statement', () => {
    it('does not invoke the trigger refresh helper', async () => {
      const db = await createDb()
      await db.watch('foo')

      let refreshCount = 0
      const tracker = (db as unknown as { changeTracker: { refreshAllTriggersUsingConnection: unknown } }).changeTracker
      const original = tracker.refreshAllTriggersUsingConnection
      tracker.refreshAllTriggersUsingConnection = async (...args: unknown[]) => {
        refreshCount++
        return (original as (...a: unknown[]) => Promise<void>).apply(tracker, args)
      }

      await db.execute("INSERT INTO foo (id, name) VALUES (1, 'alice')")
      await db.execute("UPDATE foo SET name = 'bob' WHERE id = 1")
      await db.execute('DELETE FROM foo WHERE id = 1')

      expect(refreshCount).toBe(0)
    })
  })

  describe('Database without a tracker', () => {
    it('pays no extra cost on ALTER TABLE', async () => {
      const fresh = await createDb()
      await expect(fresh.execute('ALTER TABLE foo ADD COLUMN bar TEXT')).resolves.toBeTruthy()

      const internals = fresh as unknown as TrackerInternals
      expect(internals.changeTracker).toBeNull()
    })
  })

  describe('concurrent execute', () => {
    it('serialises DDL and DML through the writer pool with consistent end state', async () => {
      const db = await createDb()
      await db.watch('foo')

      await Promise.all([
        db.execute('ALTER TABLE foo ADD COLUMN bar TEXT'),
        (async () => {
          await new Promise(r => setTimeout(r, 5))
          await db.execute("INSERT INTO foo (id, name) VALUES (1, 'alice')")
        })(),
      ])

      const rows = await db.query<{ id: number; name: string; bar: string | null }>('SELECT * FROM foo ORDER BY id')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('alice')

      await db.execute("INSERT INTO foo (id, name, bar) VALUES (2, 'bob', 'b-val')")
      const events = await collectEvents(db, 'foo', 2)
      const bobEvent = events.find(e => (e.row as { id: number }).id === 2)
      expect(bobEvent).toBeDefined()
      const bobRow = bobEvent?.row as { bar: string }
      expect(bobRow.bar).toBe('b-val')
    })
  })

  describe('executeBatch with non-DDL', () => {
    it('does not invoke the DDL hook for DML batches', async () => {
      const db = await createDb()
      await db.watch('foo')

      let refreshCount = 0
      const tracker = (db as unknown as { changeTracker: { refreshAllTriggersUsingConnection: unknown } }).changeTracker
      const original = tracker.refreshAllTriggersUsingConnection
      tracker.refreshAllTriggersUsingConnection = async (...args: unknown[]) => {
        refreshCount++
        return (original as (...a: unknown[]) => Promise<void>).apply(tracker, args)
      }

      await db.executeBatch('INSERT INTO foo (id, name) VALUES (?, ?)', [
        [1, 'alice'],
        [2, 'bob'],
      ])

      expect(refreshCount).toBe(0)
    })
  })
})
