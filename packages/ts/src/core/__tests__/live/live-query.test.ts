import { afterEach, describe, expect, it } from 'vitest'
import { isRevalidating, type LiveHarness, openHarness, readyRows, waitForRows, waitForState } from './_helpers.js'

const SCHEMA = `CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  qty INTEGER NOT NULL,
  bucket TEXT NOT NULL
)`

interface Closeable {
  close(): Promise<void>
}

let harness: LiveHarness | undefined
let open: Closeable | undefined

afterEach(async () => {
  await open?.close().catch(() => {})
  open = undefined
  await harness?.dispose()
  harness = undefined
})

function bulkInserts(count: number, firstId: number, label: string): { sql: string; params: unknown[] }[] {
  return Array.from({ length: count }, (_, index) => ({
    sql: 'INSERT INTO items VALUES (?, ?, ?, ?)',
    params: [firstId + index, `${label}-${index}`, index, 'a'],
  }))
}

describe('Database.live', () => {
  it('starts ready with the rows the statement already matches', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'nut', 4, 'a'), (2, 'bolt', 9, 'a')")

    const live = await harness.db.live<{ id: number; name: string }>(
      'SELECT id, name FROM items WHERE bucket = ? ORDER BY id',
      ['a'],
    )
    open = live

    expect(readyRows(live)).toEqual([
      { id: 1, name: 'nut' },
      { id: 2, name: 'bolt' },
    ])
  })

  it('tells an empty result apart from one that has not read yet', async () => {
    harness = await openHarness(SCHEMA)
    const live = await harness.db.live<{ id: number }>("SELECT id FROM items WHERE bucket = 'missing' ORDER BY id")
    open = live

    expect(live.getState().status).toBe('ready')
    expect(readyRows(live)).toEqual([])
  })

  it('applies an insert into the ordered position rather than appending it', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a'), (3, 'cc', 3, 'a')")

    const live = await harness.db.live<{ name: string }>('SELECT name FROM items ORDER BY name')
    open = live
    await harness.db.execute("INSERT INTO items VALUES (2, 'bb', 2, 'a')")

    const rows = await waitForRows(live, current => current.length === 3)
    expect(rows.map(row => row.name)).toEqual(['aa', 'bb', 'cc'])
  })

  it('carries a row into and back out of the statement filter', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'nut', 4, 'a'), (2, 'bolt', 9, 'b')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items WHERE bucket = ? ORDER BY id', ['a'])
    open = live
    expect(readyRows(live)).toEqual([{ id: 1 }])

    await harness.db.execute("UPDATE items SET bucket = 'a' WHERE id = 2")
    await waitForRows(live, rows => rows.length === 2)

    await harness.db.execute("UPDATE items SET bucket = 'c' WHERE id = 1")
    expect(await waitForRows(live, rows => rows.length === 1)).toEqual([{ id: 2 }])
  })

  it('removes a deleted row and ignores a change to a row it never matched', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'nut', 4, 'a'), (2, 'bolt', 9, 'b')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items WHERE bucket = ? ORDER BY id', ['a'])
    open = live
    let notifications = 0
    live.subscribe(() => notifications++)

    await harness.db.execute('UPDATE items SET qty = 100 WHERE id = 2')
    await new Promise(resolve => setTimeout(resolve, 120))
    expect(notifications).toBe(0)

    await harness.db.execute('DELETE FROM items WHERE id = 1')
    expect(await waitForRows(live, rows => rows.length === 0)).toEqual([])
  })

  it('moves a row when the value it is ordered by changes', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a'), (2, 'bb', 2, 'a'), (3, 'cc', 3, 'a')")

    const live = await harness.db.live<{ id: number; qty: number }>('SELECT id, qty FROM items ORDER BY qty')
    open = live
    await harness.db.execute('UPDATE items SET qty = 99 WHERE id = 1')

    const rows = await waitForRows(live, current => current[current.length - 1]?.id === 1)
    expect(rows.map(row => row.id)).toEqual([2, 3, 1])
  })

  it('applies a whole transaction at once and never publishes a half-applied state', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a'), (2, 'bb', 2, 'a'), (3, 'cc', 3, 'a')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id')
    open = live
    const sizes: number[] = []
    live.subscribe(() => {
      const state = live.getState()
      if (state.status === 'ready' && !state.revalidating) sizes.push(state.rows.length)
    })

    await harness.db.transaction(async tx => {
      await tx.execute("INSERT INTO items VALUES (4, 'dd', 4, 'a')")
      await tx.execute("INSERT INTO items VALUES (5, 'ee', 5, 'a')")
      await tx.execute("INSERT INTO items VALUES (6, 'ff', 6, 'a')")
    })

    await waitForRows(live, rows => rows.length === 6)
    expect(sizes).toEqual([6])
  })

  it('follows an update that changes the primary key', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a'), (2, 'bb', 2, 'a')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id')
    open = live
    await harness.db.execute('UPDATE items SET id = 9 WHERE id = 1')

    const rows = await waitForRows(live, current => current.every(row => row.id !== 1))
    expect(rows).toEqual([{ id: 2 }, { id: 9 }])
  })

  it('re-reads when a transaction carries more changes than the result holds rows', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id', undefined, {
      rereadJitterMs: 0,
    })
    open = live
    await harness.db.executeTransaction(bulkInserts(20, 10, 'row'))

    expect((await waitForRows(live, rows => rows.length === 21)).length).toBe(21)
  })

  it('reports that it is re-reading and then settles', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id', undefined, {
      rereadJitterMs: 40,
    })
    open = live
    await harness.db.executeTransaction(bulkInserts(12, 10, 'row'))

    await waitForState(live, state => state.status === 'ready' && state.revalidating)
    await waitForRows(live, rows => rows.length === 13)
    expect(isRevalidating(live)).toBe(false)
  })

  it('keeps a LIMIT window full when a row inside it is deleted', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.executeTransaction(bulkInserts(6, 1, 'row'))

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id LIMIT 3', undefined, {
      rereadJitterMs: 0,
    })
    open = live
    expect(readyRows(live)).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])

    await harness.db.execute('DELETE FROM items WHERE id = 2')
    const rows = await waitForRows(live, current => current.length === 3 && current[1]?.id === 3)
    expect(rows).toEqual([{ id: 1 }, { id: 3 }, { id: 4 }])
  })

  it('pushes the tail out of a LIMIT window when a row enters ahead of it', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.executeTransaction(
      Array.from({ length: 5 }, (_, index) => ({
        sql: 'INSERT INTO items VALUES (?, ?, ?, ?)',
        params: [(index + 1) * 10, `row-${index}`, index, 'a'],
      })),
    )

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id LIMIT 3', undefined, {
      rereadJitterMs: 0,
    })
    open = live
    await harness.db.execute("INSERT INTO items VALUES (5, 'first', 0, 'a')")

    const rows = await waitForRows(live, current => current[0]?.id === 5)
    expect(rows).toEqual([{ id: 5 }, { id: 10 }, { id: 20 }])
  })

  it('serves an OFFSET window by reading again when the window moves', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.executeTransaction(bulkInserts(10, 1, 'row'))

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id LIMIT 3 OFFSET 3', undefined, {
      rereadJitterMs: 0,
    })
    open = live
    expect(readyRows(live)).toEqual([{ id: 4 }, { id: 5 }, { id: 6 }])

    await harness.db.execute('DELETE FROM items WHERE id = 1')
    const rows = await waitForRows(live, current => current[0]?.id === 5)
    expect(rows).toEqual([{ id: 5 }, { id: 6 }, { id: 7 }])
  })

  it('stops notifying once closed while a change is still in flight', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id')
    let notifications = 0
    live.subscribe(() => notifications++)

    await harness.db.execute("INSERT INTO items VALUES (2, 'bb', 2, 'a')")
    await live.close()
    await live.close()

    const after = notifications
    await harness.db.execute("INSERT INTO items VALUES (3, 'cc', 3, 'a')")
    await new Promise(resolve => setTimeout(resolve, 150))
    expect(notifications).toBe(after)
  })

  it('tracks a table with no declared primary key by its rowid', async () => {
    harness = await openHarness('CREATE TABLE notes (body TEXT NOT NULL, tag TEXT NOT NULL)')
    await harness.db.execute("INSERT INTO notes VALUES ('one', 'x'), ('two', 'y')")

    const live = await harness.db.live<{ body: string }>('SELECT body FROM notes WHERE tag = ? ORDER BY body', ['x'])
    open = live
    await harness.db.execute("UPDATE notes SET tag = 'x' WHERE body = 'two'")

    expect(await waitForRows(live, rows => rows.length === 2)).toEqual([{ body: 'one' }, { body: 'two' }])
  })

  it('orders text by the collation the column declares', async () => {
    harness = await openHarness('CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)')
    await harness.db.execute("INSERT INTO people VALUES (1, 'delta'), (2, 'Bravo')")

    const live = await harness.db.live<{ name: string }>('SELECT name FROM people ORDER BY name')
    open = live
    expect(readyRows(live)).toEqual([{ name: 'Bravo' }, { name: 'delta' }])

    await harness.db.execute("INSERT INTO people VALUES (3, 'charlie')")
    expect(await waitForRows(live, rows => rows.length === 3)).toEqual([
      { name: 'Bravo' },
      { name: 'charlie' },
      { name: 'delta' },
    ])
  })

  it('applies a transaction that spans more than one poll batch', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.executeTransaction(bulkInserts(1200, 1, 'row'))

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id', undefined, {
      rereadJitterMs: 0,
    })
    open = live
    expect(readyRows(live).length).toBe(1200)

    const sizes: number[] = []
    live.subscribe(() => {
      const state = live.getState()
      if (state.status === 'ready' && !state.revalidating) sizes.push(state.rows.length)
    })

    await harness.db.executeTransaction(bulkInserts(1100, 10_000, 'bulk'))

    const rows = await waitForRows(live, current => current.length === 2300, 15_000)
    expect(rows[0]).toEqual({ id: 1 })
    expect(rows[2299]).toEqual({ id: 11_099 })
    expect(sizes.every(size => size === 1200 || size === 2300)).toBe(true)
  })

  it('survives the database closing under it', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a')")

    const live = await harness.db.live<{ id: number }>('SELECT id FROM items ORDER BY id')
    await harness.db.close()
    await expect(live.close()).resolves.toBeUndefined()
  })

  it('keeps working when a clause ends in a line comment', async () => {
    harness = await openHarness(SCHEMA)
    await harness.db.execute("INSERT INTO items VALUES (1, 'aa', 1, 'a')")

    const live = await harness.db.live<{ id: number }>(
      "SELECT id -- projected\nFROM items WHERE bucket = 'a' -- filtered\nORDER BY id",
    )
    open = live
    expect(readyRows(live)).toEqual([{ id: 1 }])

    await harness.db.execute("INSERT INTO items VALUES (2, 'bb', 2, 'a')")
    expect(await waitForRows(live, rows => rows.length === 2)).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('refuses a statement whose result no row change can describe', async () => {
    harness = await openHarness(SCHEMA)

    await expect(harness.db.live('SELECT COUNT(*) AS total FROM items')).rejects.toThrow(/aggregate/i)
    await expect(harness.db.live('SELECT bucket FROM items GROUP BY bucket')).rejects.toThrow(/GROUP BY/i)
    await expect(harness.db.live('SELECT a.id FROM items a JOIN items b ON a.id = b.id')).rejects.toThrow(/join/i)
    await expect(harness.db.live('SELECT id FROM items LIMIT 5')).rejects.toThrow(/LIMIT/i)
  })
})
