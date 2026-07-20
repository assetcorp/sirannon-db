import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { Sirannon } from '../sirannon.js'

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-writer-isolation-'))
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

function gate(): { wait: Promise<void>; open: () => void } {
  let open!: () => void
  const wait = new Promise<void>(resolve => {
    open = resolve
  })
  return { wait, open }
}

async function openDatabase(name: string) {
  const sirannon = new Sirannon({ driver: betterSqlite3() })
  const db = await sirannon.open('main', join(dir, name))
  await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY)')
  return { sirannon, db }
}

describe('writer isolation', () => {
  it('rolls back a write issued through the database inside a transaction callback', async () => {
    const { db } = await openDatabase('inside.db')

    await expect(
      db.transaction(async tx => {
        await tx.execute('INSERT INTO items (id) VALUES (1)')
        await db.execute('INSERT INTO items (id) VALUES (2)')
        throw new Error('abort')
      }),
    ).rejects.toThrow('abort')

    expect(await db.query('SELECT id FROM items ORDER BY id')).toEqual([])
    await db.close()
  })

  it('rolls back several writes issued through the database inside a transaction callback', async () => {
    const { db } = await openDatabase('inside-group.db')

    await expect(
      db.transaction(async tx => {
        await tx.execute('INSERT INTO items (id) VALUES (1)')
        const second = db.execute('INSERT INTO items (id) VALUES (2)')
        await db.execute('INSERT INTO items (id) VALUES (3)')
        await second
        throw new Error('abort')
      }),
    ).rejects.toThrow('abort')

    expect(await db.query('SELECT id FROM items ORDER BY id')).toEqual([])
    await db.close()
  })

  it('commits a write issued through the database inside a transaction that succeeds', async () => {
    const { db } = await openDatabase('inside-commit.db')

    await db.transaction(async tx => {
      await tx.execute('INSERT INTO items (id) VALUES (1)')
      await db.execute('INSERT INTO items (id) VALUES (2)')
    })

    expect(await db.query('SELECT id FROM items ORDER BY id')).toEqual([{ id: 1 }, { id: 2 }])
    await db.close()
  })

  it('keeps another caller write out of a transaction that rolls back', async () => {
    const { db } = await openDatabase('stranger.db')
    const held = gate()

    const transaction = db
      .transaction(async tx => {
        await tx.execute('INSERT INTO items (id) VALUES (1)')
        await held.wait
        await db.execute('INSERT INTO items (id) VALUES (3)')
        throw new Error('abort')
      })
      .catch((err: Error) => err.message)

    const other = db.execute('INSERT INTO items (id) VALUES (2)')
    held.open()

    expect(await transaction).toBe('abort')
    await expect(other).resolves.toMatchObject({ changes: 1 })
    expect(await db.query('SELECT id FROM items ORDER BY id')).toEqual([{ id: 2 }])
    await db.close()
  })

  it('waits for an in-flight transaction before closing', async () => {
    const { sirannon, db } = await openDatabase('close.db')
    const held = gate()

    const transaction = db.transaction(async tx => {
      await tx.execute('INSERT INTO items (id) VALUES (1)')
      await held.wait
      await tx.execute('INSERT INTO items (id) VALUES (2)')
      return 'committed'
    })

    await new Promise(resolve => setTimeout(resolve, 20))
    const closing = db.close()
    held.open()

    await expect(transaction).resolves.toBe('committed')
    await closing

    const reopened = await sirannon.open('verify', join(dir, 'close.db'))
    expect(await reopened.query('SELECT id FROM items ORDER BY id')).toEqual([{ id: 1 }, { id: 2 }])
    await reopened.close()
  })
})
