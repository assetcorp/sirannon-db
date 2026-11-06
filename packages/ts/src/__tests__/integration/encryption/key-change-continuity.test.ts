import { randomBytes } from 'node:crypto'
import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery } from '../../../core/live/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { ENCRYPTED_EXIT_TABLE, encryptedExitingDriver } from '../../helpers/fault-drivers/encrypted-exiting-driver.js'
import { readWithIndependentAegis } from '../../helpers/independent-aegis.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'

const binding = builtSirannonSqliteBindingPath()
const WAIT_LIMIT_MS = 10_000
const ROWS_BEFORE = 40
const ROWS_DURING = 15
const ROWS_AFTER = 40

interface Order {
  id: number
  item: string
}

async function waitUntil(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

function liveRows(query: LiveQuery<Order>): readonly Order[] | null {
  const state = query.getState()
  return state.status === 'ready' && !state.revalidating ? state.rows : null
}

function sortedById(rows: readonly Order[]): Order[] {
  return [...rows].map(row => ({ id: Number(row.id), item: row.item })).sort((a, b) => a.id - b.id)
}

describe.skipIf(binding === null)('subscribers and live queries across a key change', () => {
  let dir: string
  let sirannon: Sirannon

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-key-change-continuity-'))
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  it.each([
    false,
    true,
  ])('deliver every change exactly once in order and keep the live rows exact, with the writer worker set to %s', async writerWorker => {
    sirannon = new Sirannon({ driver: betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined }) })
    const firstKey = randomBytes(32)
    const secondKey = randomBytes(32)
    const path = join(dir, 'orders.db')
    const db: Database = await sirannon.open('orders', path, {
      writerWorker,
      cdcPollInterval: 10,
      encryption: { key: () => Uint8Array.from(firstKey) },
    })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)')
    await db.watch('orders')
    const events: ChangeEvent<Order>[] = []
    const errors: Error[] = []
    db.on('orders').subscribe<Order>(event => events.push(event), { onError: err => errors.push(err) })
    const live = await db.live<Order>('SELECT id, item FROM orders', [], { rereadJitterMs: 0 })
    const written: Order[] = []
    const insert = async (item: string) => {
      const id = Number((await db.execute('INSERT INTO orders (item) VALUES (?)', [item])).lastInsertRowId)
      written.push({ id, item })
    }
    for (let n = 0; n < ROWS_BEFORE; n++) await insert(`before-${n}`)

    let calls = 0
    const status = await db.changeEncryption({
      key: async () => {
        calls++
        if (calls === 2) for (let n = 0; n < ROWS_DURING; n++) await insert(`during-${n}`)
        return Uint8Array.from(secondKey)
      },
    })
    expect(status.change.state).toBe('done')
    for (let n = 0; n < ROWS_AFTER; n++) await insert(`after-${n}`)

    await waitUntil(() => events.length >= written.length, 'every change event')
    await waitUntil(() => liveRows(live)?.length === written.length, 'the live query to hold every row')
    expect(errors).toEqual([])
    expect(events.map(event => ({ id: Number(event.row.id), item: event.row.item }))).toEqual(written)
    const seqs = events.map(event => event.seq)
    expect(new Set(seqs).size).toBe(seqs.length)
    expect(seqs.every((seq, at) => at === 0 || seq > (seqs[at - 1] ?? 0n))).toBe(true)
    expect(sortedById(liveRows(live) ?? [])).toEqual(written)
    await live.close()
    await sirannon.close('orders')
    expect(sortedById(readWithIndependentAegis<Order>(path, secondKey, 'SELECT id, item FROM orders'))).toEqual(written)
  })

  it('aborts a key change whose writer worker dies part way, keeps the old key, and loses no committed row', async () => {
    const driverBinding = binding ?? ''
    sirannon = new Sirannon({ driver: encryptedExitingDriver(driverBinding) })
    const firstKey = randomBytes(32)
    const path = join(dir, 'crashed.db')
    const db = await sirannon.open('crashed', path, {
      writerWorker: { writeTimeoutMs: 5_000 },
      encryption: { key: () => Uint8Array.from(firstKey) },
    })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)')
    const written: Order[] = []
    const insert = async (item: string) => {
      const id = Number((await db.execute('INSERT INTO orders (item) VALUES (?)', [item])).lastInsertRowId)
      written.push({ id, item })
    }
    for (let n = 0; n < ROWS_BEFORE; n++) await insert(`before-${n}`)

    const secondKey = randomBytes(32)
    let calls = 0
    const outcome = await db
      .changeEncryption({
        key: async () => {
          calls++
          if (calls === 2) {
            await insert('before-the-crash')
            await db.execute(`SELECT * FROM ${ENCRYPTED_EXIT_TABLE}`).catch(() => undefined)
            await insert('after-the-crash')
          }
          return Uint8Array.from(secondKey)
        },
      })
      .catch(err => err)

    expect(outcome).toMatchObject({ code: 'ENCRYPTION_CHANGE_ABORTED' })
    await insert('after-the-abort')
    expect(sortedById(await db.query<Order>('SELECT id, item FROM orders'))).toEqual(written)
    await sirannon.close('crashed')
    expect(readdirSync(dir).filter(name => name.includes('.sirannon-key-change'))).toEqual([])
    expect(sortedById(readWithIndependentAegis<Order>(path, firstKey, 'SELECT id, item FROM orders'))).toEqual(written)
  }, 30_000)
})
