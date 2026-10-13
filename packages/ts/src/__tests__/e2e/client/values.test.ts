import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { ChangeEvent } from '../../../core/types.js'
import { readSqlite, waitUntil } from '../lib/served-registry.js'
import { DATABASE_ID, releaseClientServers, type ServedShop, servedShop } from './harness.js'

const LEDGER_SCHEMA = 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, balance INTEGER, payload BLOB, note TEXT)'
const INSERT_ENTRY = 'INSERT INTO ledger (balance, payload, note) VALUES (?, ?, ?)'
const INT64_MAX = 2n ** 63n - 1n
const INT64_MIN = -(2n ** 63n)
const PAST_SAFE = 2n ** 53n + 1n
const WIDE_ROW_IDS = { http: 2n ** 62n + 1n, websocket: 2n ** 62n + 1_000_001n } as const

interface StoredEntry {
  id: bigint
  balance: bigint | null
  balanceType: string
  payload: Buffer | null
  payloadType: string
  note: string | null
}

afterEach(releaseClientServers)

async function ledgerShop(): Promise<ServedShop> {
  const shop = await servedShop({ customers: [] })
  await shop.database.execute(LEDGER_SCHEMA)
  return shop
}

function storedEntries(path: string, note: string): StoredEntry[] {
  return readSqlite<StoredEntry>(
    path,
    'SELECT id, balance, typeof(balance) AS balanceType, payload, typeof(payload) AS payloadType, note FROM ledger WHERE note LIKE ? ORDER BY id',
    [`${note}%`],
  )
}

function nativeInteger(value: bigint | null): bigint | number | null {
  if (value === null) return null
  const safe = BigInt(Number.MAX_SAFE_INTEGER)
  return value >= -safe && value <= safe ? Number(value) : value
}

function bytesOf(value: unknown): number[] | string {
  return value instanceof Uint8Array ? [...value] : `not bytes: ${typeof value}`
}

describe('values that JSON cannot carry as they are', () => {
  it('stores and returns 64-bit integers and bytes exactly over both transports', async () => {
    const shop = await ledgerShop()
    const entries: Array<[balance: bigint | number, payload: Uint8Array, label: string]> = [
      [INT64_MAX, Buffer.from([0x00, 0xff, 0x7f, 0x80]), 'largest'],
      [INT64_MIN, new Uint8Array([]), 'smallest'],
      [PAST_SAFE, new Uint8Array([0xde, 0xad, 0xbe, 0xef]), 'past-safe'],
      [Number.MAX_SAFE_INTEGER, Buffer.alloc(4_096, 0xab), 'safe'],
      [-1, new Uint8Array([0]), 'negative'],
    ]

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(shop.served.base, { transport, autoReconnect: false })
      const db = client.database(DATABASE_ID)
      const [first, ...rest] = entries
      if (first === undefined) throw new Error('no entries')
      await db.execute(INSERT_ENTRY, [first[0], first[1], `${transport}:${first[2]}`])
      await db.batch(
        INSERT_ENTRY,
        rest.slice(0, 2).map(([balance, payload, label]) => [balance, payload, `${transport}:${label}`]),
      )
      await db.transaction(
        rest.slice(2).map(([balance, payload, label]) => ({
          sql: 'INSERT INTO ledger (balance, payload, note) VALUES (:balance, :payload, :note)',
          params: { balance, payload, note: `${transport}:${label}` },
        })),
      )
      const returned = await db.query<{ balance: unknown; payload: unknown; note: string }>(
        'SELECT balance, payload, note FROM ledger WHERE note LIKE ? ORDER BY id',
        [`${transport}:%`],
      )
      const matched = await db.query<{ note: string }>('SELECT note FROM ledger WHERE balance = ? AND payload = ?', [
        PAST_SAFE,
        new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
      ])
      const wideId = await db.execute('INSERT INTO ledger (id, balance, note) VALUES (?, ?, ?)', [
        WIDE_ROW_IDS[transport],
        0,
        `${transport}:wide-id`,
      ])
      client.close()

      const stored = storedEntries(shop.path, `${transport}:`).filter(entry => !entry.note?.endsWith('wide-id'))
      expect(
        stored.map(entry => [entry.balance, entry.balanceType, entry.payloadType, [...(entry.payload ?? [])]]),
        transport,
      ).toEqual(entries.map(([balance, payload]) => [BigInt(balance), 'integer', 'blob', [...payload]]))
      expect(
        returned.map(row => row.balance),
        transport,
      ).toEqual(stored.map(entry => nativeInteger(entry.balance)))
      expect(
        returned.map(row => bytesOf(row.payload)),
        transport,
      ).toEqual(stored.map(entry => [...(entry.payload ?? [])]))
      expect(
        matched.map(row => row.note),
        transport,
      ).toEqual(['http:past-safe', ...(transport === 'websocket' ? ['websocket:past-safe'] : [])])
      expect(wideId.lastInsertRowId, transport).toBe(WIDE_ROW_IDS[transport].toString())
    }
  })

  it('decodes integers and bytes in change events and matches a filter on the exact 64-bit integer', async () => {
    const shop = await ledgerShop()
    const client = new SirannonClient(shop.served.base, { transport: 'websocket', autoReconnect: false })
    const db = client.database(DATABASE_ID)
    const seen: ChangeEvent[] = []
    const subscription = await db
      .on('ledger')
      .filter({ balance: PAST_SAFE })
      .subscribe(event => {
        seen.push(event)
      })

    await shop.database.execute(INSERT_ENTRY, [PAST_SAFE - 1n, Buffer.from([1]), 'rounds-to-the-same-double'])
    await shop.database.execute(INSERT_ENTRY, [PAST_SAFE, Buffer.from([2, 0, 255]), 'exact'])
    await shop.database.execute('UPDATE ledger SET payload = ? WHERE note = ?', [Buffer.from([9, 9]), 'exact'])
    await waitUntil(() => seen.length >= 2, 'two filtered change events')
    subscription.unsubscribe()
    client.close()

    const [stored] = storedEntries(shop.path, 'exact')
    expect(seen.map(event => event.type)).toEqual(['insert', 'update'])
    expect(seen.map(event => event.row.balance)).toEqual([PAST_SAFE, PAST_SAFE])
    expect(seen.map(event => bytesOf(event.row.payload))).toEqual([[2, 0, 255], [...(stored?.payload ?? [])]])
    expect(bytesOf(seen[1]?.oldRow?.payload)).toEqual([2, 0, 255])
    expect(seen.every(event => event.row.note === 'exact')).toBe(true)
  })

  it('refuses an integer that does not fit in 64 bits and stores nothing', async () => {
    const shop = await ledgerShop()
    const outcomes: string[] = []

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(shop.served.base, { transport, autoReconnect: false })
      const db = client.database(DATABASE_ID)
      for (const balance of [2n ** 64n, INT64_MAX + 1n, INT64_MIN - 1n]) {
        outcomes.push(
          await db.execute(INSERT_ENTRY, [balance, null, `${transport}:overflow`]).then(
            () => 'stored',
            (err: { name?: string }) => err.name ?? 'unnamed',
          ),
        )
      }
      client.close()
    }

    expect(outcomes).toEqual(Array(6).fill('RemoteError'))
    expect(storedEntries(shop.path, '')).toEqual([])
  })
})
