import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const INT64_MAX = 2n ** 63n - 1n
const INT64_MIN = -(2n ** 63n)
const SAFE_MAX = BigInt(Number.MAX_SAFE_INTEGER)
const INSERT_ENTRY = 'INSERT INTO ledger (label, amount, payload) VALUES (?, ?, ?)'
const WRITE_ROUTES = ['execute', 'transaction', 'batch', 'load'] as const

interface StoredEntry {
  label: string
  amount: bigint | number | null
  kind: string
  payload: string | null
  payloadKind: string
}

const STORED_VALUES: Array<[amount: bigint, payload: string]> = [
  [INT64_MAX, 'FF00'],
  [INT64_MIN, ''],
  [SAFE_MAX + 2n, '00'],
  [-(SAFE_MAX + 2n), 'DEADBEEF'],
  [SAFE_MAX, '0A'],
  [0n, '7F80'],
  [42n, 'C0FFEE'],
]

let path: string
let served: ServedRegistry

function envelopeOf(value: bigint): number | { __sirannon_int: string } {
  return value >= -SAFE_MAX && value <= SAFE_MAX ? Number(value) : { __sirannon_int: value.toString() }
}

function storedEntries(): StoredEntry[] {
  return readSqlite<StoredEntry>(
    path,
    'SELECT label, amount, typeof(amount) AS kind, iif(payload IS NULL, NULL, hex(payload)) AS payload, typeof(payload) AS payloadKind FROM ledger ORDER BY id',
  )
}

function writeBody(route: (typeof WRITE_ROUTES)[number], label: string): Record<string, unknown> {
  const sets = STORED_VALUES.map(([amount, payload], index) => [
    `${label}-${index}`,
    { __sirannon_int: amount.toString() },
    { __sirannon_blob: payload },
  ])
  if (route === 'execute') return { sql: INSERT_ENTRY, params: sets[0] }
  if (route === 'transaction') return { statements: sets.map(params => ({ sql: INSERT_ENTRY, params })) }
  return { sql: INSERT_ENTRY, paramsBatch: sets }
}

function insertRaw(label: string, amount: bigint | number | string | null, payload: Buffer | null): void {
  const raw = new BetterSqlite3(path)
  try {
    raw.defaultSafeIntegers(true)
    raw.prepare(INSERT_ENTRY).run(label, amount, payload)
  } finally {
    raw.close()
  }
}

beforeEach(async () => {
  path = join(scratchDirectory('value-encoding'), 'ledger.db')
  const sirannon = trackedRegistry()
  const db = await sirannon.open('ledger', path)
  await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, label TEXT NOT NULL, amount INTEGER, payload BLOB)')
  served = await serveRegistry(sirannon, { acceptSql: true })
})

afterEach(releaseServedRegistries)

describe('tagged values on the wire', () => {
  it('stores each tagged integer and BLOB exactly as sent, whichever write route and transport carries it', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
    const expected: StoredEntry[] = []

    for (const route of WRITE_ROUTES) {
      const overHttp = await postJson(served.base, `/db/ledger/${route}`, writeBody(route, `http-${route}`))
      const overWs = await ws.request({ id: route, type: route, ...writeBody(route, `ws-${route}`) })
      expect(overHttp.status, `${route} over HTTP ${overHttp.text}`).toBe(200)
      expect(overWs.type, `${route} over WebSocket ${JSON.stringify(overWs)}`).toBe('result')

      for (const transport of ['http', 'ws']) {
        const written = route === 'execute' ? STORED_VALUES.slice(0, 1) : STORED_VALUES
        for (const [index, [amount, payload]] of written.entries()) {
          expected.push({
            label: `${transport}-${route}-${index}`,
            amount,
            kind: 'integer',
            payload,
            payloadKind: 'blob',
          })
        }
      }
    }

    const byLabel = (entry: StoredEntry) => entry.label
    expect(storedEntries().sort((a, b) => byLabel(a).localeCompare(byLabel(b)))).toEqual(
      expected.sort((a, b) => byLabel(a).localeCompare(byLabel(b))),
    )
  })

  it('returns integers past the safe range and every BLOB as envelopes, and every other value in its own JSON form', async () => {
    const values: Array<[label: string, amount: bigint | number | string | null, payload: Buffer | null]> = [
      ['largest', INT64_MAX, Buffer.from([0xff, 0x00])],
      ['smallest', INT64_MIN, Buffer.alloc(0)],
      ['just past safe', SAFE_MAX + 1n, Buffer.from('sirannon')],
      ['just below negative safe', -(SAFE_MAX + 1n), null],
      ['largest safe', SAFE_MAX, null],
      ['smallest safe', -SAFE_MAX, null],
      ['real', 1.5, null],
      ['text that looks like an envelope', '{"__sirannon_int":"5"}', null],
      ['null', null, null],
    ]
    for (const [label, amount, payload] of values) insertRaw(label, amount, payload)
    const expectedRows = values.map(([label, amount, payload]) => ({
      label,
      amount: typeof amount === 'bigint' ? envelopeOf(amount) : amount,
      payload: payload === null ? null : { __sirannon_blob: payload.toString('hex').toUpperCase() },
    }))
    const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
    const sql = 'SELECT label, amount, payload FROM ledger ORDER BY id'

    const overHttp = await postJson(served.base, '/db/ledger/query', { sql })
    const overWs = await ws.request({ id: 'rows', type: 'query', sql })

    expect(overHttp.body).toEqual({ rows: expectedRows })
    expect(overWs.data).toEqual({ rows: expectedRows })
  })

  it('carries the same envelopes in a change event as in a query row', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
    const subscribed = await ws.request({ id: 'ledger', type: 'subscribe', table: 'ledger' })
    const change = ws.reply('ledger')
    const amount = { __sirannon_int: (SAFE_MAX + 4n).toString() }
    const payload = { __sirannon_blob: '0102FEFF' }

    await postJson(served.base, '/db/ledger/execute', { sql: INSERT_ENTRY, params: ['watched', amount, payload] })
    const event = ((await change).event ?? {}) as { row?: Record<string, unknown> }
    const read = await postJson(served.base, '/db/ledger/query', { sql: 'SELECT * FROM ledger' })

    expect(subscribed.type).toBe('subscribed')
    expect(event.row).toEqual({ id: 1, label: 'watched', amount, payload })
    expect(read.body).toEqual({ rows: [{ id: 1, label: 'watched', amount, payload }] })
  })

  it('refuses a malformed envelope in the parameters of every route with 400 or INVALID_MESSAGE and stores nothing', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
    const malformed = [
      { __sirannon_int: '' },
      { __sirannon_int: '+1' },
      { __sirannon_int: ' 1' },
      { __sirannon_int: '1 ' },
      { __sirannon_int: '1.5' },
      { __sirannon_int: '1e3' },
      { __sirannon_int: '0x10' },
      { __sirannon_int: '12abc' },
      { __sirannon_int: '-' },
      { __sirannon_int: '--1' },
      { __sirannon_int: '1'.repeat(20) },
      { __sirannon_int: `-${'1'.repeat(20)}` },
      { __sirannon_blob: 'F' },
      { __sirannon_blob: 'GG' },
      { __sirannon_blob: 'F F0' },
      { __sirannon_blob: '0x00' },
    ]
    const routes: Array<[route: string, body: (value: unknown) => Record<string, unknown>]> = [
      ['query', value => ({ sql: 'SELECT ? AS v', params: [value] })],
      ['query', value => ({ sql: 'SELECT :v AS v', params: { v: value } })],
      ['execute', value => ({ sql: INSERT_ENTRY, params: ['lost', value, null] })],
      [
        'transaction',
        value => ({
          statements: [
            { sql: INSERT_ENTRY, params: ['lost', 1, null] },
            { sql: INSERT_ENTRY, params: ['lost', value, null] },
          ],
        }),
      ],
      [
        'batch',
        value => ({
          sql: INSERT_ENTRY,
          paramsBatch: [
            ['lost', 1, null],
            ['lost', value, null],
          ],
        }),
      ],
      [
        'load',
        value => ({
          sql: INSERT_ENTRY,
          paramsBatch: [
            ['lost', 1, null],
            ['lost', value, null],
          ],
        }),
      ],
    ]

    let sent = 0
    for (const value of malformed) {
      for (const [route, body] of routes) {
        const label = `${route} ${JSON.stringify(value)}`
        const answer = await postJson(served.base, `/db/ledger/${route}`, body(value))
        const reply = await ws.request({ id: `malformed-${sent++}`, type: route, ...body(value) })

        expect([answer.status, errorCodeOf(answer.body)], label).toEqual([400, 'INVALID_REQUEST'])
        expect([reply.type, reply.error?.code], label).toEqual(['error', 'INVALID_MESSAGE'])
      }
    }
    expect(storedEntries()).toEqual([])
  })

  it('refuses an integer envelope past the 64-bit range on every write route and stores nothing', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
    const outOfRange = [(INT64_MAX + 1n).toString(), (INT64_MIN - 1n).toString(), '9'.repeat(19), `-${'9'.repeat(19)}`]

    for (const decimal of outOfRange) {
      const value = { __sirannon_int: decimal }
      for (const [route, body] of [
        ['execute', { sql: INSERT_ENTRY, params: ['lost', value, null] }],
        [
          'transaction',
          {
            statements: [
              { sql: INSERT_ENTRY, params: ['lost', 1, null] },
              { sql: INSERT_ENTRY, params: ['lost', value, null] },
            ],
          },
        ],
        [
          'batch',
          {
            sql: INSERT_ENTRY,
            paramsBatch: [
              ['lost', 1, null],
              ['lost', value, null],
            ],
          },
        ],
        [
          'load',
          {
            sql: INSERT_ENTRY,
            paramsBatch: [
              ['lost', 1, null],
              ['lost', value, null],
            ],
          },
        ],
      ] as const) {
        const answer = await postJson(served.base, `/db/ledger/${route}`, body)
        const reply = await ws.request({ id: `${route}-${decimal}`, type: route, ...body })

        expect(answer.status, `${route} ${decimal}`).toBe(400)
        expect(reply.type, `${route} ${decimal}`).toBe('error')
      }
    }
    expect(storedEntries()).toEqual([])
  })

  it('round-trips 64-bit integers and bytes through the published client over each transport', async () => {
    const bytes = new Uint8Array([0, 1, 127, 128, 255])
    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(served.base, { transport, autoReconnect: false })
      const ledger = client.database('ledger')
      await ledger.execute(INSERT_ENTRY, [`${transport}-max`, INT64_MAX, bytes])
      await ledger.batch(INSERT_ENTRY, [
        [`${transport}-min`, INT64_MIN, new Uint8Array(0)],
        [`${transport}-safe`, SAFE_MAX, null],
      ])
      const rows = await ledger.query<{ label: string; amount: unknown; payload: unknown }>(
        'SELECT label, amount, payload FROM ledger WHERE label LIKE ? ORDER BY id',
        [`${transport}-%`],
      )

      const decoded = rows.map(row => ({
        ...row,
        payload: row.payload instanceof Uint8Array ? [...row.payload] : row.payload,
      }))

      expect(decoded, transport).toEqual([
        { label: `${transport}-max`, amount: INT64_MAX, payload: [...bytes] },
        { label: `${transport}-min`, amount: INT64_MIN, payload: [] },
        { label: `${transport}-safe`, amount: Number(SAFE_MAX), payload: null },
      ])
      client.close()
    }
    expect(storedEntries().map(entry => [entry.label, entry.amount, entry.payload])).toEqual([
      ['http-max', INT64_MAX, '00017F80FF'],
      ['http-min', INT64_MIN, ''],
      ['http-safe', SAFE_MAX, null],
      ['websocket-max', INT64_MAX, '00017F80FF'],
      ['websocket-min', INT64_MIN, ''],
      ['websocket-safe', SAFE_MAX, null],
    ])
  })
})
