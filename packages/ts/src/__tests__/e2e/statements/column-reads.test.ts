import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const INT64_MAX = 2n ** 63n - 1n
const SAFE_MAX = BigInt(Number.MAX_SAFE_INTEGER)
const REPEATED =
  'SELECT a.id, b.id, a.label, a.amount, a.payload, a.amount + 0 AS total FROM ledger a JOIN ledger b ON a.id = b.id ORDER BY a.id'
const ENDLESS = 'WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT i, zeroblob(4096) FROM n'

let path: string
let served: ServedRegistry
let ws: RawSocket
let sent = 0

interface OracleRow {
  id: bigint
  label: string | null
  amount: bigint | null
  payload: Buffer | null
}

function oracleRows(): OracleRow[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    raw.defaultSafeIntegers(true)
    return raw.prepare('SELECT id, label, amount, payload FROM ledger ORDER BY id').all() as OracleRow[]
  } finally {
    raw.close()
  }
}

function wireInteger(value: bigint | null): unknown {
  if (value === null) return null
  return value >= -SAFE_MAX && value <= SAFE_MAX ? Number(value) : { __sirannon_int: value.toString() }
}

function wireBlob(value: Buffer | null): unknown {
  return value === null ? null : { __sirannon_blob: value.toString('hex').toUpperCase() }
}

async function both(
  body: Record<string, unknown>,
): Promise<{ http: { status: number; body: unknown }; ws: ServerReply }> {
  const http = await postJson(served.base, '/db/ledger/query', body)
  const reply = (await ws.request({ id: `columns-${sent++}`, type: 'query', ...body })) as ServerReply
  return { http: { status: http.status, body: http.body }, ws: reply }
}

interface ServerReply {
  type: string
  data?: unknown
  error?: { code: string; message: string }
}

beforeEach(async () => {
  path = join(scratchDirectory('column-reads'), 'ledger.db')
  const sirannon = trackedRegistry()
  const db = await sirannon.open('ledger', path)
  await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, payload BLOB)')
  const insert = 'INSERT INTO ledger (id, label, amount, payload) VALUES (?, ?, ?, ?)'
  await db.execute(insert, [1, 'héllo 😀', INT64_MAX, Uint8Array.from([0xff, 0x00])])
  await db.execute(insert, [2, '{"__sirannon_int":"5"}', SAFE_MAX, new Uint8Array(0)])
  await db.execute(insert, [3, null, null, null])
  served = await serveRegistry(sirannon, { acceptSql: true })
  ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
})

afterEach(releaseServedRegistries)

describe('POST /db/:id/query and the WebSocket query with shape set to columns', () => {
  it('answers with every column, rows in column order with tagged values, and leaves the answer without shape as it was', async () => {
    const stored = oracleRows()
    const expectedRows = stored.map(row => [
      Number(row.id),
      Number(row.id),
      row.label,
      wireInteger(row.amount),
      wireBlob(row.payload),
      wireInteger(row.amount),
    ])

    const answers = await both({ sql: REPEATED, shape: 'columns' })
    const plain = await both({ sql: 'SELECT id, label FROM ledger ORDER BY id' })

    for (const data of [answers.http.body, answers.ws.data]) {
      expect(data).toEqual({
        columns: [
          { name: 'id', declaredType: 'INTEGER', table: 'ledger', column: 'id', database: 'main' },
          { name: 'id', declaredType: 'INTEGER', table: 'ledger', column: 'id', database: 'main' },
          { name: 'label', declaredType: 'TEXT', table: 'ledger', column: 'label', database: 'main' },
          { name: 'amount', declaredType: 'INTEGER', table: 'ledger', column: 'amount', database: 'main' },
          { name: 'payload', declaredType: 'BLOB', table: 'ledger', column: 'payload', database: 'main' },
          { name: 'total', declaredType: null, table: null, column: null, database: null },
        ],
        rows: expectedRows,
        truncated: null,
      })
    }
    const plainRows = stored.map(row => ({ id: Number(row.id), label: row.label }))
    expect(plain.http.body).toEqual({ rows: plainRows })
    expect(plain.ws.data).toEqual({ rows: plainRows })
  })

  it('stops at maxRows and maxBytes on an endless read, and refuses a bad shape or limit with the status of each transport', async () => {
    const rows = await both({ sql: ENDLESS, shape: 'columns', maxRows: 4 })
    const bytes = await both({ sql: ENDLESS, shape: 'columns', maxBytes: 3 * (16 + 4096) })
    for (const answer of [rows.http.body, rows.ws.data]) {
      expect(answer).toMatchObject({ truncated: 'rows' })
      expect((answer as { rows: unknown[][] }).rows.map(row => row[0])).toEqual([1, 2, 3, 4])
    }
    for (const answer of [bytes.http.body, bytes.ws.data]) {
      expect(answer).toMatchObject({ truncated: 'bytes' })
      expect((answer as { rows: unknown[] }).rows).toHaveLength(3)
    }

    const refused: Record<string, unknown>[] = [
      { sql: 'SELECT 1', shape: 'rows' },
      { sql: 'SELECT 1', shape: 7 },
      { sql: 'SELECT 1', maxRows: 1 },
      { sql: 'SELECT 1', maxBytes: 1 },
      { sql: 'SELECT 1', shape: 'columns', maxRows: -1 },
      { sql: 'SELECT 1', shape: 'columns', maxRows: 1.5 },
      { sql: 'SELECT 1', shape: 'columns', maxBytes: 2 ** 53 },
      { sql: 'SELECT 1', shape: 'columns', maxBytes: '10' },
      { sql: 'SELECT 1', shape: 'columns', maxRows: null },
    ]
    for (const body of refused) {
      const answer = await both(body)
      expect([answer.http.status, errorCodeOf(answer.http.body)], JSON.stringify(body)).toEqual([
        400,
        'INVALID_REQUEST',
      ])
      expect([answer.ws.type, answer.ws.error?.code], JSON.stringify(body)).toEqual(['error', 'INVALID_MESSAGE'])
    }
    const write = await both({ sql: 'DELETE FROM ledger', shape: 'columns' })
    expect(write.http.status).toBeGreaterThanOrEqual(400)
    expect(write.ws.type).toBe('error')
    expect(oracleRows()).toHaveLength(3)
  })
})

describe('a WebSocket query message that names a registered read', () => {
  it('refuses shape, maxRows, and maxBytes with INVALID_MESSAGE, and answers the same read without them', async () => {
    const sirannon = trackedRegistry()
    const db = await sirannon.open('ledger', join(scratchDirectory('column-reads-named'), 'ledger.db'))
    await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY)')
    await db.execute('INSERT INTO ledger (id) VALUES (1), (2)')
    const named = await serveRegistry(sirannon, {
      operations: {
        ledger: { reads: { everyEntry: { statement: () => ({ sql: 'SELECT id FROM ledger ORDER BY id' }) } } },
      },
    })
    const socket = await RawSocket.open(`${named.wsBase}/db/ledger`)

    const refused: Record<string, unknown>[] = [
      { shape: 'columns' },
      { shape: 'rows' },
      { maxRows: 1 },
      { maxBytes: 1 },
      { shape: 'columns', maxRows: 1, maxBytes: 64 },
      { shape: null },
    ]
    for (const [index, fields] of refused.entries()) {
      const reply = (await socket.request({
        id: `named-${index}`,
        type: 'query',
        name: 'everyEntry',
        ...fields,
      })) as ServerReply
      expect([reply.type, reply.error?.code], JSON.stringify(fields)).toEqual(['error', 'INVALID_MESSAGE'])
    }
    const plain = (await socket.request({ id: 'named-plain', type: 'query', name: 'everyEntry' })) as ServerReply
    expect(plain).toMatchObject({ type: 'result', data: { rows: [{ id: 1 }, { id: 2 }] } })
  })
})

describe.each(['http', 'websocket'] as const)('RemoteDatabase.queryWithColumns over %s', transport => {
  it('decodes tagged values in column order, stops at its limits, and refuses bad limits before sending them', async () => {
    const client = new SirannonClient(served.base, { transport, autoReconnect: false })
    try {
      const db = client.database('ledger')
      const stored = oracleRows()

      const read = await db.queryWithColumns(REPEATED)
      const limited = await db.queryWithColumns(ENDLESS, [], { maxRows: 2 })

      expect(read.columns.map(column => column.name)).toEqual(['id', 'id', 'label', 'amount', 'payload', 'total'])
      expect(read.truncated).toBeNull()
      expect(
        read.rows.map(row =>
          row.map(value => (value instanceof Uint8Array ? Buffer.from(value).toString('hex') : value)),
        ),
      ).toEqual(
        stored.map(row => {
          const amount = row.amount === null ? null : row.amount > SAFE_MAX ? row.amount : Number(row.amount)
          const payload = row.payload === null ? null : row.payload.toString('hex')
          return [Number(row.id), Number(row.id), row.label, amount, payload, amount]
        }),
      )
      expect(limited.truncated).toBe('rows')
      expect(limited.rows.map(row => row[0])).toEqual([1, 2])
      for (const options of [{ maxRows: -1 }, { maxBytes: 1.5 }, { maxRows: 2 ** 53 }]) {
        await expect(db.queryWithColumns('SELECT 1', [], options), JSON.stringify(options)).rejects.toMatchObject({
          code: 'INVALID_ARGUMENT',
        })
      }
      await expect(db.queryWithColumns("SELECT * FROM '_sirannon_meta'")).rejects.toMatchObject({
        code: 'FORBIDDEN_SQL',
      })
    } finally {
      client.close()
    }
  })
})
