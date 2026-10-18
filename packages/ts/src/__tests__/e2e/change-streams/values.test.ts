import BetterSqlite3 from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { ChangeEvent } from '../../../core/types.js'
import { releaseChangeStreams, serveDatabase, type WireEvent, WireSocket, waitFor } from './harness.js'

const INT64_MAX = 2n ** 63n - 1n
const INT64_MIN = -(2n ** 63n)
const PAST_SAFE = 2n ** 53n + 1n
const HUGE = 2n ** 60n
const LEDGER_SCHEMA = 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, payload BLOB)'
const INSERT_ENTRY = 'INSERT INTO ledger (id, label, amount, payload) VALUES (?, ?, ?, ?)'
const ENVELOPE_LOOKALIKE = '{"__sirannon_int":"5"}'

afterEach(releaseChangeStreams)

function int(value: bigint): { __sirannon_int: string } {
  return { __sirannon_int: value.toString() }
}

function blob(hex: string): { __sirannon_blob: string } {
  return { __sirannon_blob: hex }
}

function writeRaw(path: string, statements: Array<[sql: string, params: unknown[]]>): void {
  const raw = new BetterSqlite3(path, { fileMustExist: true })
  try {
    raw.pragma('busy_timeout = 5000')
    for (const [sql, params] of statements) raw.prepare(sql).run(...params)
  } finally {
    raw.close()
  }
}

function contents(events: readonly WireEvent[]): unknown[] {
  return events.map(event => ({ type: event.type, row: event.row, oldRow: event.oldRow }))
}

describe('values in change events', () => {
  it('carry 64-bit integers, BLOBs, reals, NULLs, and text that looks like an envelope intact in row, oldRow, filters, and replay', async () => {
    const served = await serveDatabase({ label: 'change-values', databaseId: 'ledger', schema: [LEDGER_SCHEMA] })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/ledger`)
    const all = await ws.request({ id: 'all', type: 'subscribe', table: 'ledger' })
    const filtered: Array<[id: string, frame: string]> = [
      ['largest', '{"amount":{"__sirannon_int":"9223372036854775807"}}'],
      ['huge-as-number', '{"amount":1152921504606846976}'],
      ['payload', '{"payload":{"__sirannon_blob":"FF00"}}'],
      ['empty-payload', '{"payload":{"__sirannon_blob":""}}'],
      ['lookalike', `{"label":${JSON.stringify(ENVELOPE_LOOKALIKE)}}`],
    ]
    for (const [id, filter] of filtered) {
      ws.sendText(`{"type":"subscribe","id":"${id}","table":"ledger","filter":${filter}}`)
    }
    await waitFor(() => ws.messages.filter(message => message.type === 'subscribed').length === 6, 'six subscriptions')

    writeRaw(served.path, [
      [INSERT_ENTRY, [1, 'largest', INT64_MAX, Buffer.from([0xff, 0x00])]],
      [INSERT_ENTRY, [2, 'smallest', INT64_MIN, Buffer.alloc(0)]],
      [INSERT_ENTRY, [3, 'past safe', PAST_SAFE, Buffer.from('sirannon')]],
      [INSERT_ENTRY, [4, 'real', 1.5, null]],
      [INSERT_ENTRY, [5, ENVELOPE_LOOKALIKE, null, null]],
      [INSERT_ENTRY, [6, 'huge', HUGE, null]],
      ['UPDATE ledger SET amount = ? WHERE id = 1', [INT64_MIN + 1n]],
      ['DELETE FROM ledger WHERE id = 2', []],
    ])
    const events = await ws.waitForEvents('all', 8)
    const largest = { id: 1, label: 'largest', amount: int(INT64_MAX), payload: blob('FF00') }
    const smallest = { id: 2, label: 'smallest', amount: int(INT64_MIN), payload: blob('') }
    const lookalike = { id: 5, label: ENVELOPE_LOOKALIKE, amount: null, payload: null }
    const huge = { id: 6, label: 'huge', amount: int(HUGE), payload: null }
    const lowered = { ...largest, amount: int(INT64_MIN + 1n) }

    expect(contents(events)).toEqual([
      { type: 'insert', row: largest },
      { type: 'insert', row: smallest },
      { type: 'insert', row: { id: 3, label: 'past safe', amount: int(PAST_SAFE), payload: blob('736972616E6E6F6E') } },
      { type: 'insert', row: { id: 4, label: 'real', amount: 1.5, payload: null } },
      { type: 'insert', row: lookalike },
      { type: 'insert', row: huge },
      { type: 'update', row: lowered, oldRow: largest },
      { type: 'delete', row: {}, oldRow: smallest },
    ])
    expect(contents(ws.events('largest'))).toEqual([
      { type: 'insert', row: largest },
      { type: 'delete', row: {}, oldRow: largest },
    ])
    expect(contents(ws.events('huge-as-number'))).toEqual([{ type: 'insert', row: huge }])
    expect(contents(ws.events('payload'))).toEqual([
      { type: 'insert', row: largest },
      { type: 'update', row: lowered, oldRow: largest },
    ])
    expect(contents(ws.events('empty-payload'))).toEqual([
      { type: 'insert', row: smallest },
      { type: 'delete', row: {}, oldRow: smallest },
    ])
    expect(contents(ws.events('lookalike'))).toEqual([{ type: 'insert', row: lookalike }])

    const replay = await WireSocket.open(`${served.served.wsBase}/db/ledger`)
    await replay.request({ id: 'all', type: 'subscribe', table: 'ledger', sinceSeq: all.seq, epoch: all.epoch })
    expect(await replay.waitForEvents('all', 8)).toEqual(events)
  })

  it('hands the published client BigInt and byte values in change events and filters on them', async () => {
    const served = await serveDatabase({ label: 'client-change-values', databaseId: 'ledger', schema: [LEDGER_SCHEMA] })
    const client = new SirannonClient(served.served.base, { transport: 'websocket', autoReconnect: false })
    const ledger = client.database('ledger')
    const everything: ChangeEvent[] = []
    const largest: ChangeEvent[] = []
    await ledger.on('ledger').subscribe(event => everything.push(event))
    await ledger
      .on('ledger')
      .filter({ amount: INT64_MAX })
      .subscribe(event => largest.push(event))

    await ledger.execute(INSERT_ENTRY, [1, 'largest', INT64_MAX, new Uint8Array([0, 1, 254, 255])])
    await ledger.execute(INSERT_ENTRY, [2, 'small', 7, new Uint8Array(0)])
    await ledger.execute('UPDATE ledger SET amount = ? WHERE id = 1', [PAST_SAFE])
    await waitFor(() => everything.length === 3 && largest.length === 2, 'the decoded change events')
    client.close()
    const bytes = (value: unknown) => (value instanceof Uint8Array ? [...value] : value)
    const decoded = (events: ChangeEvent[]) =>
      events.map(event => ({
        type: event.type,
        amount: event.row.amount ?? null,
        payload: bytes(event.row.payload ?? null),
        oldAmount: event.oldRow?.amount ?? null,
      }))

    expect(decoded(everything)).toEqual([
      { type: 'insert', amount: INT64_MAX, payload: [0, 1, 254, 255], oldAmount: null },
      { type: 'insert', amount: 7, payload: [], oldAmount: null },
      { type: 'update', amount: PAST_SAFE, payload: [0, 1, 254, 255], oldAmount: INT64_MAX },
    ])
    expect(decoded(largest)).toEqual([
      { type: 'insert', amount: INT64_MAX, payload: [0, 1, 254, 255], oldAmount: null },
      { type: 'delete', amount: null, payload: null, oldAmount: INT64_MAX },
    ])
  })
})
