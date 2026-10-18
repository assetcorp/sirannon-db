import { afterEach, describe, expect, it } from 'vitest'
import { postJson } from '../lib/served-registry.js'
import {
  changeLog,
  lastOfEachTransaction,
  pause,
  releaseChangeStreams,
  serveDatabase,
  type WireEvent,
  WireSocket,
  writeFile,
} from './harness.js'

const SCHEMA = [
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT NOT NULL, total INTEGER NOT NULL)',
  'CREATE TABLE audit (id INTEGER PRIMARY KEY, entry TEXT NOT NULL)',
]
const INSERT_ORDER = 'INSERT INTO orders (region, total) VALUES (?, ?)'
const LARGE_TRANSACTION_ROWS = 2_500
const DELIVERY_DEADLINE_MS = 30_000
const LONGER_THAN_A_POLL_MS = 150

afterEach(releaseChangeStreams)

function markers(events: readonly WireEvent[]): Array<[string, string | undefined, boolean]> {
  return events.map(event => [event.seq, event.txId, event.txEnd === true])
}

async function succeed(answer: Promise<{ status: number; text: string }>): Promise<void> {
  const settled = await answer
  expect(settled.status, settled.text).toBe(200)
  await pause(LONGER_THAN_A_POLL_MS)
}

describe('transaction boundaries on a table subscription', () => {
  it('marks the last change of each transaction, and only that change, on the live stream and on replay', async () => {
    const served = await serveDatabase({ label: 'transaction-ends', databaseId: 'shop', schema: SCHEMA })
    const base = served.served.base
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const writer = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const subscribed = await ws.request({ id: 'live', type: 'subscribe', table: 'orders' })

    await succeed(postJson(base, '/db/shop/execute', { sql: INSERT_ORDER, params: ['emea', 1] }))
    await succeed(postJson(base, '/db/shop/execute', { sql: INSERT_ORDER, params: ['apac', 2] }))
    await succeed(
      postJson(base, '/db/shop/transaction', {
        statements: [
          { sql: INSERT_ORDER, params: ['emea', 3] },
          { sql: 'INSERT INTO audit (entry) VALUES (?)', params: ['between'] },
          { sql: 'UPDATE orders SET total = total + 10 WHERE id = 1' },
          { sql: 'DELETE FROM orders WHERE id = 2' },
        ],
      }),
    )
    const batched = await writer.request({
      id: 'batch',
      type: 'batch',
      sql: INSERT_ORDER,
      paramsBatch: [1, 2, 3, 4].map(total => ['amer', total]),
    })
    await pause(LONGER_THAN_A_POLL_MS)
    await succeed(
      postJson(base, '/db/shop/batch', {
        sql: INSERT_ORDER,
        paramsBatch: [
          ['amer', 5],
          ['amer', 6],
        ],
      }),
    )
    await succeed(
      postJson(base, '/db/shop/load', { sql: INSERT_ORDER, paramsBatch: [5, 6, 7].map(total => ['apac', total]) }),
    )
    const large = await writer.request({
      id: 'large',
      type: 'batch',
      sql: INSERT_ORDER,
      paramsBatch: Array.from({ length: LARGE_TRANSACTION_ROWS }, (_, index) => ['bulk', index]),
    })
    await pause(LONGER_THAN_A_POLL_MS)
    await succeed(postJson(base, '/db/shop/execute', { sql: INSERT_ORDER, params: ['emea', 9] }))
    const log = changeLog(served.path, 'orders').filter(row => row.seq > BigInt(subscribed.seq ?? '0'))
    const live = await ws.waitForEvents('live', log.length, DELIVERY_DEADLINE_MS)
    const replay = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await replay.request({
      id: 'replay',
      type: 'subscribe',
      table: 'orders',
      sinceSeq: subscribed.seq,
      epoch: subscribed.epoch,
    })
    const replayed = await replay.waitForEvents('replay', log.length, DELIVERY_DEADLINE_MS)
    const ends = lastOfEachTransaction(log)
    const writes = [1, 1, 3, 4, 2, 3, LARGE_TRANSACTION_ROWS, 1]
    const transactionOfEachWrite = writes.map((size, index) => {
      const start = writes.slice(0, index).reduce((sum, earlier) => sum + earlier, 0)
      return new Set(log.slice(start, start + size).map(row => row.tx_id))
    })

    expect([batched.type, large.type]).toEqual(['result', 'result'])
    expect(log).toHaveLength(writes.reduce((sum, size) => sum + size, 0))
    expect(transactionOfEachWrite.map(ids => ids.size)).toEqual(writes.map(() => 1))
    expect(new Set(transactionOfEachWrite.flatMap(ids => [...ids]).filter(id => id !== '')).size).toBe(writes.length)
    expect(markers(live)).toEqual(log.map(row => [row.seq.toString(), row.tx_id, ends.has(row.seq)]))
    expect(live.filter(event => event.txEnd === true)).toHaveLength(writes.length)
    expect(replayed).toEqual(live)
  })

  it('marks a write from another process as its own transaction on replay, as it did live', async () => {
    const served = await serveDatabase({ label: 'external-transaction-ends', databaseId: 'shop', schema: SCHEMA })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const subscribed = await ws.request({ id: 'live', type: 'subscribe', table: 'orders' })

    const base = served.served.base
    const sirannonWrites: Array<() => Promise<{ status: number; text: string }>> = [
      () => postJson(base, '/db/shop/execute', { sql: INSERT_ORDER, params: ['emea', 2] }),
      () => postJson(base, '/db/shop/batch', { sql: INSERT_ORDER, paramsBatch: [['apac', 3]] }),
      () =>
        postJson(base, '/db/shop/transaction', {
          statements: [{ sql: 'SELECT count(*) FROM orders' }, { sql: INSERT_ORDER, params: ['amer', 4] }],
        }),
      () =>
        postJson(base, '/db/shop/transaction', {
          statements: [
            { sql: INSERT_ORDER, params: ['emea', 5] },
            { sql: INSERT_ORDER, params: ['emea', 6] },
          ],
        }),
    ]
    let expected = 0
    for (const [index, write] of sirannonWrites.entries()) {
      writeFile(served.path, 'INSERT INTO orders (region, total) VALUES (?, ?)', [`external-${index}`, index])
      expected += 1
      await ws.waitForEvents('live', expected)
      await succeed(write())
      expected += index === sirannonWrites.length - 1 ? 2 : 1
    }
    const live = await ws.waitForEvents('live', expected)
    const replay = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await replay.request({
      id: 'replay',
      type: 'subscribe',
      table: 'orders',
      sinceSeq: subscribed.seq,
      epoch: subscribed.epoch,
    })
    const replayed = await replay.waitForEvents('replay', expected)
    const log = changeLog(served.path, 'orders')
    const external = log.filter(row => String(JSON.parse(row.new_data ?? '{}').region).startsWith('external'))
    const stamped = log.filter(row => !external.includes(row))

    const serverNode = stamped[0]?.node_id
    const externalSeqs = new Set(external.map(row => row.seq))
    const hlcAfterEachExternal = log.flatMap((row, at) => (externalSeqs.has(row.seq) ? [log[at + 1]?.hlc ?? ''] : []))

    expect(external).toHaveLength(sirannonWrites.length)
    expect(serverNode).toMatch(/^[0-9a-f]{32}$/)
    expect(stamped.every(row => row.node_id === serverNode)).toBe(true)
    expect(external.map(row => [row.node_id, /^[0-9a-f]{32}$/.test(row.tx_id)])).toEqual(
      external.map(() => [serverNode, true]),
    )
    expect(external.map((row, at) => row.hlc !== '' && row.hlc < hlcAfterEachExternal[at])).toEqual(
      external.map(() => true),
    )
    expect(stamped.map(row => row.tx_id).filter(id => id === '')).toEqual([])
    expect(new Set(stamped.map(row => row.tx_id)).size).toBe(sirannonWrites.length)
    expect(new Set(log.map(row => row.tx_id)).size).toBe(sirannonWrites.length + external.length)
    expect(live.map(event => event.txEnd === true)).toEqual([true, true, true, true, true, true, true, false, true])
    const ends = lastOfEachTransaction(log)
    expect(markers(live)).toEqual(
      log.map(row => [row.seq.toString(), externalSeqs.has(row.seq) ? undefined : row.tx_id, ends.has(row.seq)]),
    )
    expect(markers(replayed)).toEqual(log.map(row => [row.seq.toString(), row.tx_id, ends.has(row.seq)]))
  })

  it('marks the last change that passes the filter and sends nothing for a transaction whose changes all fail it', async () => {
    const served = await serveDatabase({ label: 'filtered-transaction-ends', databaseId: 'shop', schema: SCHEMA })
    const ws = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    const subscribed = await ws.request({ id: 'emea', type: 'subscribe', table: 'orders', filter: { region: 'emea' } })
    const transactions: string[][] = [
      [
        "INSERT INTO orders VALUES (1, 'emea', 1)",
        "INSERT INTO orders VALUES (2, 'emea', 2)",
        "INSERT INTO orders VALUES (3, 'apac', 3)",
      ],
      ["INSERT INTO orders VALUES (4, 'apac', 4)", "INSERT INTO orders VALUES (5, 'apac', 5)"],
      ["INSERT INTO orders VALUES (6, 'apac', 6)", "INSERT INTO orders VALUES (7, 'emea', 7)"],
      [
        "UPDATE orders SET region = 'apac' WHERE id = 1",
        'UPDATE orders SET total = 20 WHERE id = 2',
        "UPDATE orders SET region = 'emea' WHERE id = 6",
      ],
      ["UPDATE orders SET region = 'amer' WHERE id = 4"],
      ["INSERT INTO orders VALUES (8, 'emea', 8)"],
    ]

    for (const statements of transactions) {
      await succeed(
        postJson(served.served.base, '/db/shop/transaction', { statements: statements.map(sql => ({ sql })) }),
      )
    }
    const live = await ws.waitForEvents('emea', 7)
    const replay = await WireSocket.open(`${served.served.wsBase}/db/shop`)
    await replay.request({
      id: 'emea',
      type: 'subscribe',
      table: 'orders',
      filter: { region: 'emea' },
      sinceSeq: subscribed.seq,
      epoch: subscribed.epoch,
    })
    const replayed = await replay.waitForEvents('emea', 7)

    expect(live.map(event => [event.type, event.rowId, event.txEnd === true])).toEqual([
      ['insert', '1', false],
      ['insert', '2', true],
      ['insert', '7', true],
      ['delete', '1', false],
      ['update', '2', false],
      ['insert', '6', true],
      ['insert', '8', true],
    ])
    expect(replayed).toEqual(live)
  })
})
