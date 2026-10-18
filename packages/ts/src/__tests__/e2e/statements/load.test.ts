import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import type { Database } from '../../../core/database.js'
import type { ServerExecutionTarget } from '../../../core/types.js'
import {
  countSqlite,
  errorCodeOf,
  errorMessageOf,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const INSERT_READING = 'INSERT INTO readings (id, value) VALUES (?, ?)'
const SYNCHRONOUS_FULL = 2
const LOADED_ROWS = 250
const CLIENT_BATCH_SIZE = 40

let path: string
let database: Database
let served: ServedRegistry

function readings(count: number, firstId = 1): Array<[number, number]> {
  return Array.from({ length: count }, (_, index) => [firstId + index, (firstId + index) * 0.25])
}

async function writerSynchronous(): Promise<number> {
  return database.transaction(async tx => {
    const [row] = await tx.query<{ synchronous: number }>('PRAGMA synchronous')
    return row?.synchronous ?? -1
  })
}

function storedReadings(): Array<[number, number]> {
  return readSqlite<{ id: bigint; value: number }>(path, 'SELECT id, value FROM readings ORDER BY id').map(row => [
    Number(row.id),
    row.value,
  ])
}

beforeEach(async () => {
  path = join(scratchDirectory('load'), 'readings.db')
  const sirannon = trackedRegistry()
  database = await sirannon.open('readings', path, { synchronous: 'full' })
  await database.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL NOT NULL)')
  served = await serveRegistry(sirannon, { acceptSql: true })
})

afterEach(releaseServedRegistries)

describe('loading rows through the server', () => {
  it('loads every row in one transaction over each transport, reports the totals, and restores full durability', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/readings`)
    const loads: Array<[label: string, body: Record<string, unknown>, rows: Array<[number, number]>]> = [
      ['the default durability', { sql: INSERT_READING, paramsBatch: readings(LOADED_ROWS) }, readings(LOADED_ROWS)],
      [
        'normal durability with no checkpoint',
        { sql: INSERT_READING, paramsBatch: readings(10, 1_000), durability: 'normal', checkpoint: false },
        readings(10, 1_000),
      ],
      [
        'durability off with a checkpoint',
        { sql: INSERT_READING, paramsBatch: readings(5, 2_000), durability: 'off', checkpoint: true },
        readings(5, 2_000),
      ],
    ]
    const expected: Array<[number, number]> = []

    for (const [label, body, rows] of loads) {
      const answer = await postJson(served.base, '/db/readings/load', body)
      expect([answer.status, answer.body], `${label} over HTTP`).toEqual([
        200,
        { rowsLoaded: rows.length, changes: rows.length },
      ])
      expected.push(...rows)
      expect(await writerSynchronous(), label).toBe(SYNCHRONOUS_FULL)

      const shifted = (body.paramsBatch as Array<[number, number]>).map(([id, value]) => [id + 10_000, value])
      const reply = await ws.request({ id: label, type: 'load', ...body, paramsBatch: shifted })
      expect(reply.data, `${label} over WebSocket`).toEqual({ rowsLoaded: rows.length, changes: rows.length })
      expected.push(...rows.map(([id, value]): [number, number] => [id + 10_000, value]))
      expect(await writerSynchronous(), label).toBe(SYNCHRONOUS_FULL)
    }

    expect(storedReadings()).toEqual(expected.sort((a, b) => a[0] - b[0]))
  })

  it('rolls back a load that breaks a constraint, refuses bad options, and restores full durability each time', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/readings`)
    const failing: Array<[body: Record<string, unknown>, status: number, code: string, mentions: string]> = [
      [{ sql: INSERT_READING, paramsBatch: [...readings(50), [25, 9]] }, 400, 'QUERY_ERROR', 'UNIQUE'],
      [
        { sql: 'INSERT INTO readings (id, value) VALUES (?, NULL)', paramsBatch: [[1]] },
        400,
        'QUERY_ERROR',
        'NOT NULL',
      ],
      [{ sql: INSERT_READING, paramsBatch: readings(3), durability: 'sometimes' }, 400, 'INVALID', 'durability'],
      [{ sql: INSERT_READING, paramsBatch: readings(3), checkpoint: 'always' }, 400, 'INVALID', 'checkpoint'],
      [{ sql: INSERT_READING }, 400, 'INVALID', 'paramsBatch'],
      [{ paramsBatch: readings(3) }, 400, 'INVALID', 'sql'],
    ]

    for (const [index, [body, status, code, mentions]] of failing.entries()) {
      const label = JSON.stringify(body).slice(0, 120)
      const answer = await postJson(served.base, '/db/readings/load', body)
      const reply = await ws.request({ id: `failing-${index}`, type: 'load', ...body })
      const httpCode = errorCodeOf(answer.body) === 'INVALID_REQUEST' ? 'INVALID' : errorCodeOf(answer.body)
      const wsCode = reply.error?.code === 'INVALID_MESSAGE' ? 'INVALID' : reply.error?.code

      expect([answer.status, httpCode], label).toEqual([status, code])
      expect(errorMessageOf(answer.body), label).toContain(mentions)
      expect(wsCode, label).toBe(code)
      expect(reply.error?.message, label).toContain(mentions)
      expect(await writerSynchronous(), label).toBe(SYNCHRONOUS_FULL)
    }
    expect(countSqlite(path, 'readings')).toBe(0)
  })

  it('answers 501 BULK_LOAD_UNSUPPORTED from an execution target that cannot bulk load, and writes nothing', async () => {
    const withoutBulkLoad: ServerExecutionTarget = {
      query: (sql, params, options) => database.query(sql, params, options),
      execute: (sql, params, options) => database.execute(sql, params, options),
      transaction: fn => database.transaction(fn),
    }
    const proxied = await serveRegistry(served.sirannon, {
      acceptSql: true,
      resolveExecutionTarget: () => withoutBulkLoad,
    })
    const ws = await RawSocket.open(`${proxied.wsBase}/db/readings`)
    const body = { sql: INSERT_READING, paramsBatch: readings(3) }

    const answer = await postJson(proxied.base, '/db/readings/load', body)
    const reply = await ws.request({ id: 'load', type: 'load', ...body })
    const execute = await postJson(proxied.base, '/db/readings/execute', { sql: INSERT_READING, params: [7, 0.5] })

    expect([answer.status, errorCodeOf(answer.body)]).toEqual([501, 'BULK_LOAD_UNSUPPORTED'])
    expect([reply.type, reply.error?.code]).toEqual(['error', 'BULK_LOAD_UNSUPPORTED'])
    expect(execute.status).toBe(200)
    expect(storedReadings()).toEqual([[7, 0.5]])
  })

  it('loads a dataset larger than one request through the published client in batches', async () => {
    for (const [index, transport] of (['http', 'websocket'] as const).entries()) {
      const client = new SirannonClient(served.base, { transport, autoReconnect: false })
      const firstId = 1 + index * 100_000
      async function* stream(): AsyncGenerator<[number, number]> {
        for (const row of readings(LOADED_ROWS, firstId)) yield row
      }

      const summary = await client
        .database('readings')
        .loadAll(INSERT_READING, stream(), { batchSize: CLIENT_BATCH_SIZE, durability: 'normal' })

      expect(summary, transport).toEqual({ rowsLoaded: LOADED_ROWS, changes: LOADED_ROWS })
      expect(await writerSynchronous(), transport).toBe(SYNCHRONOUS_FULL)
      client.close()
    }
    expect(storedReadings()).toEqual([...readings(LOADED_ROWS, 1), ...readings(LOADED_ROWS, 100_001)])
  })
})
