import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/types.js'
import { collect, createWorkspace, PLACEMENTS, readRaw, type Workspace, waitForCount } from './harness.js'

interface Projection {
  type: string
  rowId: string | undefined
  row: Record<string, unknown>
  oldRow: Record<string, unknown> | undefined
}

function project(event: ChangeEvent): Projection {
  return { type: event.type, rowId: event.rowId, row: event.row, oldRow: event.oldRow }
}

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('events')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(PLACEMENTS)('change events with $label', placement => {
  it('delivers each insert, update, and delete in commit order with the row before and after it and the moment it committed', async () => {
    const db = await workspace.open('ledger', { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute('CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, balance INTEGER NOT NULL)')
    await db.watch('accounts')
    const seen = collect(db.on('accounts'))

    const writes: Array<{ sql: string; params?: unknown[][]; changes: number }> = [
      { sql: "INSERT INTO accounts VALUES (1, 'ama', 100)", changes: 1 },
      { sql: "INSERT INTO accounts VALUES (2, 'kofi', 250)", changes: 1 },
      { sql: "INSERT INTO accounts VALUES (3, 'esi', 75)", changes: 1 },
      { sql: 'UPDATE accounts SET balance = 300 WHERE id = 2', changes: 1 },
      { sql: 'UPDATE accounts SET balance = balance + 1 WHERE balance < 200', changes: 2 },
      { sql: 'DELETE FROM accounts WHERE id = 1', changes: 1 },
      {
        sql: 'INSERT INTO accounts VALUES (?, ?, ?)',
        params: [
          [4, 'yaw', 0],
          [5, 'abena', -5],
        ],
        changes: 2,
      },
    ]
    const windows: Array<{ statement: number; from: number; to: number }> = []
    for (const [statement, write] of writes.entries()) {
      const from = Date.now()
      if (write.params) await db.executeBatch(write.sql, write.params)
      else await db.execute(write.sql)
      const to = Date.now()
      for (let change = 0; change < write.changes; change++) windows.push({ statement, from, to })
    }
    await waitForCount(seen, windows.length)

    expect(seen.events.map(project)).toEqual([
      { type: 'insert', rowId: '1', row: { id: 1, owner: 'ama', balance: 100 }, oldRow: undefined },
      { type: 'insert', rowId: '2', row: { id: 2, owner: 'kofi', balance: 250 }, oldRow: undefined },
      { type: 'insert', rowId: '3', row: { id: 3, owner: 'esi', balance: 75 }, oldRow: undefined },
      {
        type: 'update',
        rowId: '2',
        row: { id: 2, owner: 'kofi', balance: 300 },
        oldRow: { id: 2, owner: 'kofi', balance: 250 },
      },
      {
        type: 'update',
        rowId: '1',
        row: { id: 1, owner: 'ama', balance: 101 },
        oldRow: { id: 1, owner: 'ama', balance: 100 },
      },
      {
        type: 'update',
        rowId: '3',
        row: { id: 3, owner: 'esi', balance: 76 },
        oldRow: { id: 3, owner: 'esi', balance: 75 },
      },
      { type: 'delete', rowId: '1', row: {}, oldRow: { id: 1, owner: 'ama', balance: 101 } },
      { type: 'insert', rowId: '4', row: { id: 4, owner: 'yaw', balance: 0 }, oldRow: undefined },
      { type: 'insert', rowId: '5', row: { id: 5, owner: 'abena', balance: -5 }, oldRow: undefined },
    ])

    const txIdsByStatement = new Map<number, Set<string | undefined>>()
    for (const [index, event] of seen.events.entries()) {
      const window = windows[index]
      expect(event.table).toBe('accounts')
      expect(Number.isInteger(event.timestamp)).toBe(true)
      expect(event.timestamp).toBeGreaterThanOrEqual(window.from - 1)
      expect(event.timestamp).toBeLessThanOrEqual(window.to + 1)
      if (index > 0) expect(event.seq > seen.events[index - 1].seq).toBe(true)
      const txIds = txIdsByStatement.get(window.statement) ?? new Set()
      txIds.add(event.txId)
      txIdsByStatement.set(window.statement, txIds)
    }
    const txIds = [...txIdsByStatement.values()].map(ids => [...ids])
    for (const ids of txIds) expect(ids).toEqual([expect.any(String)])
    expect(new Set(txIds.flat()).size).toBe(writes.length)
    expect(seen.errors).toEqual([])
  })

  it('delivers all the changes of one transaction in one batch, even a transaction of thousands of rows', async () => {
    const db = await workspace.open('bulk', { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, sensor TEXT NOT NULL, value INTEGER NOT NULL)')
    await db.watch('readings')

    let batch = 0
    let batchOpen = false
    const batches: number[] = []
    const seen = collect(db.on('readings'))
    db.on('readings').subscribe(() => {
      if (!batchOpen) {
        batchOpen = true
        batch++
        setImmediate(() => {
          batchOpen = false
        })
      }
      batches.push(batch)
    })

    const firstSize = 2500
    const secondSize = 1500
    await db.executeBatch(
      'INSERT INTO readings (id, sensor, value) VALUES (?, ?, ?)',
      Array.from({ length: firstSize }, (_, index) => [index + 1, `sensor-${index % 7}`, index + 1]),
    )
    await db.transaction(async tx => {
      for (let id = 1; id <= secondSize; id++) await tx.execute('UPDATE readings SET value = -value WHERE id = ?', [id])
    })
    await db.execute("INSERT INTO readings VALUES (9999, 'last', 0)")
    await waitForCount(seen, firstSize + secondSize + 1)

    const runs: Array<{ txId: string | undefined; count: number; batches: Set<number> }> = []
    for (const [index, event] of seen.events.entries()) {
      const current = runs.at(-1)
      if (current && current.txId === event.txId) {
        current.count++
        current.batches.add(batches[index])
      } else {
        runs.push({ txId: event.txId, count: 1, batches: new Set([batches[index]]) })
      }
    }
    expect(runs.map(run => run.count)).toEqual([firstSize, secondSize, 1])
    for (const run of runs) expect(run.batches.size).toBe(1)
    expect(seen.events.slice(0, firstSize).map(event => event.row.id)).toEqual(
      Array.from({ length: firstSize }, (_, index) => index + 1),
    )
    expect(seen.events.slice(firstSize, firstSize + secondSize).map(event => event.row.value)).toEqual(
      Array.from({ length: secondSize }, (_, index) => -(index + 1)),
    )
  })
})

describe('the row id of a change', () => {
  it('holds the key of a one-column key, the key columns joined by hyphens in key order, or the rowid when the table declares no key', async () => {
    const db = await workspace.open('shapes')
    await db.execute('CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID')
    await db.execute(
      'CREATE TABLE grants (user_id INTEGER, role TEXT, granted_at TEXT, PRIMARY KEY (user_id, role)) WITHOUT ROWID',
    )
    await db.execute('CREATE TABLE pairs (a INTEGER, b INTEGER, label TEXT, PRIMARY KEY (b, a))')
    await db.execute('CREATE TABLE logs (message TEXT NOT NULL)')
    await db.execute('CREATE TABLE extremes (id INTEGER PRIMARY KEY, note TEXT)')
    const tables = ['kv', 'grants', 'pairs', 'logs', 'extremes']
    for (const table of tables) await db.watch(table)
    const seen = Object.fromEntries(tables.map(table => [table, collect(db.on(table))]))

    await db.execute("INSERT INTO kv VALUES ('greeting', 'hello')")
    await db.execute("UPDATE kv SET value = 'hi' WHERE key = 'greeting'")
    await db.execute("DELETE FROM kv WHERE key = 'greeting'")
    await db.execute("INSERT INTO grants VALUES (7, 'admin', '2025-01-01')")
    await db.execute("UPDATE grants SET granted_at = '2026-01-01' WHERE user_id = 7")
    await db.execute('DELETE FROM grants WHERE user_id = 7')
    await db.execute("INSERT INTO pairs VALUES (1, 2, 'first')")
    await db.execute("INSERT INTO logs VALUES ('hello')")
    await db.execute("INSERT INTO logs (rowid, message) VALUES (9000000000000000000, 'far')")
    const [{ rowid: helloRowid }] = readRaw<{ rowid: bigint }>(
      workspace.path('shapes'),
      "SELECT rowid FROM logs WHERE message = 'hello'",
    )
    await db.execute("DELETE FROM logs WHERE message = 'far'")
    await db.execute('INSERT INTO extremes VALUES (?, ?)', [-(2n ** 63n), 'lowest'])
    await db.execute('INSERT INTO extremes VALUES (?, ?)', [2n ** 63n - 1n, 'highest'])
    for (const [table, count] of Object.entries({ kv: 3, grants: 3, pairs: 1, logs: 3, extremes: 2 })) {
      await waitForCount(seen[table], count)
    }

    const rowIds = Object.fromEntries(tables.map(table => [table, seen[table].events.map(event => event.rowId)]))
    expect(rowIds).toEqual({
      kv: ['greeting', 'greeting', 'greeting'],
      grants: ['7-admin', '7-admin', '7-admin'],
      pairs: ['2-1'],
      logs: [String(helloRowid), '9000000000000000000', '9000000000000000000'],
      extremes: ['-9223372036854775808', '9223372036854775807'],
    })
    expect(seen.grants.events.map(event => [event.type, event.row, event.oldRow])).toEqual([
      ['insert', { user_id: 7, role: 'admin', granted_at: '2025-01-01' }, undefined],
      [
        'update',
        { user_id: 7, role: 'admin', granted_at: '2026-01-01' },
        { user_id: 7, role: 'admin', granted_at: '2025-01-01' },
      ],
      ['delete', {}, { user_id: 7, role: 'admin', granted_at: '2026-01-01' }],
    ])
    expect(seen.logs.events.map(event => [event.type, event.row, event.oldRow])).toEqual([
      ['insert', { message: 'hello' }, undefined],
      ['insert', { message: 'far' }, undefined],
      ['delete', {}, { message: 'far' }],
    ])
    expect(seen.extremes.events.map(event => event.row.id)).toEqual([-(2n ** 63n), 2n ** 63n - 1n])
  })
})
