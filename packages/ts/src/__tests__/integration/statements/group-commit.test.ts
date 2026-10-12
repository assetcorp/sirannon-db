import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { QueryError } from '../../../core/errors.js'
import { columnRaw, createWorkspace, DRIVER_PLACEMENTS, readRaw, type Workspace, walCommitCount } from './harness.js'

const LEDGER_SCHEMA =
  'CREATE TABLE ledger (id INTEGER PRIMARY KEY, memo TEXT NOT NULL UNIQUE, amount INTEGER CHECK (amount >= 0))'
const INSERT_ENTRY = 'INSERT INTO ledger (memo, amount) VALUES (?, ?)'
const CREDIT_ENTRY = 'UPDATE ledger SET amount = amount + ? WHERE memo = ?'
const TAKEN_MEMO = 'taken'
const BURST = 200
const STATEMENTS_PER_COMMIT_LIMIT = 1000
const OVER_LIMIT_WRITES = 2500
const STATEMENTS_PER_TRANSFER = 3
const OVER_LIMIT_TRANSFERS = 400
const REPEATS = 10

interface Attempt {
  sql: string
  params: unknown[]
  accepted: boolean
}

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('group-commit')
})

afterEach(async () => {
  await workspace.dispose()
})

function hostileAttempts(round: number): Attempt[] {
  return [
    { sql: INSERT_ENTRY, params: [`first-${round}`, round], accepted: true },
    { sql: INSERT_ENTRY, params: [TAKEN_MEMO, round], accepted: false },
    { sql: INSERT_ENTRY, params: [null, round], accepted: false },
    { sql: INSERT_ENTRY, params: [`negative-${round}`, -1], accepted: false },
    { sql: 'INSERT INTO ledger_archive (memo) VALUES (?)', params: [`archived-${round}`], accepted: false },
    { sql: 'UPDATE ledger SET memo = NULL WHERE memo = ?', params: [TAKEN_MEMO], accepted: false },
    { sql: INSERT_ENTRY, params: [`last-${round}`, round], accepted: true },
  ]
}

describe.each(DRIVER_PLACEMENTS)('group commit with $label', ({ driver, writerWorker }) => {
  async function openLedger(name = 'ledger.db'): Promise<{ db: Database; path: string }> {
    const path = workspace.path(name)
    const db = await workspace.registry({ driver: driver() }).open('ledger', path, {
      writerWorker: writerWorker && { maxPendingWrites: OVER_LIMIT_WRITES },
    })
    await db.execute(LEDGER_SCHEMA)
    await db.execute(INSERT_ENTRY, [TAKEN_MEMO, 0])
    return { db, path }
  }

  it('commits a burst of concurrent writes in a few commits, keeping the order in which they were made', async () => {
    const { db, path } = await openLedger()
    const commitsBefore = walCommitCount(path)

    const results = await Promise.all(
      Array.from({ length: BURST }, (_, index) => db.execute(INSERT_ENTRY, [`burst-${index}`, index])),
    )

    expect(walCommitCount(path) - commitsBefore).toBeLessThanOrEqual(3)
    const stored = readRaw<{ id: bigint; memo: string }>(
      path,
      'SELECT id, memo FROM ledger WHERE memo != ? ORDER BY id',
      [TAKEN_MEMO],
    )
    expect(stored.map(row => row.memo)).toEqual(Array.from({ length: BURST }, (_, index) => `burst-${index}`))
    expect(results.map(result => result.changes)).toEqual(Array(BURST).fill(1))
    expect(results.map(result => BigInt(result.lastInsertRowId))).toEqual(stored.map(row => row.id))
  })

  it('splits concurrent writes into commits of at most 1000 statements', async () => {
    const { db, path } = await openLedger()
    const commitsBefore = walCommitCount(path)
    await Promise.all(
      Array.from({ length: OVER_LIMIT_WRITES }, (_, index) => db.execute(INSERT_ENTRY, [`write-${index}`, index])),
    )
    const commitsForWrites = walCommitCount(path) - commitsBefore

    await Promise.all(
      Array.from({ length: OVER_LIMIT_TRANSFERS }, (_, index) =>
        db.executeTransaction([
          { sql: INSERT_ENTRY, params: [`transfer-${index}-from`, 0] },
          { sql: INSERT_ENTRY, params: [`transfer-${index}-to`, 0] },
          { sql: CREDIT_ENTRY, params: [index, `transfer-${index}-to`] },
        ]),
      ),
    )
    const commitsForTransfers = walCommitCount(path) - commitsBefore - commitsForWrites

    expect(commitsForWrites).toBeGreaterThanOrEqual(Math.ceil(OVER_LIMIT_WRITES / STATEMENTS_PER_COMMIT_LIMIT))
    expect(commitsForWrites).toBeLessThan(OVER_LIMIT_WRITES / 100)
    expect(commitsForTransfers).toBeGreaterThanOrEqual(
      Math.ceil((OVER_LIMIT_TRANSFERS * STATEMENTS_PER_TRANSFER) / STATEMENTS_PER_COMMIT_LIMIT),
    )
    expect(readRaw(path, 'SELECT count(*) AS n FROM ledger')).toEqual([
      { n: BigInt(1 + OVER_LIMIT_WRITES + OVER_LIMIT_TRANSFERS * 2) },
    ])
    expect(readRaw(path, "SELECT sum(amount) AS total FROM ledger WHERE memo LIKE 'transfer-%-to'")).toEqual([
      { total: BigInt((OVER_LIMIT_TRANSFERS * (OVER_LIMIT_TRANSFERS - 1)) / 2) },
    ])
  })

  it('rejects only the writes that SQLite refuses, each with its own statement, and commits every write grouped with them', async () => {
    const { db, path } = await openLedger()
    const attempts = Array.from({ length: REPEATS }, (_, round) => hostileAttempts(round)).flat()

    const settled = await Promise.allSettled(attempts.map(attempt => db.execute(attempt.sql, attempt.params)))

    expect(settled.map(outcome => outcome.status === 'fulfilled')).toEqual(attempts.map(attempt => attempt.accepted))
    settled.forEach((outcome, index) => {
      if (outcome.status === 'fulfilled') return
      expect(outcome.reason).toBeInstanceOf(QueryError)
      expect(outcome.reason).toMatchObject({ code: 'QUERY_ERROR', sql: attempts[index].sql })
    })
    const expected = [TAKEN_MEMO, ...attempts.filter(attempt => attempt.accepted).map(attempt => attempt.params[0])]
    expect(columnRaw(path, 'SELECT memo FROM ledger ORDER BY id')).toEqual(expected)
  })

  it('rolls back the whole of a failing transaction and commits every transaction grouped with it', async () => {
    const { db, path } = await openLedger()
    const failing = (index: number) => index % 5 === 2
    const transfers = Array.from({ length: 30 }, (_, index) => [
      { sql: INSERT_ENTRY, params: [`from-${index}`, 10] },
      { sql: INSERT_ENTRY, params: [`to-${index}`, 0] },
      failing(index)
        ? { sql: INSERT_ENTRY, params: [TAKEN_MEMO, 0] }
        : { sql: CREDIT_ENTRY, params: [index, `to-${index}`] },
    ])

    const settled = await Promise.allSettled(transfers.map(statements => db.executeTransaction(statements)))

    expect(settled.map(outcome => outcome.status)).toEqual(
      transfers.map((_, index) => (failing(index) ? 'rejected' : 'fulfilled')),
    )
    for (const [index, outcome] of settled.entries()) {
      if (outcome.status === 'fulfilled') {
        expect(outcome.value.map(result => result.changes)).toEqual([1, 1, 1])
      } else {
        expect(outcome.reason).toMatchObject({ code: 'QUERY_ERROR', sql: INSERT_ENTRY })
      }
      const stored = readRaw(path, 'SELECT memo, amount FROM ledger WHERE memo IN (?, ?) ORDER BY id', [
        `from-${index}`,
        `to-${index}`,
      ])
      expect(stored).toEqual(
        failing(index)
          ? []
          : [
              { memo: `from-${index}`, amount: 10n },
              { memo: `to-${index}`, amount: BigInt(index) },
            ],
      )
    }
  })

  it('commits the neighbours of a write whose conflict clause rolls back the whole transaction', async () => {
    const { db, path } = await openLedger()
    await db.execute('CREATE TABLE badges (id INTEGER PRIMARY KEY, holder TEXT UNIQUE ON CONFLICT ROLLBACK)')
    await db.execute('INSERT INTO badges (holder) VALUES (?)', ['ada'])
    const insertBadge = 'INSERT INTO badges (holder) VALUES (?)'

    const settled = await Promise.allSettled([
      db.execute(insertBadge, ['grace']),
      db.execute(insertBadge, ['ada']),
      db.executeTransaction([
        { sql: insertBadge, params: ['linus'] },
        { sql: INSERT_ENTRY, params: ['linus-ledger', 1] },
      ]),
      db.executeTransaction([
        { sql: insertBadge, params: ['margaret'] },
        { sql: insertBadge, params: ['ada'] },
      ]),
      db.execute(insertBadge, ['barbara']),
    ])

    expect(settled.map(outcome => outcome.status)).toEqual([
      'fulfilled',
      'rejected',
      'fulfilled',
      'rejected',
      'fulfilled',
    ])
    expect(columnRaw(path, 'SELECT holder FROM badges ORDER BY holder')).toEqual(['ada', 'barbara', 'grace', 'linus'])
    expect(columnRaw(path, 'SELECT memo FROM ledger ORDER BY id')).toEqual([TAKEN_MEMO, 'linus-ledger'])
  })

  it('fails a write that SQLite refuses to commit with TRANSACTION_ERROR and applies every other write in its group once', async () => {
    const { db, path } = await openLedger()
    await db.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY)')
    await db.execute(
      'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED)',
    )
    await db.execute('INSERT INTO customers (id) VALUES (1)')

    const settled = await Promise.allSettled([
      db.execute('INSERT INTO customers (id) VALUES (2)'),
      db.execute('INSERT INTO orders (id, customer_id) VALUES (10, 404)'),
      db.execute('INSERT INTO customers (id) VALUES (1)'),
      db.executeTransaction([
        { sql: 'INSERT INTO customers (id) VALUES (3)' },
        { sql: 'INSERT INTO orders (id, customer_id) VALUES (11, 3)' },
      ]),
      db.executeTransaction([
        { sql: 'INSERT INTO orders (id, customer_id) VALUES (12, 1)' },
        { sql: 'INSERT INTO orders (id, customer_id) VALUES (13, 405)' },
      ]),
      db.execute(INSERT_ENTRY, ['after-refusal', 1]),
    ])

    expect(settled.map(outcome => (outcome.status === 'fulfilled' ? 'ok' : outcome.reason.code))).toEqual([
      'ok',
      'TRANSACTION_ERROR',
      'QUERY_ERROR',
      'ok',
      'TRANSACTION_ERROR',
      'ok',
    ])
    expect(columnRaw(path, 'SELECT id FROM customers ORDER BY id')).toEqual([1n, 2n, 3n])
    expect(readRaw(path, 'SELECT id, customer_id FROM orders ORDER BY id')).toEqual([{ id: 11n, customer_id: 3n }])
    expect(columnRaw(path, 'SELECT memo FROM ledger ORDER BY id')).toEqual([TAKEN_MEMO, 'after-refusal'])
  })

  it('keeps the order of a burst that mixes grouped writes with statements that execute alone', async () => {
    const { db, path } = await openLedger()

    const settled = await Promise.allSettled([
      db.execute(INSERT_ENTRY, ['before-schema-change', 1]),
      db.execute('CREATE TABLE refunds (id INTEGER PRIMARY KEY, memo TEXT NOT NULL)'),
      db.execute('INSERT INTO refunds (memo) VALUES (?)', ['refund-1']),
      db.executeTransaction([
        { sql: 'INSERT INTO refunds (memo) VALUES (?)', params: ['refund-2'] },
        { sql: INSERT_ENTRY, params: ['refund-2-ledger', 2] },
      ]),
      db.execute('DROP TABLE refunds'),
      db.execute('INSERT INTO refunds (memo) VALUES (?)', ['refund-after-drop']),
      db.execute(INSERT_ENTRY, ['after-schema-change', 3]),
    ])

    expect(settled.map(outcome => outcome.status)).toEqual([
      'fulfilled',
      'fulfilled',
      'fulfilled',
      'fulfilled',
      'fulfilled',
      'rejected',
      'fulfilled',
    ])
    expect(columnRaw(path, "SELECT name FROM sqlite_master WHERE name = 'refunds'")).toEqual([])
    expect(columnRaw(path, 'SELECT memo FROM ledger ORDER BY id')).toEqual([
      TAKEN_MEMO,
      'before-schema-change',
      'refund-2-ledger',
      'after-schema-change',
    ])
  })
})
