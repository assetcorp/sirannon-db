import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import {
  columnRaw,
  countRaw,
  createWorkspace,
  DRIVER_PLACEMENTS,
  deferred,
  readRaw,
  type Workspace,
} from './harness.js'

const ACCOUNTS_SCHEMA =
  'CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT NOT NULL UNIQUE, balance INTEGER NOT NULL CHECK (balance >= 0))'
const OPEN_ACCOUNT = 'INSERT INTO accounts (owner, balance) VALUES (?, ?)'
const DEPOSIT = 'UPDATE accounts SET balance = balance + ? WHERE owner = ?'
const OWNERS_IN_ORDER = 'SELECT owner FROM accounts ORDER BY id'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('transactions')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(DRIVER_PLACEMENTS)('transactions with $label', ({ driver, writerWorker }) => {
  async function openAccounts(): Promise<{ db: Database; path: string }> {
    const path = workspace.path('accounts.db')
    const db = await workspace.registry({ driver: driver() }).open('accounts', path, { writerWorker })
    await db.execute(ACCOUNTS_SCHEMA)
    return { db, path }
  }

  it('commits a callback that returns, hands back its value, and hides its writes from readers until the commit', async () => {
    const { db, path } = await openAccounts()
    const readerViews: unknown[] = []

    const value = await db.transaction(async tx => {
      const before = tx.lastInsertRowId
      await tx.execute(OPEN_ACCOUNT, ['ada', 10])
      await tx.executeBatch(OPEN_ACCOUNT, [
        ['grace', 20],
        ['linus', 30],
      ])
      readerViews.push(await db.query(OWNERS_IN_ORDER))
      const inside = await tx.query<{ owner: string }>(OWNERS_IN_ORDER)
      return { before, inside: inside.map(row => row.owner), last: tx.lastInsertRowId }
    })

    expect(value).toEqual({ before: 0, inside: ['ada', 'grace', 'linus'], last: 3 })
    expect(readerViews).toEqual([[]])
    expect(readRaw(path, 'SELECT id, owner FROM accounts ORDER BY id')).toEqual([
      { id: 1n, owner: 'ada' },
      { id: 2n, owner: 'grace' },
      { id: 3n, owner: 'linus' },
    ])
  })

  it('makes every write issued through the database inside a callback part of that transaction', async () => {
    const { db, path } = await openAccounts()
    async function writeThroughEveryMethod(fail: boolean, suffix: string): Promise<void> {
      await db.transaction(async tx => {
        await tx.execute(OPEN_ACCOUNT, [`ada-${suffix}`, 10])
        await Promise.all([
          db.execute(OPEN_ACCOUNT, [`grace-${suffix}`, 20]),
          db.execute(DEPOSIT, [5, `ada-${suffix}`]),
        ])
        await db.executeBatch(OPEN_ACCOUNT, [
          [`margaret-${suffix}`, 1],
          [`barbara-${suffix}`, 2],
        ])
        await db.executeTransaction([
          { sql: OPEN_ACCOUNT, params: [`edsger-${suffix}`, 3] },
          { sql: DEPOSIT, params: [7, `edsger-${suffix}`] },
        ])
        await db.transaction(async inner => {
          await inner.execute(OPEN_ACCOUNT, [`donald-${suffix}`, 4])
        })
        if (fail) throw new Error(`declined ${suffix}`)
      })
    }

    await expect(writeThroughEveryMethod(true, 'declined')).rejects.toThrow('declined declined')
    expect(countRaw(path, 'accounts')).toBe(0)

    await writeThroughEveryMethod(false, 'kept')
    expect(readRaw(path, 'SELECT owner, balance FROM accounts ORDER BY id')).toEqual([
      { owner: 'ada-kept', balance: 15n },
      { owner: 'grace-kept', balance: 20n },
      { owner: 'margaret-kept', balance: 1n },
      { owner: 'barbara-kept', balance: 2n },
      { owner: 'edsger-kept', balance: 10n },
      { owner: 'donald-kept', balance: 4n },
    ])
  })

  it('rolls back only the nested call that fails inside a callback and commits the rest of the callback', async () => {
    const { db, path } = await openAccounts()
    await db.execute(OPEN_ACCOUNT, ['taken', 0])

    const nestedOutcomes = await db.transaction(async tx => {
      await tx.execute(OPEN_ACCOUNT, ['before', 1])
      const outcomes = await Promise.allSettled([
        db.executeTransaction([
          { sql: OPEN_ACCOUNT, params: ['list-first', 1] },
          { sql: OPEN_ACCOUNT, params: ['taken', 1] },
        ]),
      ])
      outcomes.push(
        ...(await Promise.allSettled([
          db.executeBatch(OPEN_ACCOUNT, [
            ['batch-first', 1],
            ['batch-negative', -1],
          ]),
        ])),
      )
      outcomes.push(
        ...(await Promise.allSettled([
          db.transaction(async inner => {
            await inner.execute(OPEN_ACCOUNT, ['inner-first', 1])
            throw new Error('inner refused')
          }),
        ])),
      )
      await tx.execute(OPEN_ACCOUNT, ['after', 2])
      return outcomes.map(outcome => (outcome.status === 'rejected' ? String(outcome.reason.message) : 'ok'))
    })

    expect(nestedOutcomes).toEqual([expect.stringMatching(/UNIQUE/), expect.stringMatching(/CHECK/), 'inner refused'])
    expect(columnRaw(path, OWNERS_IN_ORDER)).toEqual(['taken', 'before', 'after'])
  })

  it('keeps a write made from outside a callback out of a transaction that rolls back', async () => {
    const { db, path } = await openAccounts()
    const held = deferred()

    const transaction = db
      .transaction(async tx => {
        await tx.execute(OPEN_ACCOUNT, ['inside-first', 1])
        await held.promise
        await db.execute(OPEN_ACCOUNT, ['inside-second', 2])
        throw new Error('rolled back')
      })
      .catch((err: Error) => err.message)
    const stranger = db.execute(OPEN_ACCOUNT, ['stranger', 3])
    held.resolve()

    expect(await transaction).toBe('rolled back')
    await expect(stranger).resolves.toMatchObject({ changes: 1 })
    expect(columnRaw(path, OWNERS_IN_ORDER)).toEqual(['stranger'])
  })

  it('waits for a transaction in flight before it closes the database, and refuses new work once closing starts', async () => {
    const { db, path } = await openAccounts()
    const held = deferred()
    const order: string[] = []
    let callbackStarted = () => {}
    const started = new Promise<void>(resolve => {
      callbackStarted = resolve
    })

    const transaction = db.transaction(async tx => {
      await tx.execute(OPEN_ACCOUNT, ['first', 1])
      callbackStarted()
      await held.promise
      await tx.execute(OPEN_ACCOUNT, ['second', 2])
      order.push('transaction committed')
    })
    await started
    const closing = db.close().then(() => order.push('closed'))
    await expect(db.execute(OPEN_ACCOUNT, ['late', 3])).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    held.resolve()

    await transaction
    await closing
    expect(order).toEqual(['transaction committed', 'closed'])
    expect(columnRaw(path, OWNERS_IN_ORDER)).toEqual(['first', 'second'])
  })

  it('executes a statement list as it stood when the call was made, and writes nothing for an empty list', async () => {
    const { db, path } = await openAccounts()
    const statements: { sql: string; params?: unknown[] }[] = [{ sql: OPEN_ACCOUNT, params: ['kept', 1] }]

    const pending = db.executeTransaction(statements)
    statements.push({ sql: 'DROP TABLE accounts' })
    statements[0] = { sql: OPEN_ACCOUNT, params: ['swapped', 1] }

    await expect(pending).resolves.toEqual([{ changes: 1, lastInsertRowId: 1 }])
    await expect(db.executeTransaction([])).resolves.toEqual([])
    expect(columnRaw(path, OWNERS_IN_ORDER)).toEqual(['kept'])
  })

  it('applies a statement list that changes the schema completely or not at all', async () => {
    const { db, path } = await openAccounts()

    await db.executeTransaction([
      { sql: 'CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT NOT NULL)' },
      { sql: 'INSERT INTO audit (note) VALUES (?)', params: ['opened'] },
    ])
    await expect(
      db.executeTransaction([
        { sql: 'CREATE TABLE refunds (id INTEGER PRIMARY KEY, note TEXT NOT NULL)' },
        { sql: 'INSERT INTO refunds (note) VALUES (?)', params: ['pending'] },
        { sql: 'INSERT INTO audit (note) VALUES (?)', params: [null] },
      ]),
    ).rejects.toMatchObject({ code: 'QUERY_ERROR', sql: 'INSERT INTO audit (note) VALUES (?)' })

    expect(columnRaw(path, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")).toEqual([
      'accounts',
      'audit',
    ])
    expect(columnRaw(path, 'SELECT note FROM audit')).toEqual(['opened'])
  })

  it('fails with TRANSACTION_ERROR and keeps none of its writes when SQLite refuses the commit', async () => {
    const { db, path } = await openAccounts()
    await db.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY)')
    await db.execute(
      'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED)',
    )

    await expect(
      db.transaction(async tx => {
        await tx.execute('INSERT INTO customers (id) VALUES (1)')
        await tx.execute('INSERT INTO orders (id, customer_id) VALUES (1, 404)')
      }),
    ).rejects.toMatchObject({ code: 'TRANSACTION_ERROR' })
    await expect(
      db.executeTransaction([
        { sql: 'INSERT INTO customers (id) VALUES (2)' },
        { sql: 'INSERT INTO orders (id, customer_id) VALUES (2, 405)' },
      ]),
    ).rejects.toMatchObject({ code: 'TRANSACTION_ERROR' })
    await expect(
      db.executeBatch('INSERT INTO orders (id, customer_id) VALUES (?, ?)', [
        [3, 406],
        [4, 407],
      ]),
    ).rejects.toMatchObject({ code: 'TRANSACTION_ERROR' })

    expect(countRaw(path, 'customers')).toBe(0)
    expect(countRaw(path, 'orders')).toBe(0)
    await expect(db.execute('INSERT INTO customers (id) VALUES (3)')).resolves.toMatchObject({ changes: 1 })
  })
})
