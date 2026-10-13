import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonError } from '../../../core/errors.js'
import {
  collect,
  createWorkspace,
  FAST_POLL_MS,
  pause,
  QUIET_PERIOD_MS,
  readRaw,
  type Workspace,
  waitForCount,
  writeRaw,
} from './harness.js'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('lifecycle')
})

afterEach(async () => {
  await workspace.dispose()
})

function tablesIn(path: string): string[] {
  return readRaw<{ name: string }>(
    path,
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_sirannon%' ESCAPE '\\' ORDER BY name",
  ).map(table => table.name)
}

describe('watching and unwatching', () => {
  it('captures a table only while it is watched, and only for subscriptions on that table', async () => {
    const db = await workspace.open('watching')
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await db.execute('CREATE TABLE invoices (id INTEGER PRIMARY KEY)')
    await db.execute('CREATE TABLE drafts (id INTEGER PRIMARY KEY)')
    const orders = collect(db.on('orders'))
    const invoices = collect(db.on('invoices'))
    const drafts = collect(db.on('drafts'))

    await db.execute('INSERT INTO orders VALUES (1)')
    await db.watch('orders')
    await db.watch('orders')
    await db.watch('invoices')
    await db.execute('INSERT INTO orders VALUES (2)')
    await db.execute('INSERT INTO invoices VALUES (2)')
    await db.execute('INSERT INTO drafts VALUES (2)')
    await waitForCount(orders, 1)
    await db.unwatch('orders')
    await db.execute('INSERT INTO orders VALUES (3)')
    await db.execute('INSERT INTO invoices VALUES (3)')
    await waitForCount(invoices, 2)
    await db.unwatch('invoices')
    await db.unwatch('invoices')
    await db.unwatch('never_watched')
    await db.unwatch('no_such_table')
    await db.execute('INSERT INTO invoices VALUES (4)')
    await db.watch('orders')
    await db.execute('INSERT INTO orders VALUES (5)')
    await waitForCount(orders, 2)

    expect(orders.events.map(event => event.row.id)).toEqual([2, 5])
    expect(invoices.events.map(event => event.row.id)).toEqual([2, 3])
    expect(drafts.events).toEqual([])
  })

  it('refuses a table name that is no plain identifier, a missing or reserved table, or a column that is no plain identifier, and keeps capturing the rest', async () => {
    const db = await workspace.open('refusals')
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY)')
    await db.execute('CREATE TABLE prices (id INTEGER PRIMARY KEY, "unit price" INTEGER)')
    await db.execute('CREATE TABLE quoted (id INTEGER PRIMARY KEY, "it""s" TEXT)')
    await db.watch('users')
    const users = collect(db.on('users'))
    const tablesBefore = tablesIn(workspace.path('refusals'))

    const refusals: Array<[string, string]> = [
      ['users; DROP TABLE users', 'CDC_ERROR'],
      ['users"; DROP TABLE users; --', 'CDC_ERROR'],
      ["users'--", 'CDC_ERROR'],
      ['my-table', 'CDC_ERROR'],
      ['table name', 'CDC_ERROR'],
      ['1users', 'CDC_ERROR'],
      ['', 'CDC_ERROR'],
      ['ünïcode', 'CDC_ERROR'],
      ['users\u0000', 'CDC_ERROR'],
      ['a'.repeat(10_000), 'CDC_ERROR'],
      ['missing_table', 'CDC_ERROR'],
      ['prices', 'CDC_ERROR'],
      ['quoted', 'CDC_ERROR'],
      ['_sirannon_changes', 'FORBIDDEN_SQL'],
      ['_SIRANNON_META', 'FORBIDDEN_SQL'],
      ['sqlite_master', 'FORBIDDEN_SQL'],
    ]
    for (const [table, code] of refusals) {
      const watching = db.watch(table)
      await expect(watching, table.slice(0, 40)).rejects.toBeInstanceOf(SirannonError)
      await expect(watching, table.slice(0, 40)).rejects.toMatchObject({ code })
    }
    await db.execute('INSERT INTO users VALUES (1)')
    await db.execute('INSERT INTO prices VALUES (1, 5)')
    await waitForCount(users, 1)

    expect(tablesIn(workspace.path('refusals'))).toEqual(tablesBefore)
    expect(users.events.map(event => event.row.id)).toEqual([1])
  })

  it('refuses watch on a read-only database and every capture call on a closed one', async () => {
    const path = workspace.path('guarded')
    const writable = workspace.registry()
    const db = await writable.open('guarded', path, { cdcPollInterval: FAST_POLL_MS })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await db.watch('orders')
    const beforeClose = collect(db.on('orders'))
    await writable.close('guarded')

    await expect(db.watch('orders')).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    await expect(db.unwatch('orders')).rejects.toMatchObject({ code: 'DATABASE_CLOSED' })
    expect(() => db.on('orders')).toThrow(expect.objectContaining({ code: 'DATABASE_CLOSED' }))
    writeRaw(path, 'INSERT INTO orders VALUES (1)')
    await pause(QUIET_PERIOD_MS)
    expect(beforeClose.events).toEqual([])

    const readOnly = await workspace.registry().open('guarded', path, { readOnly: true })
    await expect(readOnly.watch('orders')).rejects.toMatchObject({ code: 'READ_ONLY' })
    await expect(readOnly.unwatch('orders')).resolves.toBeUndefined()
  })
})

function triggersIn(path: string): string[] {
  return readRaw<{ name: string }>(path, "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name").map(
    trigger => trigger.name,
  )
}

describe('the table name passed to watch', () => {
  it('resolves in any letter case to the stored table, records its changes under the stored name, and refuses a view or a virtual table with CDC_ERROR', async () => {
    const path = workspace.path('names')
    const db = await workspace.open('names')
    await db.execute('CREATE TABLE Orders (id INTEGER PRIMARY KEY, item TEXT)')
    await db.execute('CREATE VIEW recent_orders AS SELECT id FROM Orders')
    await db.execute('CREATE VIRTUAL TABLE order_search USING fts5(body)')
    const lower = collect(db.on('orders'))
    const upper = collect(db.on('ORDERS'))

    await db.watch('ORDERS')
    await db.watch('orders')
    await db.watch('Orders')
    for (const table of ['recent_orders', 'RECENT_ORDERS', 'order_search', 'ORDER_SEARCH']) {
      await expect(db.watch(table), table).rejects.toMatchObject({ code: 'CDC_ERROR' })
    }
    await db.execute("INSERT INTO orders VALUES (1, 'kente')")
    await db.execute("UPDATE ORDERS SET item = 'shea' WHERE id = 1")
    await waitForCount(lower, 2)

    expect(triggersIn(path)).toEqual([
      '_sirannon_trg_Orders_delete',
      '_sirannon_trg_Orders_insert',
      '_sirannon_trg_Orders_insert_conflicts',
      '_sirannon_trg_Orders_update',
      '_sirannon_trg_Orders_update_conflicts',
    ])
    expect(readRaw(path, 'SELECT DISTINCT table_name FROM _sirannon_changes')).toEqual([{ table_name: 'Orders' }])
    expect(lower.events.map(event => [event.table, event.type, event.row])).toEqual([
      ['Orders', 'insert', { id: 1, item: 'kente' }],
      ['Orders', 'update', { id: 1, item: 'shea' }],
    ])
    expect(upper.events).toEqual(lower.events)

    await db.unwatch('oRdErS')
    await db.execute("INSERT INTO orders VALUES (2, 'gari')")
    await pause(QUIET_PERIOD_MS)

    expect(triggersIn(path)).toEqual([])
    expect(lower.events).toHaveLength(2)
  })

  it('stops capturing a watched table that a statement drops under another letter case, and captures its replacement once it is watched', async () => {
    const path = workspace.path('dropped-names')
    const db = await workspace.open('dropped-names')
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await db.watch('orders')
    const seen = collect(db.on('orders'))

    await db.execute('INSERT INTO orders VALUES (1)')
    await db.execute('DROP TABLE ORDERS')
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)')
    await db.execute("INSERT INTO orders VALUES (2, 'unwatched')")
    const triggersAfterDrop = triggersIn(path)
    await db.watch('Orders')
    await db.execute("INSERT INTO orders VALUES (3, 'watched again')")
    await waitForCount(seen, 2)

    expect(triggersAfterDrop).toEqual([])
    expect(seen.events.map(event => event.row)).toEqual([{ id: 1 }, { id: 3, note: 'watched again' }])
  })
})

describe('a change log written by an older version', () => {
  it('gains the columns of the current version on watch and keeps the changes it held', async () => {
    const path = workspace.path('legacy')
    const raw = new BetterSqlite3(path)
    raw.exec(`
      CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT);
      INSERT INTO orders VALUES (1, 'legacy');
      CREATE TABLE _sirannon_changes (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT NOT NULL,
        operation TEXT NOT NULL,
        row_id TEXT NOT NULL,
        changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
        old_data TEXT,
        new_data TEXT
      );
      INSERT INTO _sirannon_changes (table_name, operation, row_id, new_data)
        VALUES ('orders', 'INSERT', '1', '{"id":1,"item":"legacy"}');
    `)
    raw.close()

    const db = await workspace.open('legacy')
    await db.watch('orders')
    const seen = collect(db.on('orders'))
    await db.execute("INSERT INTO orders VALUES (2, 'current')")
    await waitForCount(seen, 2)

    const columns = readRaw<{ name: string }>(path, "SELECT name FROM pragma_table_info('_sirannon_changes')").map(
      column => column.name,
    )
    expect(columns).toEqual(expect.arrayContaining(['node_id', 'tx_id', 'hlc']))
    expect(
      readRaw<{ seq: bigint; new_data: string }>(path, 'SELECT seq, new_data FROM _sirannon_changes ORDER BY seq')[0],
    ).toEqual({ seq: 1n, new_data: '{"id":1,"item":"legacy"}' })
    expect(seen.events.map(event => [event.type, event.rowId, event.row])).toEqual([
      ['insert', '1', { id: 1, item: 'legacy' }],
      ['insert', '2', { id: 2, item: 'current' }],
    ])
  })
})
