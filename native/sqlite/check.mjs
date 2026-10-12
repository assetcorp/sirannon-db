import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const referenceManifest = join(sourceDirectory, '..', '..', 'packages', 'ts', 'package.json')

const SEEDED_ROW_COUNT = 5000
const DISTINCT_TOTAL_COUNT = 1000
const MATCHED_TOTAL = 42
const MISSING_TOTAL = DISTINCT_TOTAL_COUNT + 1
const MATCHES_PER_TOTAL = SEEDED_ROW_COUNT / DISTINCT_TOTAL_COUNT
const LOOKUP_ID = 2500
const LAST_CUSTOMER = `customer-${SEEDED_ROW_COUNT - 1}`
const JSON_ELEMENTS = [1, 2, 3, 4]

const counts = new Float64Array(2)

function hostTarget() {
  const pair = `${process.platform}-${process.arch}`
  if (process.platform !== 'linux') return pair
  return process.report.getReport().header.glibcVersionRuntime ? pair : `${pair}-musl`
}

function addonPath() {
  const requested = process.argv[2]
  const path = requested ? resolve(requested) : join(sourceDirectory, 'out', hostTarget(), 'better_sqlite3.node')
  if (!existsSync(path)) {
    throw new Error(`No row-counting SQLite build exists at ${path}, so build it first with pnpm build:sqlite`)
  }
  return path
}

function takeCounts(statement) {
  statement.takeRowCounts(counts)
  return { rowsRead: counts[0], rowsWritten: counts[1] }
}

function seed(db) {
  db.exec(`
    CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL, customer TEXT NOT NULL);
    CREATE INDEX orders_by_total ON orders (total);
  `)
  const insert = db.prepare('INSERT INTO orders (total, customer) VALUES (?, ?)')
  db.transaction(() => {
    for (let i = 0; i < SEEDED_ROW_COUNT; i++) insert.run(i % DISTINCT_TOTAL_COUNT, `customer-${i}`)
  })()
  assert.deepEqual(takeCounts(insert), { rowsRead: 0, rowsWritten: SEEDED_ROW_COUNT * 2 })
}

function checkReads(db) {
  const scan = db.prepare('SELECT * FROM orders WHERE customer = ?')
  assert.equal(scan.all(LAST_CUSTOMER).length, 1)
  assert.deepEqual(takeCounts(scan), { rowsRead: SEEDED_ROW_COUNT, rowsWritten: 0 })
  assert.deepEqual(takeCounts(scan), { rowsRead: 0, rowsWritten: 0 })

  const lookup = db.prepare('SELECT * FROM orders WHERE id = ?')
  assert.equal(lookup.get(LOOKUP_ID).id, LOOKUP_ID)
  assert.deepEqual(takeCounts(lookup), { rowsRead: 1, rowsWritten: 0 })

  const byTotal = db.prepare('SELECT * FROM orders WHERE total = ?')
  assert.equal(byTotal.all(MATCHED_TOTAL).length, MATCHES_PER_TOTAL)
  assert.deepEqual(takeCounts(byTotal), { rowsRead: MATCHES_PER_TOTAL + 1, rowsWritten: 0 })
  assert.equal(byTotal.all(MISSING_TOTAL).length, 0)
  assert.deepEqual(takeCounts(byTotal), { rowsRead: 0, rowsWritten: 0 })

  const count = db.prepare('SELECT count(*) AS n FROM orders')
  assert.equal(count.get().n, SEEDED_ROW_COUNT)
  assert.deepEqual(takeCounts(count), { rowsRead: SEEDED_ROW_COUNT, rowsWritten: 0 })

  const elements = db.prepare('SELECT value FROM json_each(?)')
  assert.equal(elements.all(JSON.stringify(JSON_ELEMENTS)).length, JSON_ELEMENTS.length)
  assert.deepEqual(takeCounts(elements), { rowsRead: JSON_ELEMENTS.length, rowsWritten: 0 })
  return scan
}

function checkWrites(db) {
  const rename = db.prepare('UPDATE orders SET customer = ? WHERE id = ?')
  assert.equal(rename.run('renamed', LOOKUP_ID).changes, 1)
  assert.deepEqual(takeCounts(rename), { rowsRead: 1, rowsWritten: 1 })

  const retotal = db.prepare('UPDATE orders SET total = ? WHERE id = ?')
  assert.equal(retotal.run(MISSING_TOTAL, LOOKUP_ID).changes, 1)
  assert.deepEqual(takeCounts(retotal), { rowsRead: 1, rowsWritten: 2 })

  db.exec('CREATE TABLE audit (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL)')
  const copy = db.prepare('INSERT INTO audit (order_id) SELECT id FROM orders WHERE total = ?')
  assert.equal(copy.run(MATCHED_TOTAL).changes, MATCHES_PER_TOTAL)
  assert.deepEqual(takeCounts(copy), { rowsRead: MATCHES_PER_TOTAL + 1, rowsWritten: MATCHES_PER_TOTAL })

  const shift = db.prepare('UPDATE audit SET order_id = order_id + 1 WHERE order_id IN (SELECT order_id FROM audit)')
  assert.equal(shift.run().changes, MATCHES_PER_TOTAL)
  assert.equal(takeCounts(shift).rowsWritten, MATCHES_PER_TOTAL)

  const failing = db.prepare('UPDATE orders SET total = NULL WHERE customer = ?')
  assert.throws(() => failing.run(LAST_CUSTOMER), /NOT NULL constraint failed/)
  assert.deepEqual(takeCounts(failing), { rowsRead: SEEDED_ROW_COUNT, rowsWritten: 0 })
}

function checkTriggers(db) {
  db.exec(`
    CREATE TABLE _sirannon_changes (seq INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL);
    CREATE TRIGGER orders_audit AFTER INSERT ON orders BEGIN
      INSERT INTO audit (order_id) VALUES (NEW.id);
    END;
  `)
  const insert = db.prepare('INSERT INTO orders (total, customer) VALUES (?, ?)')
  insert.run(MATCHED_TOTAL, 'with-caller-trigger')
  assert.deepEqual(takeCounts(insert), { rowsRead: 0, rowsWritten: 3 })

  db.exec(`
    CREATE TRIGGER "_sirannon_trg_orders_insert" AFTER INSERT ON orders BEGIN
      INSERT INTO _sirannon_changes (order_id) VALUES (NEW.id);
    END;
  `)
  insert.run(MATCHED_TOTAL, 'with-change-capture')
  assert.deepEqual(takeCounts(insert), { rowsRead: 0, rowsWritten: 3 })
  insert.run(MATCHED_TOTAL, 'with-change-capture-again')
  assert.deepEqual(takeCounts(insert), { rowsRead: 0, rowsWritten: 3 })
  assert.equal(db.prepare('SELECT count(*) AS n FROM _sirannon_changes').get().n, 2)

  db.exec('CREATE TABLE receipts (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT NOT NULL)')
  const receipt = db.prepare('INSERT INTO receipts (note) VALUES (?)')
  receipt.run('first')
  takeCounts(receipt)
  receipt.run('second')
  assert.deepEqual(takeCounts(receipt), { rowsRead: 2, rowsWritten: 2 })

  db.exec(`
    CREATE TABLE customers (id INTEGER PRIMARY KEY);
    CREATE TABLE invoices (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers (id) ON DELETE CASCADE);
    INSERT INTO customers (id) VALUES (1), (2);
    INSERT INTO invoices (customer_id) VALUES (1), (1), (2);
  `)
  const removeCustomer = db.prepare('DELETE FROM customers WHERE id = ?')
  assert.equal(removeCustomer.run(1).changes, 1)
  assert.equal(takeCounts(removeCustomer).rowsWritten, 3)
}

const fromSirannon = createRequire(pathToFileURL(referenceManifest))
const Database = fromSirannon('better-sqlite3')
const binding = addonPath()
const db = new Database(':memory:', { nativeBinding: binding })
db.pragma('foreign_keys = ON')
seed(db)
const scan = checkReads(db)
checkWrites(db)
checkTriggers(db)
assert.throws(() => scan.takeRowCounts(new Float64Array(1)), TypeError)
db.close()
assert.throws(() => scan.takeRowCounts(counts), TypeError)
console.log(`Row counts match on ${hostTarget()} with Node ${process.version}, using ${binding}`)
