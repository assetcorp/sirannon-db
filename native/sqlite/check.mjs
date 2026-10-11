import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const sourceDirectory = dirname(fileURLToPath(import.meta.url))
const referenceManifest = join(sourceDirectory, '..', '..', 'packages', 'ts', 'package.json')

const SEEDED_ROW_COUNT = 5000
const DISTINCT_VALUE_COUNT = 1000
const MATCHED_VALUE = 42
const MATCHES_PER_VALUE = SEEDED_ROW_COUNT / DISTINCT_VALUE_COUNT
const LOOKUP_ID = 2500

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

function seed(db) {
  db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL, customer TEXT NOT NULL)')
  const insert = db.prepare('INSERT INTO orders (total, customer) VALUES (?, ?)')
  db.transaction(() => {
    for (let i = 0; i < SEEDED_ROW_COUNT; i++) insert.run(i % DISTINCT_VALUE_COUNT, `customer-${i}`)
  })()
  assert.deepEqual(insert.takeRowCounts(), { rowsRead: 0, rowsWritten: SEEDED_ROW_COUNT })
}

function checkCounts(db) {
  const scan = db.prepare('SELECT * FROM orders WHERE total = ?')
  assert.equal(scan.all(MATCHED_VALUE).length, MATCHES_PER_VALUE)
  assert.deepEqual(scan.takeRowCounts(), { rowsRead: SEEDED_ROW_COUNT, rowsWritten: 0 })
  assert.deepEqual(scan.takeRowCounts(), { rowsRead: 0, rowsWritten: 0 })

  const lookup = db.prepare('SELECT * FROM orders WHERE id = ?')
  assert.equal(lookup.get(LOOKUP_ID).id, LOOKUP_ID)
  assert.deepEqual(lookup.takeRowCounts(), { rowsRead: 1, rowsWritten: 0 })

  const update = db.prepare('UPDATE orders SET customer = ? WHERE total = ?')
  assert.equal(update.run('renamed', MATCHED_VALUE).changes, MATCHES_PER_VALUE)
  assert.deepEqual(update.takeRowCounts(), { rowsRead: SEEDED_ROW_COUNT, rowsWritten: MATCHES_PER_VALUE })
  return scan
}

const fromSirannon = createRequire(pathToFileURL(referenceManifest))
const Database = fromSirannon('better-sqlite3')
const binding = addonPath()
const db = new Database(':memory:', { nativeBinding: binding })
seed(db)
const scan = checkCounts(db)
db.close()
assert.throws(() => scan.takeRowCounts(), TypeError)
console.log(`Row counts match on ${hostTarget()} with Node ${process.version}, using ${binding}`)
