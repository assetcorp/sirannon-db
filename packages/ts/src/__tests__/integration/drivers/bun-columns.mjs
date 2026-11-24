import { join } from 'node:path'
import { Sirannon } from '../../../core/sirannon.js'
import { bunSqlite } from '../../../drivers/bun/index.js'

const path = join(process.argv[2], 'columns.db')
const REPEATED =
  'SELECT a.id, b.id, a.label, a.amount, a.payload, a.amount + 0 AS total FROM items a JOIN items b ON a.id = b.id ORDER BY a.id'
const UNIQUE = 'SELECT id, label, amount, payload, 1 AS "__proto__", 2 AS "constructor" FROM items ORDER BY id'

async function outcome(call) {
  try {
    return await call()
  } catch (err) {
    return { refused: err?.code ?? 'UNCODED' }
  }
}

const sirannon = new Sirannon({ driver: bunSqlite() })
const db = await sirannon.open('columns', path)
await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, payload BLOB)')
const insert = 'INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)'
await db.execute(insert, [1, 'héllo 😀', 2n ** 53n + 1n, Uint8Array.from([0, 1, 255])])
await db.execute(insert, [2, '', -(2n ** 63n), new Uint8Array(0)])
await db.execute(insert, [3, null, 7, null])

const result = {
  repeated: await outcome(() => db.queryWithColumns(REPEATED)),
  repeatedLimited: await outcome(() => db.queryWithColumns(REPEATED, [], { maxRows: 2 })),
  unique: await outcome(() => db.queryWithColumns(UNIQUE)),
  uniqueLimited: await outcome(() => db.queryWithColumns(UNIQUE, [], { maxRows: 2 })),
  bytes: await outcome(() => db.queryWithColumns('SELECT label FROM items ORDER BY id', [], { maxBytes: 8 + 11 })),
  reserved: await outcome(() => db.queryWithColumns("SELECT * FROM '_sirannon_meta'")),
}
await db.execute('CREATE TABLE evolving (id INTEGER)')
await db.execute('INSERT INTO evolving VALUES (1)')
const evolving = 'SELECT * FROM evolving'
for (let repeat = 0; repeat < 4; repeat++) await db.queryWithColumns(evolving)
await db.execute('ALTER TABLE evolving ADD COLUMN label TEXT')
result.schemaRead = await db.queryWithColumns(evolving)
await db.execute('ALTER TABLE evolving ADD COLUMN amount INTEGER')
result.schemaLimited = await db.queryWithColumns(evolving, [], { maxRows: 1 })
await sirannon.shutdown()

process.stdout.write(
  JSON.stringify(result, (_, value) => {
    if (typeof value === 'bigint') return `${value}n`
    if (value instanceof Uint8Array) return `x'${Buffer.from(value).toString('hex')}'`
    return value
  }),
)
