import { join } from 'node:path'
import { Sirannon } from '../../../core/sirannon.js'
import { bunSqlite } from '../../../drivers/bun/index.js'

const path = join(process.argv[2], 'values.db')
const LARGEST_INT64 = 2n ** 63n - 1n

async function refusal(call) {
  try {
    await call()
    return null
  } catch (err) {
    return err?.code ?? 'UNCODED'
  }
}

const sirannon = new Sirannon({ driver: bunSqlite() })
const db = await sirannon.open('values', path)
await db.execute('CREATE TABLE cells (id INTEGER PRIMARY KEY, label TEXT, value)')
await db.execute('INSERT INTO cells (id, label, value) VALUES (:id, :label, :value)', {
  id: 1,
  label: 'colon',
  value: 'named',
})
await db.execute('INSERT INTO cells (id, label, value) VALUES (@id, @label, @value)', {
  id: 2,
  label: 'at sign',
  value: true,
})
await db.execute('INSERT INTO cells (id, label, value) VALUES ($id, $label, $value)', {
  id: 3,
  label: 'dollar',
  value: false,
})
await db.execute('INSERT INTO cells (id, label, value) VALUES (?, ?, ?)', [4, 'empty blob', new Uint8Array(0)])
await db.execute('INSERT INTO cells (id, label, value) VALUES (?, ?, ?)', [5, 'largest integer', LARGEST_INT64])
await db.executeBatch('INSERT INTO cells (id, label, value) VALUES (:id, :label, :value)', [
  { id: 6, label: 'batch', value: Uint8Array.from([0, 255]) },
])
const refusals = {
  missingNamed: await refusal(() =>
    db.execute('INSERT INTO cells (id, label, value) VALUES (:id, :label, :value)', { id: 7, label: 'missing' }),
  ),
  beyondInt64: await refusal(() => db.execute('INSERT INTO cells (id, value) VALUES (?, ?)', [8, LARGEST_INT64 + 1n])),
  plainObject: await refusal(() => db.execute('INSERT INTO cells (id, value) VALUES (?, ?)', [9, { amount: 1 }])),
}
const read = await db.query('SELECT id, value FROM cells WHERE id = :id OR value = :value ORDER BY id', {
  id: 5,
  value: true,
})
await sirannon.shutdown()

process.stdout.write(
  JSON.stringify({ refusals, read }, (_, value) => (typeof value === 'bigint' ? `${value}n` : value)),
)
