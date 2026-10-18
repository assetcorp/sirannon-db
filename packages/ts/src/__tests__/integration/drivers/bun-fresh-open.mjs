import { join } from 'node:path'
import { Sirannon } from '../../../core/sirannon.js'
import { bunSqlite } from '../../../drivers/bun/index.js'

const path = join(process.argv[2], 'fresh.db')

const sirannon = new Sirannon({ driver: bunSqlite() })
const db = await sirannon.open('fresh', path, { readPoolSize: 2 })
await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
await db.watch('notes')
await db.execute("INSERT INTO notes (body) VALUES ('first write')")
await db.transaction(async tx => {
  await tx.execute("INSERT INTO notes (body) VALUES ('through the handle')")
  await db.execute("INSERT INTO notes (body) VALUES ('through the database')")
})
const read = await db.query('SELECT body FROM notes ORDER BY id')
await sirannon.shutdown()

const reopened = new Sirannon({ driver: bunSqlite() })
const again = await reopened.open('fresh', path, { readOnly: true })
const readAgain = await again.query('SELECT count(*) AS n FROM notes')
await reopened.shutdown()

process.stdout.write(JSON.stringify({ read, readAgain }))
