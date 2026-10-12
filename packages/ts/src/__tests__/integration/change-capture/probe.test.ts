import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

let dir: string
let sirannon: Sirannon

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'probe-'))
  sirannon = new Sirannon({ driver: betterSqlite3() })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

const settle = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

it('replace and closed alter', async () => {
  const path = join(dir, 'a.db')
  const db = await sirannon.open('a', path, { cdcPollInterval: 5 })
  await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)')
  await db.watch('t')
  const got: ChangeEvent[] = []
  db.on('t').subscribe(e => got.push(e))
  await db.execute("INSERT INTO t VALUES (1, 'a@x')")
  await db.execute("INSERT OR REPLACE INTO t VALUES (2, 'a@x')")
  await db.execute("REPLACE INTO t VALUES (2, 'b@x')")
  await settle(80)
  console.log(got.map(e => [e.type, JSON.stringify(e.row), JSON.stringify(e.oldRow)]))
  await sirannon.close('a')
  const raw = new BetterSqlite3(path)
  raw.exec('ALTER TABLE t ADD COLUMN extra TEXT')
  raw.close()
  const db2 = await sirannon.open('a', path, { cdcPollInterval: 5 })
  await db2.watch('t')
  const got2: ChangeEvent[] = []
  db2.on('t').subscribe(e => got2.push(e))
  await db2.execute("INSERT INTO t VALUES (3, 'c@x', 'present')")
  await settle(80)
  console.log(got2.map(e => JSON.stringify(e.row)))
})
