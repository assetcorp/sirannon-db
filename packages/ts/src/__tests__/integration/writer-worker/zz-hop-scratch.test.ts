import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'
import { it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const ROUNDS = 2000

it('counts worker messages and time per write', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'hop-scratch-'))
  const original = Worker.prototype.postMessage
  let posted = 0
  Worker.prototype.postMessage = function (this: Worker, ...args: Parameters<Worker['postMessage']>) {
    posted++
    return original.apply(this, args)
  }
  const lines: string[] = []
  try {
    for (const watched of [false, true]) {
      const sirannon = new Sirannon({ driver: betterSqlite3() })
      const db = await sirannon.open(`w${watched}`, join(dir, `w${watched}.db`), { writerWorker: true })
      await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
      if (watched) await db.watch('notes')
      for (const [label, write] of [
        ['transaction', () => db.transaction(tx => tx.execute("INSERT INTO notes (body) VALUES ('x')"))],
        ['executeBatch', () => db.executeBatch('INSERT INTO notes (body) VALUES (?)', [['a'], ['b']])],
        ['execute', () => db.execute("INSERT INTO notes (body) VALUES ('x')")],
      ] as const) {
        for (let i = 0; i < 200; i++) await write()
        posted = 0
        const start = performance.now()
        for (let i = 0; i < ROUNDS; i++) await write()
        const us = ((performance.now() - start) / ROUNDS) * 1000
        lines.push(`watched=${watched} ${label}: ${(posted / ROUNDS).toFixed(2)} messages, ${us.toFixed(1)} us`)
      }
      await sirannon.shutdown()
    }
  } finally {
    Worker.prototype.postMessage = original
    rmSync(dir, { recursive: true, force: true })
  }
  console.log(lines.join('\n'))
}, 300_000)
