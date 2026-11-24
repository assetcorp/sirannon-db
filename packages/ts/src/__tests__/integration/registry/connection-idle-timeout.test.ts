import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

it('accepts an idle timeout longer than the runtime timer limit without overflowing timers', async () => {
  const path = join(mkdtempSync(join(tmpdir(), 'sirannon-long-idle-')), 'orders.db')
  const registry = new Sirannon({ driver: betterSqlite3() })
  const overflowWarnings: Error[] = []
  const recordWarning = (warning: Error) => {
    if (warning.name === 'TimeoutOverflowWarning') overflowWarnings.push(warning)
  }
  process.on('warning', recordWarning)
  try {
    const db = await registry.open('orders', path, { connectionIdleTimeout: 30 * 24 * 60 * 60 * 1000 })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
    await db.execute('INSERT INTO orders VALUES (1)')
    await new Promise<void>(resolve => setImmediate(resolve))
    expect(overflowWarnings).toEqual([])
    const oracle = new BetterSqlite3(path, { readonly: true })
    try {
      expect(await db.query('SELECT id FROM orders')).toEqual(oracle.prepare('SELECT id FROM orders').all())
    } finally {
      oracle.close()
    }
  } finally {
    await registry.shutdown()
    process.off('warning', recordWarning)
  }
})
