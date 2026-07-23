import { randomBytes } from 'node:crypto'
import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { META_TABLE } from '../internal-tables.js'
import { ensureMetaTable } from '../system-catalog/index.js'

const EPOCH_KEY = 'cdc_epoch'

/**
 * Returns a stable identifier for this database's change-log sequence space,
 * minting and persisting one on first use. Because `seq` values in
 * `_sirannon_changes` are a per-file autoincrement, a resume cursor is only
 * meaningful within the file that produced it: the same value survives a
 * restart of this node but means something different on another node. Clients
 * echo this epoch when resuming so the server can force a full resync instead
 * of replaying another file's rows against a foreign cursor.
 */
export async function ensureCdcEpoch(conn: SQLiteConnection): Promise<string> {
  await ensureMetaTable(conn)

  const insert = await conn.prepare(`INSERT OR IGNORE INTO "${META_TABLE}" (key, value) VALUES (?, ?)`)
  await insert.run(EPOCH_KEY, randomBytes(16).toString('hex'))

  const select = await conn.prepare(`SELECT value FROM "${META_TABLE}" WHERE key = ?`)
  const row = (await select.get(EPOCH_KEY)) as { value?: unknown } | undefined
  const value = row?.value
  if (typeof value !== 'string' || value.length === 0) {
    throw new CDCError('Failed to read the CDC epoch identifier')
  }
  return value
}
