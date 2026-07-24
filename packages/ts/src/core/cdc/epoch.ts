import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import { ensureMetaTable, getMetaValue, initMetaValue } from '../system-catalog/index.js'

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
  await initMetaValue(conn, EPOCH_KEY, randomHex(16))

  const value = await getMetaValue(conn, EPOCH_KEY)
  if (value === null || value.length === 0) {
    throw new CDCError('Failed to read the CDC epoch identifier')
  }
  return value
}
